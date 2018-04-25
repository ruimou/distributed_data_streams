package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const GREEN_COL = "\x1b[32;1m"
const ERR_COL = "\x1b[31;1m"
const ERR_END = "\x1b[0m"

type FileData struct {
	Version int    `json:"version"`
	Data    string `json:"data"`
}

type ClusterData struct {
	Topic   string     `json:"topic"`
	Dataset []FileData `json: "dataset"`
}

// DataPath where files are written to disk
var DataPath string

var TopicName string

// For a Leader node, this list is guaranteed to be in order and continuous
// since a Leader serializes the Writes but we cannot guarantee
// the time arrival of those Writes to Follower nodes
var (
	VersionListLock sync.Mutex
	VersionList     []FileData

	// Writes may come in different order so this index is to show
	// where in VersionList are the Writes no longer ordered
	// i.e. [1,2,3,5,6], the first mismatch is 3
	// since V5 does not match its index of 4. (note: WriteId's begin at 1)
	// If FirstMismatch == 0 or Len(VersionList), we have all the writes
	FirstMismatch int

	// Channel for passing the new WriteId back to node main package
	writeIdCh chan int
)

// FileSystem related errors //////
type FileSystemError string

func (e FileSystemError) Error() string {
	return fmt.Sprintf(string(e))
}

type InsufficientConfirmedWritesError string

func (e InsufficientConfirmedWritesError) Error() string {
	return fmt.Sprintf("Could not replicate write enough times. %s", string(e))
}

type IncompleteDataError string

func (e IncompleteDataError) Error() string {
	return fmt.Sprintf("There is an incomplete dataset. Cannot read.")
}

////////////////////////////////////

func MountFiles(path string, writeCh chan int) {
	VersionListLock = sync.Mutex{}
	VersionList = make([]FileData, 0)
	writeIdCh = writeCh
	DataPath = path
	fname := filepath.Join(path, "data.json")

	// First time a node has registered with a server
	if _, err := os.Stat(fname); os.IsNotExist(err) {
		f, err := os.Create(fname)
		if err != nil {
			checkError(err, "MountFiles CreateFiles")
			log.Fatalf("Couldn't create file [%s]", fname)
		}

		f.Sync()
		defer f.Close()

		return
	}

	// Rejoining node, read topic data from disk
	var clusterData ClusterData
	err := readFromDisk(fname, &clusterData)
	TopicName = clusterData.Topic
	VersionList = clusterData.Dataset

	if err != nil {
		checkError(err, "MountFiles DiskRead")
		log.Fatalf("Could not read from disk")
	}
}

// Add the Write to the VersionList and commit Write to disk
func WriteNode(topic, data string, version int) error {
	if TopicName != "" && topic != TopicName {
		return errors.New("Writing to wrong topic")
	}

	TopicName = topic
	VersionListLock.Lock()

	// Minor optimizations.
	// We're assuming that writes often come in order and if its greater than the last
	// item in the list, just append it.
	versionLen := len(VersionList)
	if versionLen == 0 {
		FirstMismatch++
	} else {
		last := VersionList[versionLen-1]
		if last.Version <= version {
			FirstMismatch++
		}
	}

	VersionList = append(VersionList, FileData{
		Version: version,
		Data:    data,
	})
	VersionListLock.Unlock()

	if err := writeToDisk(DataPath); err != nil {
		log.Println("ERROR WRITING TO DISK IN WRITEFILE")
		return err
	}
	return nil
}

// Returns confirmed writes the node contains
// Errors:
// IncompleteDataError - Not all writes have been received
func ReadNode(topic string) ([]string, error) {
	if data, hasCompleteData := HasAllData(); hasCompleteData {
		return data, nil
	}

	return nil, IncompleteDataError("")
}

// writeStatusCh - Channel to wait on for peers to write to whether they have confirmed the write
// numRequiredWrites - Needed number of confirmed writes to reach majority
// maxFailures - Number of unconfirmed writes until we should return (otherwise could wait indefinitely for numRequiredWrites)
// Returns a channel with whether the Write was replicated
func CountConfirmedWrites(writeStatusCh chan bool, numRequiredWrites, maxFailures uint8) chan bool {
	writeReplicatedCh := make(chan bool)
	go func() {
		numWrites, numFailures := uint8(0), uint8(0)
		for {
			if numRequiredWrites >= numWrites {
				writeReplicatedCh <- true
			}

			if numFailures > maxFailures {
				writeReplicatedCh <- false
			}
			select {
			case writeSucceeded := <-writeStatusCh:
				if writeSucceeded {
					numWrites++
				} else {
					numFailures++
				}
			}
		}
	}()

	return writeReplicatedCh
}

// Diffs between the data a node has and returns its missing data compared to local VersionList
// Called on Leader node's when Followers ask to rejoin their cluster
func DiffMissingData(containingData map[int]bool) []FileData {
	missingData := make([]FileData, 0)
	versionLen := len(VersionList)

	VersionListLock.Lock()
	sortVersionList()
	VersionListLock.Unlock()

	if len(containingData) != versionLen {
		fmt.Println(ERR_COL+"DiffsMissingData:: Follower node has %d numWrites, Leader node has %d numWrites"+ERR_END, len(containingData), versionLen)
		for i := 0; i < versionLen; i++ {
			if !containingData[i+1] { // writeId's begin at 1, so we +1 compared to index i
				missingData = append(missingData, VersionList[i])
			}
		}
	}

	return missingData
}

// Gets missing data when node is newly elected Leader and now needs a complete set of writes
// Returns error if cannot find data
func GetMissingData(latestVersion int) error {
	VersionListLock.Lock()
	sortVersionList()
	VersionListLock.Unlock()

	// No data has been written
	versionLen := len(VersionList)
	fmt.Printf("Version len is: %d, latestVersion: %d\n", versionLen, latestVersion)
	if latestVersion == 0 || latestVersion == versionLen {
		return nil
	}

	// Find missing versions
	var missingVersions map[int]bool = make(map[int]bool)
	fmt.Printf("FirstMismatch: %d, Missing Versions: %+v", FirstMismatch, missingVersions)
	// i is for index in the VersionList
	// m the index we're looking for
	for i, m := FirstMismatch, FirstMismatch; i < latestVersion; i, m = i+1, m+1 {

		// All indices are above our VersionLen so we won't have it
		if i >= versionLen {
			missingVersions[m] = true
			// missingVersions = append(missingVersions, m)
			continue
		}

		for {
			if VersionList[i].Version != m {
				missingVersions[m] = true
				// missingVersions = append(missingVersions, m)
				m++
			} else {
				break
			}
		}
	}

	// Send the list of missingVersions to each Peer
	// The Peer will return a map[versionNum]Data
	// and we will remove the keys that we received from missingVersions
	// We will continue this process until either
	// 1) we have no more missingVersions
	// 2) no more Peers to request data from
	// Case 2 should not happen if there fewer than ClusterSize/2 node failures

	for ip := range DirectFollowersList {
		if len(missingVersions) == 0 {
			break
		}

		peer, ok := PeerMap.Get(ip)
		if !ok {
			fmt.Println("Peer died")
			continue
		}

		var writeData []FileData

		fmt.Println(ERR_COL+"Missing versions is:"+ERR_END+"%+v", missingVersions)
		err := peer.PeerConn.Call("Peer.GetWrites", missingVersions, writeData)
		if err != nil {
			fmt.Println(ERR_COL+"Err to GetWrites for Peer [%s]"+ERR_END, ip)
			continue
		}

		for _, fdata := range writeData {
			delete(missingVersions, fdata.Version)
		}
	}

	if len(missingVersions) != 0 {
		log.Println(ERR_COL + "Data is missing. Could not get data from followers" + ERR_END)
		return IncompleteDataError("")
	}

	return nil
}

///////////////Writing to disk helpers /////////////////
func writeToDisk(path string) error {
	fileData := ClusterData{
		Topic:   TopicName,
		Dataset: VersionList,
	}

	fname := filepath.Join(path, "data.json")
	contents, err := json.MarshalIndent(fileData, "", "  ")
	if err = ioutil.WriteFile(fname, contents, 0644); err != nil {
		log.Println(ERR_COL + "ERROR WRITING TO DISK" + ERR_END)
		return err
	}

	return nil
}

func readFromDisk(fname string, clusterData *ClusterData) error {
	contents, err := ioutil.ReadFile(fname)
	if err != nil {
		checkError(err, "readFromDisk")
		return err
	}

	if len(contents) == 0 {
		log.Println("Reading from disk ... No ClusterData")
		return nil
	}

	err = json.Unmarshal(contents, clusterData)
	return err
}

////////////End Writing to disk helpers /////////////////

/////////////// VersionList Helpers ///////////////////

// Sorts VersionList by its version number and returns the first index
// that does not match its version number
// Returns length of VersionList if all indices match its version number
func sortVersionList() {
	sort.Slice(VersionList, func(i, j int) bool {
		return VersionList[i].Version < VersionList[j].Version
	})

	var j int
	for i, fdata := range VersionList {
		if i+1 != fdata.Version {
			FirstMismatch = i
			fmt.Println(GREEN_COL+"After sorting ... First Mismatch: %d"+ERR_END, FirstMismatch)
			return
		}
		j++
	}

	FirstMismatch = j
	fmt.Println(GREEN_COL+"After sorting ... First Mismatch: %d"+ERR_END, FirstMismatch)
}

// Returns list of data (empty list if does not contain all data),
func HasAllData() (data []string, hasAllData bool) {
	VersionListLock.Lock()
	defer VersionListLock.Unlock()

	data = make([]string, 0)
	for i, fdata := range VersionList {
		if i+1 != fdata.Version {
			return nil, false
		}

		data = append(data, fdata.Data)
	}

	return data, true
}

// Return the highest version number the node has. If node has no data, returns 0
func GetLatestVersion() int {
	VersionListLock.Lock()

	printVersionList()
	defer VersionListLock.Unlock()

	versionLen := len(VersionList)
	if versionLen == 0 {
		return 0
	}

	var max int = 0
	for _, fdata := range VersionList {
		if max < fdata.Version {
			max = fdata.Version
		}
	}

	return max
}

/////////////// End VersionList Helpers ///////////////////

func printVersionList() {
	fmt.Println("Printing versionlist ...")

	for i, v := range VersionList {
		fmt.Printf("Index: %d, version: %+v\n", i, v)
	}

	fmt.Println("Version length is ... : %d", len(VersionList))
}
