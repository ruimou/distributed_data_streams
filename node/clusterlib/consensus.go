/*

This file contains the consensus protocol functions. This is both for data
consensus as well as leader nomination consensus.

*/
package node

import (
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"sync"
	"time"

	"../../structs"
)

// Maximum number of seconds for consensus job to wait for before timeout error.
const ELECTION_ATTEMPT_FOLLOW_WAIT = 2

// Number of seconds to wait until election is considered complete
const ELECTION_COMPLETE_TIMEOUT = 8

// Number of seconds to wait after election complete to let results come in
const ELECTION_WAIT_FOR_RESULTS = 16

var dataChannels sync.Map

// Election related file-global vars
var electionInProgress bool = false
var electionNumRequired int
var electionNumAccepted int
var electionLock sync.Mutex
var electionComplete bool = false
var electionUpdateCond sync.Cond
var chosenLeaderId string
var chosenLeaderLatestNum int
var nominationCompleteCh chan bool = make(chan bool, 1)

var PotentialFollowerIps []string
var receiveFollowerChannel chan string
var MyAddr string // Address for PeerRPC
var ClusterRpcAddr string

// Initial entry point to the consensus protocol
// Called when the leader heartbeat stops
// Returns error if too many nodes failed and we cannot rebuild the dataset
func StartConsensusProtocol() error {
	fmt.Println("ELECTION STARTED: my follower id is", FollowerId)

	// create Consensus job that returns an update channel and a channel for the func to receive followers on
	var updateChannel chan bool

	failErr := ""
	for len(DirectFollowersList) > 0 {
		lowestFollowerIp, lowestFollowerId := ScanFollowerList()
		// case 1: we are the lowest follower and likely to become leader
		if FollowerId == lowestFollowerId {
			fmt.Printf("Expecting to become leader, follower id is %d, ip is %s\n\n",
				FollowerId, lowestFollowerIp)

			electionLock.Lock()
			electionInProgress = true
			electionLock.Unlock()

			updateChannel, receiveFollowerChannel = StartElection()
			// block on update channel
			becameLeader := <-updateChannel
			// when channel returns and it's true then start become leader protocol
			// Assume PotentialFollowerIps was filled up

			electionLock.Lock()
			electionInProgress = false
			electionLock.Unlock()

			if becameLeader {
				fmt.Println("ELECTION COMPLETE: became the new Leader, my IP is", lowestFollowerIp)
				latestVersions, _ := BecomeLeader(PotentialFollowerIps, lowestFollowerIp)

				// Get entire dataset from Followers
				// 1) Scan for highest VersionNumber

				fmt.Printf(ERR_COL+"LatestVersions is: %+v\n"+ERR_END, latestVersions)
				var max int
				for _, v := range latestVersions {
					if max < v {
						max = v
					}
				}

				fmt.Println(fmt.Sprintf(ERR_COL+"MAX VERSION NUM IS: %d"+ERR_END, max))
				// Set the next WriteId to be after the highestLatestVersion and send back to node main
				writeIdCh <- max + 1

				// Aggregate any missing data
				if err := GetMissingData(max); err != nil {
					failErr = ERR_COL + "ELECTION PROTOCOL COULD NOT BE COMPLETE BECAUSE COULD NOT REBUILD DATA" + ERR_END
					break
				}

				// Notify Server of new leader
				fmt.Println(ERR_COL + "Notifying server of becoming leader" + ERR_END)

				var ignore string
				topic := structs.Topic{
					TopicName:   TopicName,
					MinReplicas: MinReplicas,
					Leaders:     []string{ClusterRpcAddr, MyAddr},
				}
				// If the server is down, we need to continuously attempt to notify it until
				// the server is aware of the new leader
				for {
					if err := ServerClient.Call("TServer.UpdateTopicLeader", &topic, &ignore); err != nil {
						checkError(err, "StartConsensusProtocol UpdateTopicLeader")
					}
					break
				}

				break
			}
		} else {
			// case 2: we should connect to the lowest follower
			time.Sleep(ELECTION_ATTEMPT_FOLLOW_WAIT * time.Second)
			fmt.Printf("Try to follow this leader: %s\n\n", lowestFollowerIp)
			err := PeerFollowThatNode(lowestFollowerIp, MyAddr, false)
			if err == nil {
				fmt.Printf("ELECTION COMPLETE: following %s\n\n", lowestFollowerIp)
				break
			} else {
				fmt.Printf("Could not follow node %s\n\n", lowestFollowerIp)
			}
		}
	}

	if failErr != "" {
		log.Print(failErr)
		return IncompleteDataError(failErr)
	}

	return nil
}

// Function Nominate is called when the peer decides there is a leader
// to join, it will return 2 channels, one for updating the status and one
// for receiving the update from peer rpc
func Nominate() (updateCh chan bool, receiveFollowerCh chan string) {
	updateCh = make(chan bool, 32)
	receiveFollowerCh = make(chan string, 32)
	timeoutCh := createTimeout(ELECTION_COMPLETE_TIMEOUT)

	go func() {
		for {
			select {
			// Receive a new FollowMe
			// end the election status
			case <-receiveFollowerCh:
				electionLock.Lock()
				/////////////
				updateCh <- true
				/////////////
				electionLock.Unlock()
				return

			case <-timeoutCh:
				// safe to close the timeout channel
				close(timeoutCh)
				updateCh <- false
				return
			}
		}
	}()
	return updateCh, receiveFollowerCh
}

// Function PeerAcceptThisNode should be called if a peer has accepted that
// this node should be leader. Currently is not responsible for ensuring that
// each peer has only sent their acceptance once. (maybe it should be though)
func PeerAcceptThisNode(ip string) error {
	electionLock.Lock()
	defer electionLock.Unlock()

	if electionInProgress {
		receiveFollowerChannel <- ip
		return nil
	} else if NodeMode == Leader {
		numPeers := PeerMap.GetCount()
		if numPeers == int(ClusterSize)-1 {
			return errors.New("Cluster is full. Cannot accept this Follower")
		}

		if numPeers > int(ClusterSize)-1 {
			fmt.Println(ERR_COL + "NUMBER OF PEERS EXCEEDS CLUSTER SIZE" + ERR_END)
			return errors.New("Cluster is full. Cannot accept this Follower")
		}

		// from clustering.go
		// it's likely this cluster is trying to join after
		// an election so just accept it
		LocalAddr, err := net.ResolveTCPAddr("tcp", ":0")
		if err != nil {
			return err
		}

		PeerAddr, err := net.ResolveTCPAddr("tcp", ip)
		if err != nil {
			return err
		}

		conn, err := net.DialTCP("tcp", LocalAddr, PeerAddr)
		if err != nil {
			return err
		}

		client := rpc.NewClient(conn)

		addPeer(ip, client, NodeDeathHandler, FollowerId)

		FollowerListLock.RLock()
		////////////////////////////
		// It's ok if it fails, gaps in follower ID sequence will not mean anything
		FollowerId += 1
		msg := FollowMeMsg{LeaderIp: MyAddr, FollowerIps: DirectFollowersList, YourId: FollowerId}
		fmt.Printf("Telling node with ip %s to follow me\n\n", ip)
		var latestVersion int
		err = client.Call("Peer.FollowMe", msg, &latestVersion)
		////////////////////////////
		FollowerListLock.RUnlock()
		if err != nil {
			return err
		}

		// Write lock when modifying the direct followers list
		FollowerListLock.Lock()
		DirectFollowersList[ip] = FollowerId
		FollowerListLock.Unlock()
		// unlock

		startPeerHb(ip)
		return nil
	} else {
		fmt.Println("Peer tried to connect to me, but am not leader and no election in progress")
		return fmt.Errorf(MyAddr, "is not a leader")
	}

}

// Function PeerFollowThatNode should be called if this node wants to follow
// the ipaddress of the given node
func PeerFollowThatNode(ip string, prpc string, isRejoin bool) error {
	electionLock.Lock()
	defer electionLock.Unlock()

	LocalAddr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		return err
	}

	PeerAddr, err := net.ResolveTCPAddr("tcp", ip)
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", LocalAddr, PeerAddr)
	if err != nil {
		return err
	}

	client := rpc.NewClient(conn)
	defer client.Close()

	var fileData []FileData

	msg := FollowMsg{Ip: prpc}
	err = client.Call("Peer.Follow", msg, &fileData)
	if err != nil {
		return err
	}

	LeaderConn = client

	if isRejoin {
		VersionListLock.Lock()
		defer VersionListLock.Unlock()
		VersionList = append(VersionList, fileData...)
		sortVersionList()
	}

	fmt.Println("Prev numWrites: %d, New numWrites after sync: %d", len(VersionList)-len(fileData), len(VersionList))
	return nil

}

// This function starts a consensus job. A caller should update the job when
// new messages are received using the update channel. Consensus is considered
// successful when there are a number of writes to the potential follower list that is
// at least the min connections needed for a cluster
func StartElection() (updateCh chan bool, receiveFollowerCh chan string) {
	updateCh = make(chan bool, 32)
	receiveFollowerCh = make(chan string, 1)
	timeoutCh := createTimeout(ELECTION_WAIT_FOR_RESULTS)

	go func() {
		for {
			select {
			// Receive a new FollowerIp
			// Add it to potential followers list
			// if we have enough then we end the election process
			case follower := <-receiveFollowerCh:
				electionLock.Lock()
				PotentialFollowerIps = append(PotentialFollowerIps, follower)
				fmt.Println("Added follower:", follower)
				if len(PotentialFollowerIps) >= int(MinReplicas) {
					updateCh <- true
					electionLock.Unlock()
					return
				}
				electionLock.Unlock()

			case <-timeoutCh:
				// safe to close the timeout channel
				close(timeoutCh)
				updateCh <- false
				return
			}
		}
	}()

	return updateCh, receiveFollowerCh
}

// Starts a goroutine that will write to the returned channel in <secs> seconds.
func createTimeout(secs time.Duration) chan bool {
	timeout := make(chan bool, 1)

	go func() {
		time.Sleep(secs * time.Second)
		timeout <- true
	}()

	return timeout
}

// Return the lowest follower's ID and the corresponding IP. Also removes
// the returned ID and IP from the list.
func ScanFollowerList() (lowestFollowerIp string, lowestFollowerId int) {
	FollowerListLock.Lock()
	defer FollowerListLock.Unlock()

	lowestFollowerId = math.MaxInt32
	lowestFollowerIp = ":0"

	for ip, id := range DirectFollowersList {
		fmt.Printf("ScanFollowerList: %s %d\n", ip, id)
	}

	// Scan for lowest follower ID
	for ip, id := range DirectFollowersList {
		if id < lowestFollowerId {
			lowestFollowerId = id
			lowestFollowerIp = ip
		}
	}
	delete(DirectFollowersList, lowestFollowerIp)
	return lowestFollowerIp, lowestFollowerId
}
