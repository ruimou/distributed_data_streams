package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"../structs"
	"./clusterlib"
)

type ClusterRpc struct{}

type PeerRpc struct{}

// ANSII Colour Codes for debugging
const ERR_COL = "\x1b[31;1m" // RED
const ERR_END = "\x1b[0m"    // Neutral

const WRITE_TIMEOUT_SEC = 10 // Time to wait for RPC call to peer nodes to confirm write

// ClusterRpcAddr is the ip:port that the producer/consumer API's interface with
// PeerRpcAddr is the ip:port connecting Leader -> Follower nodes
var ClusterRpcAddr, PeerRpcAddr, PublicIp string

var id int = 0

var (
	WriteLock *sync.Mutex
	WriteId   int
	WriteIdCh chan int
)

/*******************************
| Initializing global vars
********************************/
func InitializeDataStructs() {
	WriteLock = &sync.Mutex{}
	WriteId = 1
	WriteIdCh = make(chan int)

	// Because of the namespacing, package node updates the WriteId
	// and we need node's main package to get that write
	go func() {
		for {
			select {
			case wId := <-WriteIdCh:
				// Set new WriteId
				fmt.Println(ERR_COL+"New WriteId set %d"+ERR_END, wId)
				WriteLock.Lock()
				WriteId = wId
				WriteLock.Unlock()
			}
		}
	}()
}

/*******************************
| Cluster RPC Calls
********************************/
func ListenClusterRpc(ln net.Listener) {
	cRpc := ClusterRpc{}
	server := rpc.NewServer()
	server.RegisterName("Cluster", cRpc)
	ClusterRpcAddr = ln.Addr().String()
	node.ClusterRpcAddr = ClusterRpcAddr
	fmt.Println("ClusterRpc is listening on: ", ERR_COL+ClusterRpcAddr+ERR_END)

	server.Accept(ln)
}

func (c ClusterRpc) WriteToCluster(write structs.WriteMsg, _ignored *string) error {

	WriteLock.Lock()
	defer WriteLock.Unlock()

	if node.NodeMode == node.Leader {
		node.PeerMap.MapLock.RLock()

		writesCh := make(chan bool, 30)

		numRequiredWrites := node.MinReplicas
		// Subtract 1 because Leader is counted in ClusterSize and only Followers confirm Writes
		maxFailures := node.ClusterSize - numRequiredWrites - 1
		writeVerdictCh := node.CountConfirmedWrites(writesCh, numRequiredWrites, maxFailures)

		go func(wId int) {
			for ip, peer := range node.PeerMap.Map {
				var writeConfirmed bool

				resp := node.PropagateWriteReq{
					Topic:      write.Topic,
					VersionNum: wId,
					LeaderId:   PublicIp,
					Data:       write.Data,
				}

				// fmt.Println(ERR_COL+"WRITE ID BEFORE CONFIRMWRITE: %d"+ERR_END, WriteId)
				writeCall := peer.PeerConn.Go("Peer.ConfirmWrite", resp, &writeConfirmed, nil)

				go func(wc *rpc.Call) {
					select {
					case w := <-wc.Done:
						if w.Error != nil {
							checkError(w.Error, "ConfirmWriteRPC")
							fmt.Println("Peer [%s] REJECTED write", ERR_COL+ip+ERR_END)
							writesCh <- false
						}
					case <-time.After(WRITE_TIMEOUT_SEC):
						writesCh <- false
					}
				}(writeCall)
			}
		}(WriteId)

		// Block on writeVerdictCh
		writeSucceed := <-writeVerdictCh
		node.PeerMap.MapLock.RUnlock()

		if writeSucceed {
			if err := node.WriteNode(write.Topic, write.Data, WriteId); err != nil {
				fmt.Println(ERR_COL + "WRITING ERROR ON LEADER" + ERR_END)
				return err
			}
			WriteId++
			return nil
		}

		return node.InsufficientConfirmedWritesError("")
	}
	log.Println("WriteToCluster:: Node is not a leader. Should not have received Write")
	return errors.New("Node is not a leader. Cannot send Write")
}

func (c ClusterRpc) ReadFromCluster(topic string, response *[]string) error {
	topicData, err := node.ReadNode(topic)
	*response = topicData
	return err
}

/*******************************
| Peer RPC Calls
********************************/
func ListenPeerRpc(ln net.Listener) {
	pRpc := new(PeerRpc)
	server := rpc.NewServer()
	server.RegisterName("Peer", pRpc)
	PeerRpcAddr = ln.Addr().String()
	fmt.Println("PeerRpc is listening on: ", PeerRpcAddr)

	go server.Accept(ln)
}

// Server -> Node rpc that sets that node as a leader
// When it returns the node will have been established as leader
func (c PeerRpc) Lead(ips []string, clusterAddr *string) error {
	_, err := node.BecomeLeader(ips, PeerRpcAddr)
	*clusterAddr = ClusterRpcAddr
	return err
}

// Leader -> Node rpc that sets the caller as this node's leader
func (c PeerRpc) FollowMe(msg node.FollowMeMsg, latestData *int) error {
	err := node.FollowLeader(msg, PeerRpcAddr)
	if err == nil {
		version := node.GetLatestVersion()
		*latestData = version
	}
	return err
}

// Leader -> Node rpc that tells followers of new joining nodes
func (c PeerRpc) AddFollower(msg node.ModFollowerListMsg, _ignored *string) error {
	err := node.ModifyFollowerList(msg, true)
	return err
}

// Leader -> Node rpc that tells followers of nodes leaving
func (c PeerRpc) RemoveFollower(msg node.ModFollowerListMsg, _ignored *string) error {
	err := node.ModifyFollowerList(msg, false)
	return err
}

// Follower -> Leader rpc that is used to join this leader's cluster
// Used during the election process when attempting to connect to this leader
func (c PeerRpc) Follow(msg node.FollowMsg, syncData *[]node.FileData) error {
	fmt.Println("Peer.Follow from:", msg.Ip)
	err := node.PeerAcceptThisNode(msg.Ip)

	if err != nil {
		return err
	}
	// Send back the missing data
	missingData := node.DiffMissingData(msg.ContainingData)
	*syncData = missingData

	return nil
}

// Node -> Node RPC that is used to notify of liveliness
func (c PeerRpc) Heartbeat(ip string, reply *string) error {
	id++
	//fmt.Println("hb from:", ip, id)
	return node.PeerHeartbeat(ip, reply, id)
}

// Leader -> Follower RPC to commit write
func (c PeerRpc) ConfirmWrite(req node.PropagateWriteReq, writeOk *bool) error {
	// fmt.Println(ERR_COL+"ConfirmWrite:: VersionNum %d"+ERR_END, req.VersionNum)
	if err := node.WriteNode(req.Topic, req.Data, req.VersionNum); err != nil {
		checkError(err, "ConfirmWrite")
		return err
	}

	*writeOk = true
	return nil
}

func (c PeerRpc) GetWrites(requestedWrites map[int]bool, writeData *[]node.FileData) error {
	writes := make([]node.FileData, 0)
	for id := range requestedWrites {
		node.VersionListLock.Lock()

		found := false
		fmt.Println("\n\nVersionList in GETWRITES")
		fmt.Println("%+v", node.VersionList)
		for _, fdata := range node.VersionList {
			if fdata.Version == id {
				writes = append(writes, fdata)
				found = true
			}
		}

		*writeData = writes

		if !found {
			log.Println(ERR_COL+"Received GetWrites for new Leader but does not have requested write[%d]"+ERR_END, id)
		}
		node.VersionListLock.Unlock()
	}
	return nil
}

/*******************************
| Main
********************************/

// Args:
// serverIP
// dataPath - a valid, existing directory path that ends with /
func main() {
	serverIP := os.Args[1]
	dataPath := os.Args[2]

	PublicIp = node.GeneratePublicIP()
	fmt.Println("The public IP is: [%s], DataPath is: %s", ERR_COL+PublicIp+ERR_END, ERR_COL+dataPath+ERR_END)
	// Listener for clients -> cluster
	ln1, _ := net.Listen("tcp", PublicIp+"0")

	// Listener for server and other nodes
	ln2, _ := net.Listen("tcp", PublicIp+"0")

	InitializeDataStructs()
	// Open Filesystem on Disk
	node.MountFiles(dataPath, WriteIdCh)
	// Open Peer to Peer RPC
	ListenPeerRpc(ln2)
	// Connect to the Server
	node.InitiateServerConnection(serverIP, PeerRpcAddr)
	// Open Cluster to App RPC
	ListenClusterRpc(ln1)
}

func checkError(err error, parent string) bool {
	if err != nil {
		log.Println(parent, ":: found error! ", err)
		return true
	}
	return false
}
