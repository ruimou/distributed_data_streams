package node

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Mode int

type Peer struct {
	HbChan   chan string
	PeerConn *rpc.Client
	DeathFn  func(string)
}

type PeerCMap struct {
	MapLock sync.RWMutex
	Map     map[string]Peer
}

///////////// Map functions for synchronized Peer Map //////////////////
func (pm *PeerCMap) Get(k string) (Peer, bool) {
	pm.MapLock.RLock()
	defer pm.MapLock.RUnlock()
	v, exists := pm.Map[k]
	return v, exists
}

func (pm *PeerCMap) Set(k string, v Peer) {
	pm.MapLock.Lock()
	defer pm.MapLock.Unlock()
	pm.Map[k] = v
}

func (pm *PeerCMap) Delete(k string) {
	pm.MapLock.Lock()
	defer pm.MapLock.Unlock()
	delete(pm.Map, k)
}

func (pm *PeerCMap) GetCount() int {
	pm.MapLock.Lock()
	defer pm.MapLock.Unlock()
	return len(pm.Map)
}

///////////// Map functions for concurrent Peer Map //////////////////

var LEADER_ID string = "leader"

const (
	Follower Mode = iota
	Leader
)

var NodeMode Mode = Follower

var PeerMap PeerCMap = PeerCMap{Map: make(map[string]Peer)}

var DirectFollowersList map[string]int // ip -> followerID

// Global incrementer for follower ID
// For followers this will be a static value of the assigned follower ID
var FollowerId int = 0
var FollowerListLock sync.RWMutex

var LeaderConn *rpc.Client

const CONSENSUS_FAILED_WAIT = 10 // Number of seconds to wait until retrying consensus protocol

// Args:
// ips - the list of potential followers should this current node get elected
// LeaderAddr - address which other followers should connect to for peer-to-peer communication
// Returns a list of the highest version numbers each follower has
func BecomeLeader(ips []string, LeaderAddr string) (latestVersions []int, err error) {
	// reference addr for consensus.go
	MyAddr = LeaderAddr
	DirectFollowersList = make(map[string]int)
	NodeMode = Leader

	latestVersions = make([]int, 0)
	successCount := 0
	for _, ip := range ips {
		if ip == LeaderAddr {
			continue
		}

		LocalAddr, err := net.ResolveTCPAddr("tcp", ":0")
		if err != nil {
			continue
		}

		PeerAddr, err := net.ResolveTCPAddr("tcp", ip)
		if err != nil {
			continue
		}

		conn, err := net.DialTCP("tcp", LocalAddr, PeerAddr)
		if err != nil {
			continue
		}

		client := rpc.NewClient(conn)

		addPeer(ip, client, NodeDeathHandler, FollowerId)

		// Write lock when modifying the direct followers list
		FollowerListLock.Lock()
		DirectFollowersList[ip] = FollowerId
		////////////////////////////

		// It's ok if it fails, gaps in follower ID sequence will not mean anything
		msg := FollowMeMsg{LeaderIp: LeaderAddr, FollowerIps: DirectFollowersList, YourId: FollowerId}
		fmt.Printf("Telling node with ip %s to follow me\n", ip)

		var latestVersion int
		err = client.Call("Peer.FollowMe", msg, &latestVersion)
		startPeerHb(ip)
		if err != nil {
			continue
		}

		latestVersions = append(latestVersions, latestVersion)
		FollowerId++
		////////////////////////////
		FollowerListLock.Unlock()

		successCount++
	}

	// Num followers required is ClusterSize - 1, since leader is counted
	go WatchFollowerCount(int(ClusterSize)-1, LeaderAddr)
	if successCount > 0 {
		return latestVersions, nil
	} else {
		fmt.Println("BecomeLeader: Could not connect to any followers!")
		return latestVersions, fmt.Errorf("Could not connect to any followers")
	}
}

func FollowLeader(msg FollowMeMsg, addr string) (err error) {
	fmt.Printf("FollowLeader: told to follow %s\n", msg.LeaderIp)
	FollowerListLock.Lock()
	DirectFollowersList = msg.FollowerIps
	FollowerListLock.Unlock()
	FollowerId = msg.YourId
	MyAddr = addr

	// Leader has given complete dataset
	if len(msg.Data) != 0 {
		VersionListLock.Lock()
		VersionList = msg.Data
		VersionListLock.Unlock()
	}

	LocalAddr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		return err
	}

	PeerAddr, err := net.ResolveTCPAddr("tcp", msg.LeaderIp)
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", LocalAddr, PeerAddr)
	if err != nil {
		return err
	}

	// check if there is already a leader connection; if so, kill it.
	oldLeader, ok := PeerMap.Get(LEADER_ID)
	if ok {
		oldLeader.HbChan <- "die"
	}

	LeaderConn = rpc.NewClient(conn)
	if receiveFollowerChannel != nil {
		receiveFollowerChannel <- msg.LeaderIp
	}

	LEADER_ID = msg.LeaderIp
	addPeer(LEADER_ID, LeaderConn, NodeDeathHandler, 0)
	startPeerHb(LEADER_ID)
	fmt.Println("FollowLeader: follower list is", msg.FollowerIps)

	return err
}

func ModifyFollowerList(follower ModFollowerListMsg, add bool) (err error) {
	FollowerListLock.Lock()
	defer FollowerListLock.Unlock()

	if add {
		_, exists := DirectFollowersList[follower.FollowerIp]
		if exists {
			err = errors.New("Clustering: Follower is already known")
		} else {
			fmt.Printf("Adding %s to follower list\n", follower.FollowerIp)
			DirectFollowersList[follower.FollowerIp] = follower.FollowerId
		}
	} else {
		_, exists := DirectFollowersList[follower.FollowerIp]
		if !exists {
			err = fmt.Errorf("Clustering: %s not known. Cannot remove follower",
				follower.FollowerIp)
			fmt.Println(err.Error())
		} else {
			fmt.Printf("Removing %s from follower list\n", follower.FollowerIp)
			delete(DirectFollowersList, follower.FollowerIp)
		}
	}

	return err
}

// Code from https://gist.github.com/jniltinho/9787946
func GeneratePublicIP() string {
	addrs, err := net.InterfaceAddrs()
	checkError(err, "GeneratePublicIP")

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String() + ":"
			}
		}
	}

	return "Could not find IP"
}

func checkError(err error, parent string) bool {
	if err != nil {
		fmt.Println(parent, ":: found error! ", err)
		return true
	}
	return false
}

// Adds a peer to the map
func addPeer(ip string, peerConn *rpc.Client, deathFn func(string), id int) {
	fmt.Printf("Adding %s to peer list\n", ip)
	newPeer := Peer{make(chan string, 8), peerConn, deathFn}
	PeerMap.Set(ip, newPeer)

	if NodeMode == Leader {
		go AddToFollowerLists(ip, id)
	}
}

// Starts heartbeat to a peer
func startPeerHb(ip string) {
	fmt.Printf("Starting hb goroutines for ip %s\n", ip)
	go peerHbSender(ip)
	go peerHbHandler(ip)
}

func AddToFollowerLists(ip string, id int) {
	FollowerListLock.RLock()
	defer FollowerListLock.RUnlock()
	var _ignored string
	msg := ModFollowerListMsg{ip, id}
	// send an add to follower list rpc to every follower
	for ip, _ := range DirectFollowersList {
		peer, ok := PeerMap.Get(ip)
		if !ok {
			fmt.Println("AddToFollowerLists :: ignoring this follower:", ip)
			continue
		}
		peer.PeerConn.Call("Peer.AddFollower", msg, &_ignored)
	}
}

func RemoveFromFollowerLists(ip string, id int) {
	FollowerListLock.RLock()
	defer FollowerListLock.RUnlock()
	var _ignored string
	msg := ModFollowerListMsg{ip, id}
	// send an add to follower list rpc to every follower
	for ip, _ := range DirectFollowersList {
		peer, ok := PeerMap.Get(ip)
		if !ok {
			fmt.Println("RemoveFromFollowerLists :: ignoring this follower:", ip)
			continue
		}
		peer.PeerConn.Call("Peer.RemoveFollower", msg, &_ignored)
	}
}

func NodeDeathHandler(ip string) {
	// This is the death function in the case that this peer
	// dies. There will be more functionality added to this
	// later for sure. Maybe put into separate function.
	fmt.Printf("Oh no, %s died!\n", ip)
	switch NodeMode {
	case Follower:
		if ip == LEADER_ID {
			fmt.Println("The leader has died, initiating consensus protocol")

			// Try the consensusProtocol indefinitely until there's a leader
			// If the consensus protocol failed, that means there were too many node failures
			// We sleep and try again hoping that nodes have rejoined
			for {
				err := StartConsensusProtocol()
				if err == nil {
					break
				}

				time.Sleep(CONSENSUS_FAILED_WAIT)
			}
		}
		// N/A since Followers do not connect to other Followers

	case Leader:
		fmt.Println("A node has died, need to remove it from everyone's follower list")
		FollowerListLock.Lock()
		id := DirectFollowersList[ip]
		delete(DirectFollowersList, ip)
		FollowerListLock.Unlock()
		RemoveFromFollowerLists(ip, id)

	default:
		// no default behavior
		fmt.Println("serious error occured in NodeDeathHandler")
	}
}

// Makes sure that there are always enough followers in the cluster. A leader
// will never stop being leader of a topic under normal operation, so this
// function has no exit conditions. Intended to be called as a goroutine.
func WatchFollowerCount(requiredNumFollowers int, LeaderAddr string) {
	fmt.Println("Watching follower count now")
	for {
		time.Sleep(3 * time.Second)
		count := PeerMap.GetCount()
		numToGet := requiredNumFollowers - count
		if numToGet <= 1 {
			continue
		}

		fmt.Printf("WatchFollowerCount: Need %d more followers!\n", numToGet)

		var nodeAddr string
		for i := 0; i < numToGet; i++ {
			err := ServerClient.Call("TServer.TakeNode", "", &nodeAddr)
			if err != nil {
				// Sleep and try again later, no point requesting any more
				break
			}

			conn, err := net.Dial("tcp", nodeAddr)
			if err != nil {
				continue
			}

			client := rpc.NewClient(conn)
			addPeer(nodeAddr, client, NodeDeathHandler, FollowerId)

			FollowerListLock.Lock()
			DirectFollowersList[nodeAddr] = FollowerId

			var latestVersion int

			VersionListLock.Lock()
			sortVersionList()
			VersionListLock.Unlock()

			msg := FollowMeMsg{LeaderAddr, DirectFollowersList, FollowerId, VersionList}
			fmt.Printf("Count Watcher: telling node with ip %s to follow me\n", nodeAddr)
			err = client.Call("Peer.FollowMe", msg, &latestVersion)
			startPeerHb(nodeAddr)
			if err != nil {
				continue
			}

			FollowerId++
			////////////////////////////
			FollowerListLock.Unlock()
		}
	}
}
