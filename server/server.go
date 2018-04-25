package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"../structs"
	c "./concurrentlib"
)

///////////////////////////////////////////////////////////////////////////////////////////////////
// ERRORS
///////////////////////////////////////////////////////////////////////////////////////////////////

// Just for debugging
const GREEN_COL = "\x1b[32;1m"
const ERR_COL = "\x1b[31;1m"
const ERR_END = "\x1b[0m"

type AddressAlreadyRegisteredError string

func (e AddressAlreadyRegisteredError) Error() string {
	return fmt.Sprintf("Server: node address already registered [%s]", string(e))
}

type UnknownNodeError string

func (e UnknownNodeError) Error() string {
	return fmt.Sprintf("Server: unknown node address heartbeat [%s]", string(e))
}

type DuplicateTopicNameError string

func (e DuplicateTopicNameError) Error() string {
	return fmt.Sprintf("Server: Topic: [%s] already exists", string(e))
}

type TopicDoesNotExistError string

func (e TopicDoesNotExistError) Error() string {
	return fmt.Sprintf("Server: Topic: [%s] does not exist", string(e))
}

type InsufficientNodesForCluster string

func (e InsufficientNodesForCluster) Error() string {
	return fmt.Sprintf("Server: There are not enough available nodes to form a topic's cluster")
}

// END OF ERRORS
///////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////
// DATA STRUCTURES
///////////////////////////////////////////////////////////////////////////////////////////////////

type TServer struct{}

type Config struct {
	NodeSettings structs.NodeSettings `json:"node-settings"`
	RpcIpPort    string               `json:"rpc-ip-port"`
	DataPath     string               `json:"data-filepath"`
}

const (
	topicFile string = "./topics.json"
)

type AllNodes struct {
	sync.RWMutex
	all map[string]*structs.Node // unique ip:port idenitifies a node
}

var (
	config Config

	allNodes    = AllNodes{all: make(map[string]*structs.Node)}
	orphanNodes = c.Orphanage{Orphans: make([]structs.Node, 0)}
	topics      = c.TopicCMap{Map: make(map[string]structs.Topic)}

	errLog = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
)

func readConfigOrDie(path string) {
	file, err := os.Open(path)
	handleErrorFatal("config file", err)

	buffer, err := ioutil.ReadAll(file)
	handleErrorFatal("read config", err)

	err = json.Unmarshal(buffer, &config)
	handleErrorFatal("parse config", err)
}

// Register Nodes
func (s *TServer) Register(n string, nodeSettings *structs.NodeSettings) error {
	allNodes.Lock()
	defer allNodes.Unlock()

	for _, node := range allNodes.all {
		if node.Address == n {
			return AddressAlreadyRegisteredError(n)
		}
	}

	outLog.Println("Register::Connecting to address: ", n)
	localAddr, err := net.ResolveTCPAddr("tcp", ":0")
	checkError(err, "GetPeers:ResolvePeerAddr")

	nodeAddr, err := net.ResolveTCPAddr("tcp", n)
	checkError(err, "GetPeers:ResolveLocalAddr")

	conn, err := net.DialTCP("tcp", localAddr, nodeAddr)
	checkError(err, "GetPeers:DialTCP")

	client := rpc.NewClient(conn)

	// Add to orphan nodes
	orphanNodes.Append(structs.Node{
		Address: n,
		Client:  client})

	allNodes.all[n] = &structs.Node{
		Address:         n,
		Client:          client,
		RecentHeartbeat: time.Now().UnixNano()}

	go monitor(n, time.Millisecond*time.Duration(config.NodeSettings.HeartBeat))

	*nodeSettings = config.NodeSettings

	outLog.Printf("Got Register from %s\n", GREEN_COL+n+ERR_END)

	return nil
}

// Rejoin Nodes to a cluster
func (s *TServer) Rejoin(n string, nodeSettings *structs.NodeSettings) error {
	allNodes.Lock()
	defer allNodes.Unlock()

	for _, node := range allNodes.all {
		if node.Address == n {
			return AddressAlreadyRegisteredError(n)
		}
	}

	outLog.Println("Register::Connecting to address: ", n)
	localAddr, err := net.ResolveTCPAddr("tcp", ":0")
	checkError(err, "GetPeers:ResolvePeerAddr")

	nodeAddr, err := net.ResolveTCPAddr("tcp", n)
	checkError(err, "GetPeers:ResolveLocalAddr")

	conn, err := net.DialTCP("tcp", localAddr, nodeAddr)
	checkError(err, "GetPeers:DialTCP")

	client := rpc.NewClient(conn)

	allNodes.all[n] = &structs.Node{
		Address:         n,
		Client:          client,
		RecentHeartbeat: time.Now().UnixNano()}

	go monitor(n, time.Millisecond*time.Duration(config.NodeSettings.HeartBeat))

	*nodeSettings = config.NodeSettings

	outLog.Printf("Got Rejoin from %s\n", GREEN_COL+n+ERR_END)

	return nil
}

// Writes to disk any connections that have been made to the server along
// with their corresponding topics (if any)
func updateNodeMap(addr string, topicName string, server *TServer) error {
	_, err := os.OpenFile(topicFile, os.O_WRONLY, 0644)

	if err == nil {
		// f.Write
		return nil
	}

	return err
}

func monitor(k string, heartBeatInterval time.Duration) {
	time.Sleep(time.Second * 10)
	for {
		allNodes.Lock()
		if time.Now().UnixNano()-allNodes.all[k].RecentHeartbeat > int64(heartBeatInterval) {
			outLog.Printf("%s timed out\n", ERR_COL+allNodes.all[k].Address+ERR_END)
			delete(allNodes.all, k)
			allNodes.Unlock()
			return
		}
		outLog.Printf("%s is alive\n", allNodes.all[k].Address)
		allNodes.Unlock()
		time.Sleep(heartBeatInterval)
	}
}

func (s *TServer) HeartBeat(addr string, _ignored *bool) error {
	allNodes.Lock()
	defer allNodes.Unlock()
	if _, ok := allNodes.all[addr]; !ok {
		return UnknownNodeError(addr)
	}

	allNodes.all[addr].RecentHeartbeat = time.Now().UnixNano()

	return nil
}

func (s *TServer) TakeNode(ignored string, nodeAddr *string) error {
	orphanNodes.Lock()
	defer orphanNodes.Unlock()

	if orphanNodes.Len <= 0 {
		outLog.Println("TakeNode: No nodes available")
		return fmt.Errorf("No nodes available for taking")
	}

	node := orphanNodes.DropN(1)
	*nodeAddr = node[0].Address
	outLog.Printf("TakeNode: gave %s\n", *nodeAddr)

	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Producer API RPC
///////////////////////////////////////////////////////////////////////////////////////////////////

func (s *TServer) CreateTopic(topicName *string, topicReply *structs.Topic) error {
	// Check if there is already a Topic with the same name
	if _, ok := topics.Get(*topicName); !ok {
		orphanNodes.Lock()
		defer orphanNodes.Unlock()

		for {
			if orphanNodes.Len >= uint32(config.NodeSettings.ClusterSize) {
				lNode := orphanNodes.Orphans[0]

				allNodes.Lock()
				node, exists := allNodes.all[lNode.Address]
				allNodes.Unlock()

				// Node may have disconnected and was removed from allNodes map. We do not
				// remove stale nodes from orphanNodes
				if !exists {
					log.Printf("Discrepancy in orphan nodes vs. Node Map. [%s] does not exist in NodeMap\n", lNode.Address)
					log.Printf("All nodes: %+v\n", allNodes.all)
					log.Printf("Orphan Nodes: %+v\n", orphanNodes.Orphans)
					orphanNodes.DropN(1)
					continue
				}

				orphanIps := make([]string, 0)

				for i, orphan := range orphanNodes.Orphans {
					if i >= int(config.NodeSettings.ClusterSize) {
						break
					}

					orphanIps = append(orphanIps, orphan.Address)
				}

				var leaderClusterRpc string
				if err := node.Client.Call("Peer.Lead", orphanIps, &leaderClusterRpc); err != nil {
					errLog.Printf("Node [%s] could not accept Leader position.\n", lNode.Address)
					return err
				}

				orphanNodes.DropN(int(config.NodeSettings.ClusterSize))

				topic := structs.Topic{
					TopicName:   *topicName,
					MinReplicas: config.NodeSettings.MinReplicas,
					Leaders:     []string{leaderClusterRpc, lNode.Address}}

				topics.Set(*topicName, topic, config.DataPath)
				*topicReply = topic
				return nil
			} else {
				break
			}
		}

		return InsufficientNodesForCluster("")
	}

	return DuplicateTopicNameError(*topicName)
}

func (s *TServer) GetTopic(topicName *string, topicReply *structs.Topic) error {
	if topic, ok := topics.Get(*topicName); ok {
		*topicReply = topic
		return nil
	}

	return TopicDoesNotExistError(*topicName)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers for Leader promotion/demotion
///////////////////////////////////////////////////////////////////////////////////////////////////

func (s *TServer) UpdateTopicLeader(topic *structs.Topic, ignore *string) (err error) {
	fmt.Println(ERR_COL + "TOPIC LEADER IS BEING UPDATED" + ERR_END)
	return topics.Set(topic.TopicName, *topic, config.DataPath)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Disk operations to survive server failure
///////////////////////////////////////////////////////////////////////////////////////////////////

func readDiskData() error {
	data, err := ioutil.ReadFile(config.DataPath)
	if err != nil {
		fmt.Println("ReadFile failed")
		return err
	}

	if len(data) == 0 {
		return nil
	}

	var topicsJson []structs.Topic
	if err = json.Unmarshal(data, &topicsJson); err != nil {
		fmt.Println("Unmarshal failed")
		return err
	}

	fmt.Println("Looking for old topics")

	// Not concurrent so it's fine to not lock
	for _, topic := range topicsJson {
		topics.Map[topic.TopicName] = topic
		fmt.Println("TOPIC:", topic)
	}

	return nil
}

func main() {
	//gob.Register(&net.TCPAddr{})

	// Pass in IP as command line argument
	path := flag.String("c", "", "Path to the JSON config")
	flag.Parse()

	if *path == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	readConfigOrDie(*path)

	// Check if there was previous data on this server

	if _, err := os.Stat(config.DataPath); os.IsNotExist(err) {
		fmt.Println(GREEN_COL + "No Previous Data" + ERR_END)
		f, err := os.Create(config.DataPath)
		if err != nil {
			handleErrorFatal("Couldn't create file "+config.DataPath, err)
		}

		f.Sync()
		f.Close()
	} else {
		fmt.Println(ERR_COL + "PREVIOUS DATA ON SERVER" + ERR_END)
		if err = readDiskData(); err != nil {
			handleErrorFatal("Could not read topics data from disk", err)
		}
	}

	rand.Seed(time.Now().UnixNano())

	// Set up Server RPC
	tServer := new(TServer)
	server := rpc.NewServer()
	server.Register(tServer)

	l, err := net.Listen("tcp", config.RpcIpPort)

	handleErrorFatal("listen error", err)
	outLog.Printf("Server started. Receiving on %s\n", config.RpcIpPort)

	if err != nil {
		fmt.Sprintln("Server: Error initializing RPC Listener")
		return
	}

	for {
		conn, _ := l.Accept()
		go server.ServeConn(conn)
	}
}

func handleErrorFatal(msg string, e error) {
	if e != nil {
		errLog.Fatalf("%s, err = %s\n", msg, e.Error())
	}
}

func checkError(err error, parent string) bool {
	if err != nil {
		errLog.Println(parent, ":: found error! ", err)
		return true
	}
	return false
}
