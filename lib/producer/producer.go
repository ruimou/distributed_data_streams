/*
This file contains functions that should be used by producer clients.
*/

package producer

import (
	"fmt"
	"net"
	"net/rpc"

	"../../structs"
)

type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("Not connected.")
}

type ConnectionError string

func (e ConnectionError) Error() string {
	return fmt.Sprintf("Error attempting to connect: %s", string(e))
}

// Object that should be used by a client for writing. This is returned by
// OpenTopic.
type WriteSession struct {
	topicName  string
	clientId   string
	leaderConn *rpc.Client
}

// Function will first try to get topic data. If the topic does not
// exist, then it will try to create it. If create fails, it will again
// try to get topic. If all of these fails, returns an error.
//
// Parameter serverAddr should be an ip:port combination.
func OpenTopic(topicName string, serverAddr string, myId string) (*WriteSession, error) {
	fmt.Println("Attempting server dial")
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return nil, ConnectionError(err.Error())
	}

	fmt.Println("Successfully dialed server")

	serverRpc := rpc.NewClient(conn)
	defer serverRpc.Close()

	var topicData structs.Topic

	// First attempt to get topic
	fmt.Println("Attempting get topic")
	err = serverRpc.Call("TServer.GetTopic", &topicName, &topicData)
	if err == nil {
		return connectToLeader(topicData, myId)
	}

	// Attempt to create topic
	fmt.Println("Could not get topic; attempting to create topic:", err)
	err = serverRpc.Call("TServer.CreateTopic", &topicName, &topicData)
	if err == nil {
		return connectToLeader(topicData, myId)
	}

	// Attempt to get topic again in case of race condition
	fmt.Println("Could not create topic; attempting to get topic again:", err)
	err = serverRpc.Call("TServer.GetTopic", &topicName, &topicData)
	if err == nil {
		return connectToLeader(topicData, myId)
	}

	fmt.Println("Final error, quitting:", err)
	return nil, fmt.Errorf("Could not get or create topic.\n")
}

// Function closes the topic. Returns an error if not currently connected or
// if somehow close returns an error.
func (s *WriteSession) Close() error {
	if s.leaderConn == nil {
		return DisconnectedError("")
	}

	err := s.leaderConn.Close()
	if err != nil {
		return err
	}

	s.leaderConn = nil
	return nil
}

// Function writes to topic. Returns an error if not currently connected, or if
// there is a connection error.
func (s *WriteSession) Write(datum string) error {
	if s.leaderConn == nil {
		return DisconnectedError("")
	}

	var req structs.WriteMsg
	var ignore string

	req.Topic = s.topicName
	req.Id = s.clientId
	req.Data = datum

	return s.leaderConn.Call("Cluster.WriteToCluster", req, &ignore)
}

// Attempt to connect to a topic leader for writing
func connectToLeader(topicData structs.Topic, myId string) (*WriteSession, error) {
	fmt.Println("Attempting to connect to leader")
	// There should only be one leader in the current implementation, so
	// only attempt the 0th index.
	conn, err := net.Dial("tcp", topicData.Leaders[0])
	if err != nil {
		fmt.Printf("Could not connect to leader %s of topic %s\n",
			topicData.TopicName, topicData.Leaders[0])
		return nil, ConnectionError(err.Error())
	}

	leaderConn := rpc.NewClient(conn)
	if leaderConn == nil {
		return nil, ConnectionError("rpc.NewClient failed")
	}

	fmt.Println("Successfully connected to leader:", topicData.Leaders)
	return &WriteSession{
		topicData.TopicName,
		myId,
		leaderConn}, nil
}
