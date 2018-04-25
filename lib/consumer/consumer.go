package consumer

import (
	"fmt"
	"net"
	"net/rpc"

	"../../structs"
)

///////////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// There is at least one successful Write operation to
// this Cluster that could not be retrieved
type DataUnvailableError string

func (e DataUnvailableError) Error() string {
	return fmt.Sprintf("Consumer: Unavailable data")
}

// Cannot connect to server
type DisconnectedServerError string

func (e DisconnectedServerError) Error() string {
	return fmt.Sprintf("Consumer: Cannot connect to server on [%s]", string(e))
}

// No Cluster with the given TopicName exists
type TopicDoesNotExistError string

func (e TopicDoesNotExistError) Error() string {
	return fmt.Sprintf("Consumer: Topic with name [%s] does not exist", string(e))
}

type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("Not connected.")
}

type ConnectionError string

func (e ConnectionError) Error() string {
	return fmt.Sprintf("Error attempting to connect: %s", string(e))
}

// </ERROR DEFINITIONS>
///////////////////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////////////////////////
// <API>

// Object that should be used by a client for reading. This is returned by
// OpenTopic.
type ReadSession struct {
	topicName  string
	clientId   string
	leaderConn *rpc.Client
}

//type Consumer interface {
// Returns the Topic associated with the given gpsCoordinates
// Can return the following errors:
// - TopicDoesNotExistError
//GetCurrentLocationCluster(gpsCoordinates GPSCoordinates) (topic structs.Topic, err error)

// Returns the Topic the client will connect to and read from
// Can return the following errors:
// - TopicDoesNotExistError
// - DisconnectedServerError
//OpenTopic(topicName string) (topic structs.Topic, err error)

// Returns a list of all GPSCoordinates that have been written to the Topic
// Can returen the following errors:
// - DataUnvailableError
//Read(topicName string) (gpsCoordinates structs.GPSCoordinates, err error)
//}

func GetTopic(topicName string, serverAddr string, myId string) (*ReadSession, error) {
	fmt.Println("Attempting server dial")
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return nil, DisconnectedServerError(err.Error())
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

	// Attempt to get topic again in case of race condition
	fmt.Println("Could not create topic; attempting to get topic again:", err)
	err = serverRpc.Call("TServer.GetTopic", &topicName, &topicData)
	if err == nil {
		return connectToLeader(topicData, myId)
	}

	fmt.Println("Final error, quitting:", err)
	return nil, fmt.Errorf("Could not get topic.\n")
}

// Function closes the topic. Returns an error if not currently connected or
// if somehow close returns an error.
func (s *ReadSession) Close() error {
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

// Function reads from topic. Returns an error if not currently connected, or if
// there is a connection error.
func (s *ReadSession) Read() ([]string, error) {
	if s.leaderConn == nil {
		return nil, DisconnectedError("")
	}

	req := s.topicName
	var data []string

	err := s.leaderConn.Call("Cluster.ReadFromCluster", req, &data)

	return data, err
}

// </API>
///////////////////////////////////////////////////////////////////////////////////////////////////

// Attempt to connect to a topic leader for writing
func connectToLeader(topicData structs.Topic, myId string) (*ReadSession, error) {
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
	return &ReadSession{
		topicData.TopicName,
		myId,
		leaderConn}, nil
}
