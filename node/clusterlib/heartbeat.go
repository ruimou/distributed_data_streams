package node

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"

	"../../structs"
)

const HBTIMEOUT = 4
const HBINTERVAL = 2
const SERVER_RECONNECT_WAIT = 10

var ServerClient *rpc.Client

var (
	MinReplicas uint8
	ClusterSize uint8
	HBInterval  uint32
)

var serverDeathCh chan bool

func InitiateServerConnection(serverIp, peerAddr string) {
	serverDeathCh = make(chan bool)

	serverConnProtocol := func() error {
		// Connect to the Server
		fmt.Println("Connecting to server...")
		if err := ConnectToServer(serverIp); err != nil {
			return err
		}

		err := AttemptRejoin(peerAddr)
		if err != nil {
			ServerRegister(peerAddr)
		}
		go ServerHeartBeat(peerAddr)
		return nil
	}

	// When the server dies, try to reconnect to server every SERVER_RECONNECT_WAIT seconds
	go func() {
		for {
			select {
			case <-serverDeathCh:
				log.Println(ERR_COL + "SERVER HAS DIED" + ERR_END)
				for {
					time.Sleep(SERVER_RECONNECT_WAIT * time.Second)
					if err := serverConnProtocol(); err == nil {
						break
					}
				}
			}
		}
	}()

	if err := serverConnProtocol(); err != nil {
		log.Fatalf(err.Error())
	}
}

func ConnectToServer(ip string) error {
	LocalAddr, _ := net.ResolveTCPAddr("tcp", ":0")
	ServerAddr, _ := net.ResolveTCPAddr("tcp", ip)
	conn, err := net.DialTCP("tcp", LocalAddr, ServerAddr)
	if err != nil {
		return errors.New("Could not connect to server")
	} else {
		log.Println("Connecting to server on:", conn.LocalAddr().String())
		ServerClient = rpc.NewClient(conn)
		return nil
	}
}

func ServerRegister(addr string) {
	var resp structs.NodeSettings
	err := ServerClient.Call("TServer.Register", addr, &resp)
	if err != nil {
		fmt.Printf("Error in heartbeat::Register()\n%s\n", err)
	}
	MinReplicas = resp.MinReplicas
	ClusterSize = resp.ClusterSize
	HBInterval = resp.HeartBeat
}

func ServerHeartBeat(addr string) {
	var _ignored bool
	fmt.Printf("starting server hb of: %d\n", HBInterval)
	interval := time.Duration(HBInterval / 2)
	heartbeat := time.Tick(interval * time.Millisecond)
	for {
		select {
		case <-heartbeat:
			err := ServerClient.Call("TServer.HeartBeat", addr, &_ignored)
			if err != nil {
				serverDeathCh <- true
				return
			}
		}
	}
}

// Logic of the heartbeat function
func PeerHeartbeat(ip string, reply *string, id int) error {
	*reply = "ok"

	// Check if peer is in map, then write to its heartbeat channel
	peer, ok := PeerMap.Get(ip)
	if !ok {
		fmt.Println("PeerHeartbeat: could not find", ip)
		return fmt.Errorf("%s not in peer list", ip)
	}
	//fmt.Println(id,": was successful")
	peer.HbChan <- "hb"

	return nil
}

// Handles periodic sending of heartbeats to a single peer. The RPC connection
// should already be established, and the peer's channel should already be put
// into the PeerMap structure.
func peerHbSender(id string) {
	peer, ok := PeerMap.Get(id)
	if !ok {
		return
	}

	// Note, could use a ticker, but not sure what behaviour would be if
	// tick occurs while not receiving, eg. occurs in the call.Done branch
	// before it starts waiting on timeout to complete
	timeout := createPeerTimeout(HBINTERVAL)

	for {
		arg := MyAddr
		var reply string

		// fmt.Printf("Sending Peer.Heartbeat, arg is %s\n", arg)
		call := peer.PeerConn.Go("Peer.Heartbeat", arg, &reply, nil)
		if call == nil {
			// connection is dead - error
			peer.HbChan <- "die"
			return
		}

		select {
		case <-timeout:
			// timeout occurs before call returns - error
			peer.HbChan <- "die"
			return
		case <-call.Done:
			if call.Error != nil {
				fmt.Printf("Peer.Heartbeat error: %s\n", call.Error)
				peer.HbChan <- "die"
				return
			}

			// Wait until timeout is done so that full interval has passed
			// before sending again.
			<-timeout
			timeout = createTimeout(HBINTERVAL)
		}
	}
}

// Handles heartbeat timeout checking for peers. Note that there is a check for
// both sending and receiving heartbeats from a peer. Seems unlikely, but there
// could be a case where a peer is taking heartbeats just fine, but is not
// sending any back.
func peerHbHandler(id string) {
	// Sanity checks - shouldn't ever happen
	peer, ok := PeerMap.Get(id)
	if !ok {
		return
	}

	// Cleanup routine for this long running function. There is more than
	// one exit point in the loop  so defer this exit function. This is the
	// single point of deletion for a peer connection.
	defer func() {
		// Delete peer from the map - can't talk to this guy anymore.
		PeerMap.Delete(id)
		peer.PeerConn.Close()

		fmt.Printf("Peer %s connection has died, calling DeathFn\n", id)
		peer.DeathFn(id)
	}()

	// Heartbeat checking loop - does not exit until a peer disconnects
	for {
		timeout := createTimeout(HBTIMEOUT)

		select {
		case <-timeout:
			// Peer failure detected!
			return
		case msg := <-peer.HbChan:
			switch msg {
			case "die":
				// Peer failure by another function, exit
				return
			case "hb":
				// fmt.Printf("HbHandler: received hb from <%s>\n", id)
				continue
			}
		}
	}
}

// Starts a goroutine that will write to the returned channel in <secs> seconds.
func createPeerTimeout(secs time.Duration) (timeout chan bool) {
	timeout = make(chan bool, 1)
	go func() {
		time.Sleep(secs * time.Second)
		timeout <- true
	}()
	return timeout
}

// Function to check if there was a previous topic and we should join it now
func AttemptRejoin(pRpcAddr string) error {
	var topic structs.Topic
	var resp structs.NodeSettings
	// A topic name exists but a followerId is not set
	if len(TopicName) > 0 && FollowerId == 0 {
		err := ServerClient.Call("TServer.GetTopic", TopicName, &topic)
		if err != nil {
			fmt.Printf("Error in heartbeat::Rejoin()::GetTopic\n%s\n", err)
			return err
		}

		// Attempt to follow the leader
		rejoin := true
		err = PeerFollowThatNode(topic.Leaders[1], pRpcAddr, rejoin)
		if err != nil {
			fmt.Printf("Error in heartbeat::Rejoin()::FollowLeader\n%s\n", err)
			return err
		}

		// Call Rejoin instead of Register and do register things
		err = ServerClient.Call("TServer.Rejoin", pRpcAddr, &resp)
		if err != nil {
			fmt.Printf("Error in heartbeat::Rejoin()::Rejoin\n%s\n", err)
			return err
		}
		MinReplicas = resp.MinReplicas
		ClusterSize = resp.ClusterSize
		HBInterval = resp.HeartBeat

		return nil
	}
	return fmt.Errorf("Nothing to rejoin")
}
