package structs

import (
	"net/rpc"
)

type NodeSettings struct {
	MinReplicas uint8  `json:"min-replicas"`
	HeartBeat   uint32 `json:"heartbeat"`
	ClusterSize uint8  `json:"cluster-size"`
}

type Node struct {
	Address         string
	Client          *rpc.Client
	RecentHeartbeat int64
	IsLeader        bool
}

type Topic struct {
	TopicName   string
	MinReplicas uint8
	Leaders     []string // [0] = ClusterRpcAddr
					 	 // [1] = PeerRpcAddr
}

////////////////////// RPC STRUCTS //////////////////////

/////////////////// RPC STRUCTS END ////////////////////
