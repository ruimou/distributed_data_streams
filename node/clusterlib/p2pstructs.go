package node

// Structs for node-based p2p messages

type FollowMeMsg struct {
	LeaderIp    string
	FollowerIps map[string]int
	YourId      int
	Data        []FileData
}

type ModFollowerListMsg struct {
	FollowerIp string
	FollowerId int
}

type PropagateWriteReq struct {
	Topic      string
	Data       string
	VersionNum int
	LeaderId   string
}

type FollowMsg struct {
	Ip             string       // Follower's ip address
	ContainingData map[int]bool // Map of versionNum -> bool that the follower node contains
}
