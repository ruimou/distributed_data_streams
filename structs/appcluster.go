package structs

/*
Standard message that the lib sends to the cluster leader
Topic = topic name so we know this is the correct topic to store information under
Id = let's use the IP address or something unique like that
Data= the GPS coordinates data
*/
type WriteMsg struct {
	Topic string
	Id string
	Data string
}