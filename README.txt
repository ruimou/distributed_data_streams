# PubSub Streams

This is a distributed data stream system for replicating a data set among several nodes and ensuring safety against node failures. Based off of Kafka.

Includes:

* A server that can manage the nodes on the system and arrange them into clusters
* Nodes that arrange into leader-follower clusters that employ a quorum-based consensus for replication of writes to the leader
* Consensus protocol for electing a new leader in case a leader fails
* Example apps that lookup the leader in the server and proceed to read/write to the leader

-----

// Requirements for config.json 

```
{
    "rpc-ip-port": ":12345",
    "node-settings": {
        "heartbeat": 10000,
        "min-replicas": 3,
        "cluster-size": 5
    }
}
```

`cluster-size` is the number of nodes in a cluster *including* the leader. 
So if `cluster-size` is 3, there will be 1 Leader node and 2 Follower nodes.

`min-replicas` is the minimum number of nodes a Write must be replicated
on in order for a Write call to succeed. For our system, we assume
the number is at least a majority of Follower nodes. i.e. If `cluster-size`
is 5, there are 4 Follower nodes. `min-replicas` >= 3.
We allow users to set a number higher in order to guarantee better replication
for Writes. 