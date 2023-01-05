# Distributed Key-Value Store

A distributed key-value store written in Go using gRPC remote procedure calls as means of server communication.

- Implements log replication and leader election portions of the Raft Consensus algorithm from scratch.
- Uses a write-ahead log based key-value store written from scratch.
- Includes a client to make gRPC requests to the cluster and find the leader node.
- Snapshotting and membership changes are NOT implemented.

## Usage

Build the server.

```
cd server
go build .
```

Build the client.
```
cd client
go build .
```

Run a 3 node cluster.
```
./scripts/start.sh
```
- This starts a cluster of 3 node running on ports 4444, 14444 and 24444.
- Each node can be ran without the full starting script using the scripts in clusters/node[0|1|2].
- Persistant data is stored in the data directory of each node.
- Each node uses 2 instances of the WAL storage. One for raft data (votedFor and currentTerm) and one for all the logs.

### Using the client
```
./scripts/start_client.sh
```
- This will start the client REPL to help make requests to the cluster. The script contains configuration for the cluster above.
- The client will randomly try to make a request to one of the servers in the cluster. If it fails due to not being the leader the next requests will be to the predicted leader. If it fails due to other errors it will try randomly again.

GET a key
```
GET [key]
```

PUT a key-value pair
```
PUT [key] [value]
```

DELETE a key
```
DELETE [key]
```
