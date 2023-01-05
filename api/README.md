# gRPC API Definitions

This directory contains gRPC API definitions. Most of the raft definitions follow the original paper with a few exeptions in the append entries response for processing. 

## Usage
Update kv.proto file. Then run this command to generate.

```
cd api
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    kv.proto
```
