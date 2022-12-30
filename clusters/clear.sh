# clear node0 data
rm "`dirname $0`"/node0/data/logs/wal.wal
rm "`dirname $0`"/node0/data/raft/wal.wal

# clear node1 data
rm "`dirname $0`"/node1/data/logs/wal.wal
rm "`dirname $0`"/node1/data/raft/wal.wal

# clear node2 data
rm "`dirname $0`"/node2/data/logs/wal.wal
rm "`dirname $0`"/node2/data/raft/wal.wal
