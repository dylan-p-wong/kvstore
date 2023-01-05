# clear node0 data
rm "`dirname \`pwd\``"/clusters/node0/data/logs/wal.wal
rm "`dirname \`pwd\``"/clusters/node0/data/raft/wal.wal

# clear node1 data
rm "`dirname \`pwd\``"/clusters/node1/data/logs/wal.wal
rm "`dirname \`pwd\``"/clusters/node1/data/raft/wal.wal

# clear node2 data
rm "`dirname \`pwd\``"/clusters/node2/data/logs/wal.wal
rm "`dirname \`pwd\``"/clusters/node2/data/raft/wal.wal
