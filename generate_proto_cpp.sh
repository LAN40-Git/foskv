protoc -I=./include ./include/foskv/api/foskvserverpb/rpc.proto ./include/foskv/api/foskvserverpb/raft_internal.proto --cpp_out=./include
protoc -I=./include ./include/foskv/raft/raft.proto --cpp_out=./include
protoc -I=./include ./include/foskv/rpc/rpc_header.proto --cpp_out=./include
mv ./include/foskv/api/foskvserverpb/*.cc ./src/api/foskvserverpb
mv ./include/foskv/raft/raft.pb.cc ./src/raft
mv ./include/foskv/rpc/rpc_header.pb.cc ./src/rpc