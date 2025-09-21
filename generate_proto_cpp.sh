protoc -I=src/foskv/rpc/pb src/foskv/rpc/pb/*.proto --cpp_out=include/foskv/rpc/pb
mv include/foskv/rpc/pb/*.cc src/foskv/rpc/pb