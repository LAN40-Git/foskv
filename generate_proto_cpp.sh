protoc -I=src/foskv/proto src/foskv/proto/*.proto --cpp_out=include/foskv/proto
mv include/foskv/proto/*.cc src/foskv/proto