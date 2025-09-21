file(GLOB CLIENT_SRCS "client/*.cpp" "client/*.cc")
file(GLOB PROTO_SRCS "proto/*.cpp" "proto/*.cc")
file(GLOB RAFT_SRCS "raft/*.cpp" "raft/*.cc")
file(GLOB RPC_SRCS "rpc/*.cpp" "rpc/*.cc")
file(GLOB SERVER_SRCS "server/*.cpp" "server/*.cc")
file(GLOB STORAGE_SRCS "storage/*.cpp" "storage/*.cc")
set(FOSKV_SRCS
        ${CLIENT_SRCS}
        ${PROTO_SRCS}
        ${RAFT_SRCS}
        ${RPC_SRCS}
        ${SERVER_SRCS}
        ${STORAGE_SRCS} PARENT_SCOPE)
