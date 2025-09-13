file(GLOB RPC_FILES "*.cpp")
add_library(foskv-rpc STATIC ${RPC_FILES})
set(FOSKV_RPC_TARGET foskv-rpc PARENT_SCOPE)