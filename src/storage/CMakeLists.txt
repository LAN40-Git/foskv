file(GLOB STORAGE_FILES "*.cpp")
add_library(foskv-storage STATIC ${STORAGE_FILES})
set(FOSKV_STORAGE_TARGET foskv-storage PARENT_SCOPE)