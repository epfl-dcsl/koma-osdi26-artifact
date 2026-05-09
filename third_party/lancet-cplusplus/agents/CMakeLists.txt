# MIT License
#
# Copyright (c) 2019-2021 Ecole Polytechnique Federale Lausanne (EPFL)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
cmake_minimum_required( VERSION 3.12...3.13 )

project( Agent
  VERSION 1.0.0
  DESCRIPTION "Agent for Lancet")

include(./cmake/common.cmake)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS_RELEASEASSERT "-O3")
set(CMAKE_CXX_STANDARD 17)

# first, lets find the flags we need
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_library(LIBRT rt REQUIRED)
find_library(LIBM m REQUIRED)
find_library(LIB_SSL ssl REQUIRED)
find_library(LIB_CRYPTO crypto REQUIRED)

find_library(LIBCONFIG config)
if(NOT LIBM)
  message(FATAL_ERROR "libconfig not found")
endif()

##### grpc related
# Proto file
get_filename_component(hw_proto "./protos/echo.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)
# Generated sources
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/echo.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/echo.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/echo.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/echo.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")
# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
# hw_grpc_proto
add_library(hw_grpc_proto
  ${hw_grpc_srcs}
  ${hw_grpc_hdrs}
  ${hw_proto_srcs}
  ${hw_proto_hdrs})
target_link_libraries(hw_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# Setting this variable because both librand.so and agent link them in
set(RAND_SRCS "rand_gen.cpp" "cpp_rand.cc")
set(COMMON_CXXFLAGS "-Wall" )

# This is used by the Controller to do fast math
# SHARED means that PIC is on automatically
add_library( rand SHARED )
target_sources( rand
  PRIVATE ${RAND_SRCS} )
target_link_libraries( rand PRIVATE lancet )
target_compile_features( rand PRIVATE cxx_std_11 )
target_compile_features( rand PRIVATE c_std_11 )
target_compile_options( rand PRIVATE ${COMMON_CXXFLAGS} )

add_executable( loader )
target_link_libraries( loader PRIVATE lancet )
target_sources( loader PRIVATE
        ${RAND_SRCS}
        "app_proto.cpp" "key_gen.cpp"
        "loader.cpp" "memcache.cpp"
  )
target_link_libraries( loader PRIVATE rand )
target_compile_features( loader PUBLIC c_std_11 )
target_compile_features( loader PUBLIC cxx_std_14 )
target_compile_options( loader PRIVATE ${COMMON_CFLAGS} )
target_include_directories( loader PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}" )

add_executable( agent )
target_sources( agent PRIVATE
        ${RAND_SRCS}
        "agent.cpp" "args.cpp"
        "app_proto.cpp"
        "tp_tcp.cpp" 
        "tp_ssl.cpp" 
        "tp_grpc.cpp"
        "tp_grpc_go.cpp"
        "key_gen.cpp"
        "stats.cpp" "timestamping.cpp" 
        "memcache.cpp"
        )
target_compile_features( agent PUBLIC c_std_11 )
target_compile_features( agent PUBLIC cxx_std_14 )
target_include_directories( agent PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}" )
target_link_libraries( agent PUBLIC lancet hw_grpc_proto 
                        ${_REFLECTION} 
                        ${_GRPC_GRPCPP} 
                        ${_PROTOBUF_LIBPROTOBUF})
target_compile_options(agent PRIVATE ${COMMON_CXXFLAGS} ${ENABLE_R2P2} ${ENABLE_R2P2_NIC_TS})
target_link_libraries( agent PRIVATE
  Threads::Threads ${LIBM} ${LIBRT} ${LIB_SSL} ${LIB_CRYPTO})

# === Add Go shared library (libecho.so) ===
set(LIBECHO_SO "${CMAKE_CURRENT_SOURCE_DIR}/../inc/lancet/libecho.so")
target_link_libraries(agent PRIVATE ${LIBECHO_SO})
set_target_properties(agent PROPERTIES BUILD_RPATH "\$ORIGIN")

# Copy libecho.so into the same folder as the agent binary after build
add_custom_command(TARGET agent POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${LIBECHO_SO}"
        $<TARGET_FILE_DIR:agent>
)

# if(BUILD_R2P2)
#   target_link_libraries( agent PRIVATE r2p2 ${LIBCONFIG})
# endif()
