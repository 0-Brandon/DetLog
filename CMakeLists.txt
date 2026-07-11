cmake_minimum_required(VERSION 3.24)

project(DetLog VERSION 0.1.0 LANGUAGES CXX)

include(CTest)
find_package(Threads REQUIRED)

option(DETLOG_ENABLE_ASAN_UBSAN "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(DETLOG_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(DETLOG_BUILD_APPS "Build command-line demonstrations" ON)
option(DETLOG_BUILD_BENCHMARKS "Build benchmark harnesses" ON)
option(DETLOG_BUILD_FUZZERS "Build libFuzzer-compatible parser targets" OFF)

if(DETLOG_ENABLE_ASAN_UBSAN AND DETLOG_ENABLE_TSAN)
  message(FATAL_ERROR "ASan/UBSan and TSan must be separate builds")
endif()
if(DETLOG_BUILD_FUZZERS AND DETLOG_ENABLE_TSAN)
  message(FATAL_ERROR "Fuzzer/ASan and TSan must be separate builds")
endif()

add_library(detlog_options INTERFACE)
target_compile_features(detlog_options INTERFACE cxx_std_20)

if(MSVC)
  target_compile_options(detlog_options INTERFACE
    /W4 /permissive- /EHsc /Zc:__cplusplus)
else()
  target_compile_options(detlog_options INTERFACE
    -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow)
endif()

if(DETLOG_ENABLE_ASAN_UBSAN)
  target_compile_options(detlog_options INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(detlog_options INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
endif()

if(DETLOG_ENABLE_TSAN)
  target_compile_options(detlog_options INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
  target_link_options(detlog_options INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
endif()

if(DETLOG_BUILD_FUZZERS AND NOT DETLOG_ENABLE_ASAN_UBSAN)
  target_compile_options(detlog_options INTERFACE
    -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(detlog_options INTERFACE
    -fsanitize=address,undefined -fno-omit-frame-pointer)
endif()

function(detlog_library target)
  add_library(${target} ${ARGN})
  target_include_directories(${target} PUBLIC ${PROJECT_SOURCE_DIR}/include)
  target_link_libraries(${target} PUBLIC detlog_options)
endfunction()

detlog_library(detlog_codec src/codec.cpp)
detlog_library(detlog_storage src/wal.cpp)
detlog_library(detlog_core src/state_machine.cpp src/raft.cpp)
detlog_library(detlog_sim src/simulator.cpp)
target_link_libraries(detlog_sim PUBLIC detlog_codec)
detlog_library(detlog_tcp src/tcp_transport.cpp)
target_link_libraries(detlog_tcp PUBLIC Threads::Threads)
if(WIN32)
  target_link_libraries(detlog_tcp PUBLIC ws2_32)
endif()

if(EXISTS "${PROJECT_SOURCE_DIR}/src/cluster.cpp")
  detlog_library(detlog_cluster src/cluster.cpp)
  target_link_libraries(detlog_cluster PUBLIC
    detlog_core detlog_storage detlog_sim detlog_codec)
endif()

if(EXISTS "${PROJECT_SOURCE_DIR}/src/node_host.cpp")
  detlog_library(detlog_runtime src/node_host.cpp)
  target_link_libraries(detlog_runtime PUBLIC
    detlog_core detlog_storage detlog_tcp detlog_codec Threads::Threads)
endif()

if(BUILD_TESTING)
  add_executable(raft_tests tests/raft_tests.cpp)
  target_link_libraries(raft_tests PRIVATE detlog_core)
  add_test(NAME raft COMMAND raft_tests)

  add_executable(wal_tests tests/wal_tests.cpp)
  target_link_libraries(wal_tests PRIVATE detlog_storage)
  add_test(NAME wal COMMAND wal_tests)

  add_executable(process_crash_tests tests/process_crash_tests.cpp)
  target_link_libraries(process_crash_tests PRIVATE detlog_storage)
  add_test(NAME process_crash COMMAND process_crash_tests)

  add_executable(simulator_tests tests/simulator_tests.cpp)
  target_link_libraries(simulator_tests PRIVATE detlog_sim detlog_codec)
  add_test(NAME simulator COMMAND simulator_tests)

  add_executable(tcp_transport_tests tests/tcp_transport_tests.cpp)
  target_link_libraries(tcp_transport_tests PRIVATE detlog_tcp detlog_codec)
  add_test(NAME tcp_transport COMMAND tcp_transport_tests)

  if(TARGET detlog_cluster AND EXISTS "${PROJECT_SOURCE_DIR}/tests/cluster_tests.cpp")
    add_executable(cluster_tests tests/cluster_tests.cpp)
    target_link_libraries(cluster_tests PRIVATE detlog_cluster)
    add_test(NAME cluster COMMAND cluster_tests)
  endif()

  if(TARGET detlog_cluster AND EXISTS "${PROJECT_SOURCE_DIR}/tests/property_tests.cpp")
    add_executable(property_tests tests/property_tests.cpp)
    target_link_libraries(property_tests PRIVATE detlog_cluster)
    add_test(NAME property COMMAND property_tests)
  endif()

  if(TARGET detlog_cluster AND
     EXISTS "${PROJECT_SOURCE_DIR}/fuzz/scenario_fuzz.cpp")
    add_executable(scenario_fuzz_smoke fuzz/scenario_fuzz.cpp)
    target_compile_definitions(scenario_fuzz_smoke PRIVATE
      DETLOG_SCENARIO_FUZZ_STANDALONE=1)
    target_link_libraries(scenario_fuzz_smoke PRIVATE detlog_cluster)
    add_test(NAME scenario_fuzz_smoke COMMAND scenario_fuzz_smoke)
  endif()

  if(TARGET detlog_runtime AND EXISTS "${PROJECT_SOURCE_DIR}/tests/node_host_tests.cpp")
    add_executable(node_host_tests tests/node_host_tests.cpp)
    target_link_libraries(node_host_tests PRIVATE detlog_runtime)
    add_test(NAME node_host COMMAND node_host_tests)
  endif()
endif()

if(DETLOG_BUILD_APPS AND EXISTS "${PROJECT_SOURCE_DIR}/apps/detlog_sim.cpp")
  add_executable(detlog-sim apps/detlog_sim.cpp)
  target_link_libraries(detlog-sim PRIVATE detlog_cluster detlog_options)
endif()

if(DETLOG_BUILD_APPS AND TARGET detlog_runtime AND
   EXISTS "${PROJECT_SOURCE_DIR}/apps/detlog_node.cpp")
  add_executable(detlog-node apps/detlog_node.cpp)
  target_link_libraries(detlog-node PRIVATE detlog_runtime detlog_options)
endif()

if(DETLOG_BUILD_BENCHMARKS AND EXISTS "${PROJECT_SOURCE_DIR}/bench/detlog_bench.cpp")
  add_executable(detlog-bench bench/detlog_bench.cpp)
  target_link_libraries(detlog-bench PRIVATE
    detlog_cluster detlog_runtime detlog_options)
  if(WIN32)
    target_link_libraries(detlog-bench PRIVATE psapi)
  endif()
endif()

if(DETLOG_BUILD_BENCHMARKS AND
   EXISTS "${PROJECT_SOURCE_DIR}/bench/detlog_wal_bench.cpp")
  add_executable(detlog-wal-bench bench/detlog_wal_bench.cpp)
  target_link_libraries(detlog-wal-bench PRIVATE
    detlog_storage detlog_options Threads::Threads)
endif()

if(DETLOG_BUILD_FUZZERS)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "The bundled fuzz target configuration requires Clang")
  endif()

  # Instrument the implementation libraries as well as the tiny fuzz drivers;
  # otherwise libFuzzer cannot guide itself through parser/cluster branches.
  foreach(fuzz_library IN ITEMS
      detlog_codec detlog_storage detlog_core detlog_sim detlog_cluster)
    if(TARGET ${fuzz_library})
      target_compile_options(${fuzz_library} PRIVATE
        -fsanitize=fuzzer-no-link)
    endif()
  endforeach()

  add_executable(detlog-codec-fuzz fuzz/codec_fuzz.cpp)
  target_link_libraries(detlog-codec-fuzz PRIVATE detlog_codec detlog_options)
  target_compile_options(detlog-codec-fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
  target_link_options(detlog-codec-fuzz PRIVATE -fsanitize=fuzzer,address,undefined)

  add_executable(detlog-wal-fuzz fuzz/wal_fuzz.cpp)
  target_link_libraries(detlog-wal-fuzz PRIVATE detlog_storage detlog_options)
  target_compile_options(detlog-wal-fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
  target_link_options(detlog-wal-fuzz PRIVATE -fsanitize=fuzzer,address,undefined)

  add_executable(detlog-scenario-fuzz fuzz/scenario_fuzz.cpp)
  target_link_libraries(detlog-scenario-fuzz PRIVATE detlog_cluster detlog_options)
  target_compile_options(detlog-scenario-fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
  target_link_options(detlog-scenario-fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
endif()
