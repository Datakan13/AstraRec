# Market-data recorder (see recorder/README.md).
#
# recorder_core: parsing, sequence checks, storage and the writer (offline, tested
# with fixtures). recorder (binary): the live pipeline on AstraConnect/AstraLib,
# with -DRECORDER_BUILD_LIVE=ON.
#
# Builds two ways:
#   cmake -S . -B build          whole project (this file is add_subdirectory'd)
#   cmake -S recorder -B build   recorder only: needs just recorder/ and
#                                external/AstraConnect (see "Clone only the recorder")

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  # ---- Standalone: define what the top-level CMakeLists.txt would provide ----
  cmake_minimum_required(VERSION 3.22)
  project(acrec CXX)
  enable_testing()
  if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
  endif()
  set(HAR_ASTRACONNECT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../external/AstraConnect"
      CACHE PATH "AstraConnect checkout")
  if(NOT EXISTS "${HAR_ASTRACONNECT_DIR}/src/api/AstraConnect.hpp")
    message(FATAL_ERROR "AstraConnect not found at ${HAR_ASTRACONNECT_DIR}. Run:\n"
                        "  git submodule update --init external/AstraConnect")
  endif()
  add_library(simdjson STATIC ${HAR_ASTRACONNECT_DIR}/include/simdjson/simdjson.cpp)
  target_include_directories(simdjson SYSTEM PUBLIC ${HAR_ASTRACONNECT_DIR}/include)
  option(RECORDER_BUILD_LIVE "Build the live recorder binary" OFF)
  if(RECORDER_BUILD_LIVE)
    # AstraConnect uses only header-only Boost (Beast, Asio, UUID). Boost.System has
    # been header-only since 1.69 and its stub library is gone from 1.89 on, so ask
    # for the headers through Boost's own CMake package (no FindBoost, no CMP0167).
    find_package(Boost 1.74 REQUIRED CONFIG)
    find_package(OpenSSL REQUIRED)
    find_package(Threads REQUIRED)
    # Header-only; its own CMakeLists builds tests and forces -march=native, so
    # the target is defined here (same as the top-level CMakeLists.txt).
    add_library(astraconnect INTERFACE)
    target_include_directories(astraconnect SYSTEM INTERFACE ${HAR_ASTRACONNECT_DIR}/src ${HAR_ASTRACONNECT_DIR}/include)
    target_compile_features(astraconnect INTERFACE cxx_std_20)
    target_link_libraries(astraconnect INTERFACE
      simdjson Boost::headers OpenSSL::SSL OpenSSL::Crypto Threads::Threads)
    add_library(AstraConnect::AstraConnect ALIAS astraconnect)
  endif()
endif()

find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd>=1.4)

add_library(recorder_core STATIC
  src/decimal.cpp
  src/timeutil.cpp
  src/symbols.cpp
  src/parser.cpp
  src/storage.cpp
  src/writer_core.cpp
  src/config.cpp
  src/logger.cpp
  src/recorder.cpp)
target_include_directories(recorder_core PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${HAR_ASTRACONNECT_DIR}/include)   # <AstraLib/...> (vendored)
target_compile_features(recorder_core PUBLIC cxx_std_20)
target_compile_options(recorder_core PRIVATE -Wall -Wextra)
target_link_libraries(recorder_core PUBLIC simdjson PkgConfig::ZSTD Threads::Threads)

# Offline replay: message files -> parser -> writer -> files (used by tests)
add_executable(acrec_replay apps/acrec_replay.cpp)
target_link_libraries(acrec_replay PRIVATE recorder_core)

# ---- Acceptance tests (spec section 8) ----
find_package(Python3 COMPONENTS Interpreter)
add_executable(recorder_test tests/recorder_test.cpp)
target_link_libraries(recorder_test PRIVATE recorder_core)
add_test(NAME recorder_test COMMAND recorder_test)
if(Python3_FOUND)
  add_test(NAME recorder_roundtrip
           COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_roundtrip.py $<TARGET_FILE:acrec_replay>)
endif()
add_executable(recorder_manager_test tests/manager_test.cpp)
target_link_libraries(recorder_manager_test PRIVATE recorder_core)
target_compile_definitions(recorder_manager_test PRIVATE FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures")
add_test(NAME recorder_manager_test COMMAND recorder_manager_test)

# ---- Live recorder (needs recorder/ASTRACONNECT_SPEC.md implemented) ----
option(RECORDER_BUILD_LIVE "Build the live recorder (needs the AstraConnect/AstraLib additions in ASTRACONNECT_SPEC.md)" OFF)
if(RECORDER_BUILD_LIVE)
  if(NOT TARGET AstraConnect::AstraConnect)
    message(FATAL_ERROR "RECORDER_BUILD_LIVE needs HAR_BUILD_LIVE=ON (the AstraConnect target)")
  endif()
  add_library(recorder_live STATIC src/queue_sink.cpp src/astra_transport.cpp)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # Boost.Beast/Asio make this one file need ~1.5 GB of compiler memory (GCC 15).
    # Collecting garbage more often caps it near 1 GB for a few extra seconds,
    # so 1 GB build servers manage with a little swap.
    set_source_files_properties(src/astra_transport.cpp PROPERTIES
      COMPILE_OPTIONS "--param=ggc-min-expand=10;--param=ggc-min-heapsize=32768")
  endif()
  target_link_libraries(recorder_live PUBLIC recorder_core AstraConnect::AstraConnect)
  add_executable(recorder apps/recorder_main.cpp)
  target_link_libraries(recorder PRIVATE recorder_live)
  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/deploy/recorder.json ${CMAKE_BINARY_DIR}/recorder.json COPYONLY)
endif()
