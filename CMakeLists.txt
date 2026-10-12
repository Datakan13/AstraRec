# Market-data recorder (see recorder/README.md).
#
# recorder_core: parsing, sequence checks, storage and the writer. Builds and is
# tested today (fixtures, no network).
# recorder (binary): adds the threaded live pipeline on AstraConnect/AstraLib.
# It needs the additions in recorder/ASTRACONNECT_SPEC.md, so it is off until
# those land: -DRECORDER_BUILD_LIVE=ON.

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
  target_link_libraries(recorder_live PUBLIC recorder_core AstraConnect::AstraConnect)
  add_executable(recorder apps/recorder_main.cpp)
  target_link_libraries(recorder PRIVATE recorder_live)
  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/deploy/recorder.json ${CMAKE_BINARY_DIR}/recorder.json COPYONLY)
endif()
