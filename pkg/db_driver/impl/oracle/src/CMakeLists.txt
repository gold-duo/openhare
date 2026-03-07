# The Flutter tooling requires that developers have CMake 3.10 or later
# installed. You should not increase this version, as doing so will cause
# the plugin to fail to compile for some customers of the plugin.
cmake_minimum_required(VERSION 3.10)

project(oracle_library VERSION 0.0.1 LANGUAGES C)

find_program(GO_EXECUTABLE go)
if (NOT GO_EXECUTABLE)
  message(FATAL_ERROR "go executable not found. Please install Go to build oracle.")
endif()

set(ORACLE_GO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../go")
if (WIN32)
  set(ORACLE_SHARED_LIB "${CMAKE_CURRENT_BINARY_DIR}/oracle.dll")
elseif(APPLE)
  set(ORACLE_SHARED_LIB "${CMAKE_CURRENT_BINARY_DIR}/liboracle.dylib")
else()
  set(ORACLE_SHARED_LIB "${CMAKE_CURRENT_BINARY_DIR}/liboracle.so")
endif()
set(ORACLE_SHARED_HEADER "${CMAKE_CURRENT_BINARY_DIR}/oracle.h")

add_custom_target(oracle_go ALL
  COMMAND "${CMAKE_COMMAND}" -E env CGO_ENABLED=1 "${GO_EXECUTABLE}" build -buildmode=c-shared -o "${ORACLE_SHARED_LIB}" .
  BYPRODUCTS "${ORACLE_SHARED_LIB}" "${ORACLE_SHARED_HEADER}"
  WORKING_DIRECTORY "${ORACLE_GO_DIR}"
  COMMENT "Building Go oracle shared library"
  VERBATIM
)

add_library(oracle SHARED IMPORTED GLOBAL)
set_target_properties(oracle PROPERTIES
  IMPORTED_LOCATION "${ORACLE_SHARED_LIB}"
)
add_dependencies(oracle oracle_go)
