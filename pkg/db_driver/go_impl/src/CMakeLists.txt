# The Flutter tooling requires that developers have CMake 3.10 or later
# installed. You should not increase this version, as doing so will cause
# the plugin to fail to compile for some customers of the plugin.
cmake_minimum_required(VERSION 3.10)

project(go_impl_library VERSION 0.0.1 LANGUAGES C)

find_program(GO_EXECUTABLE go)
if(NOT GO_EXECUTABLE)
  message(FATAL_ERROR "go executable not found. Install Go to build go_impl.")
endif()

set(GO_IMPL_GO_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
if(WIN32)
  set(GO_IMPL_SHARED_LIB "${CMAKE_CURRENT_BINARY_DIR}/go_impl.dll")
elseif(APPLE)
  set(GO_IMPL_SHARED_LIB "${CMAKE_CURRENT_BINARY_DIR}/libgo_impl.dylib")
else()
  set(GO_IMPL_SHARED_LIB "${CMAKE_CURRENT_BINARY_DIR}/libgo_impl.so")
endif()
set(GO_IMPL_SHARED_HEADER "${CMAKE_CURRENT_BINARY_DIR}/libgo_impl.h")

add_custom_target(
  go_impl_go
  ALL
  COMMAND "${CMAKE_COMMAND}" -E env CGO_ENABLED=1 "${GO_EXECUTABLE}" build -buildmode=c-shared
          -o "${GO_IMPL_SHARED_LIB}" .
  BYPRODUCTS "${GO_IMPL_SHARED_LIB}" "${GO_IMPL_SHARED_HEADER}"
  WORKING_DIRECTORY "${GO_IMPL_GO_DIR}"
  COMMENT "Building Go go_impl shared library"
  VERBATIM)

add_library(go_impl SHARED IMPORTED GLOBAL)
set_target_properties(go_impl PROPERTIES IMPORTED_LOCATION "${GO_IMPL_SHARED_LIB}")
add_dependencies(go_impl go_impl_go)

if(ANDROID)
  # Support Android 15 16k page size
  set_target_properties(go_impl PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES C)
endif()
