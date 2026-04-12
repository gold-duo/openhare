#include "stream_bridge.h"
#include "dart-sdk-include/dart_api_dl.c"
#include "dart_api_dl.h"

#if defined(_WIN32) || defined(_WIN64)
/* MSVC (and MinGW) need explicit exports for symbols used via dart:ffi. */
#define GO_IMPL_C_EXPORT __declspec(dllexport)
#elif defined(__GNUC__) || defined(__clang__)
#define GO_IMPL_C_EXPORT __attribute__((visibility("default")))
#else
#define GO_IMPL_C_EXPORT
#endif

GO_IMPL_C_EXPORT int go_impl_init_dart_api(void* data) {
  return (int)Dart_InitializeApiDL(data);
}

GO_IMPL_C_EXPORT int go_impl_post_stream_message(int64_t port_id, int32_t kind,
                                                 uintptr_t payload) {
  if (Dart_PostCObject_DL == NULL) {
    return 0;
  }

  Dart_CObject kind_obj;
  kind_obj.type = Dart_CObject_kInt32;
  kind_obj.value.as_int32 = kind;

  Dart_CObject payload_obj;
  payload_obj.type = Dart_CObject_kInt64;
  payload_obj.value.as_int64 = (int64_t)payload;

  Dart_CObject* elems[2];
  elems[0] = &kind_obj;
  elems[1] = &payload_obj;

  Dart_CObject root;
  root.type = Dart_CObject_kArray;
  root.value.as_array.length = 2;
  root.value.as_array.values = elems;

  return Dart_PostCObject_DL((Dart_Port_DL)port_id, &root) ? 1 : 0;
}
