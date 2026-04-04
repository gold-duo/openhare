// libgo_impl.a is force-loaded in go_impl.podspec (OTHER_LDFLAGS).
// Keep symbols visible for dlsym / -dead_strip.
#include "../../src/go_impl.h"

__attribute__((used)) static void *const _go_impl_keep_ffi_exports[] = {
    (void *)go_impl_init_dart_api,
    (void *)go_impl_conn_open_async,
    (void *)go_impl_conn_close_async,
    (void *)go_impl_free_cstr,
    (void *)go_impl_query_stream,
};
