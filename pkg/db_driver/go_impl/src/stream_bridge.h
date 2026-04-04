#ifndef GO_IMPL_STREAM_BRIDGE_H
#define GO_IMPL_STREAM_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

int go_impl_init_dart_api(void* data);
int go_impl_post_stream_message(int64_t port_id, int32_t kind, uintptr_t payload);

#endif /* GO_IMPL_STREAM_BRIDGE_H */
