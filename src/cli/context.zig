const std = @import("std");
const base = @import("base");
const EndpointRegistry = base.EndpointRegistry;

pub const Context = struct {
    endpoint_registry: *EndpointRegistry,
    log_level: *std.log.Level,
};
