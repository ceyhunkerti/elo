const base = @import("base");
const EndpointRegistry = base.EndpointRegistry;

pub const Params = struct {
    endpoint_registry: *EndpointRegistry,
};
