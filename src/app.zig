// endpoints
pub const endpoint = @import("endpoint/endpoint.zig");
pub const Endpoint = endpoint.Endpoint;
pub const Source = endpoint.Source;
pub const Sink = endpoint.Sink;

// wire
pub const w = @import("wire/wire.zig");
pub const Wire = w.Wire;
pub const Term = w.Term;
pub const M = @import("wire/M.zig");
pub const Message = w.Message;
