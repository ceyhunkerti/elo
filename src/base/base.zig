// endpoints
pub const endpoint = @import("endpoint/endpoint.zig");
pub const EndpointRegistry = endpoint.Registry;
pub const Source = endpoint.Source;
pub const Sink = endpoint.Sink;

// wire
pub const w = @import("wire/wire.zig");
pub const Wire = w.Wire;
pub const Term = w.Term;
pub const Message = w.Message;
pub const MessageFactory = w.MessageFactory;

// proto
pub const p = @import("wire/proto/proto.zig");
pub const Record = p.Record;
pub const Value = p.Value;
pub const RecordFormatter = p.RecordFormatter;
pub const FieldType = p.FieldType;
pub const Timestamp = p.Timestamp;

// utils
pub const utils = @import("utils.zig");
pub const fromMap = utils.fromMap;
pub const fromMapOwned = utils.fromMapOwned;
