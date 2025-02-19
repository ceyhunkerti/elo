// endpoints
pub const endpoint = @import("endpoint/endpoint.zig");
pub const EndpointRegistry = endpoint.Registry;
pub const Source = endpoint.Source;
pub const Sink = endpoint.Sink;
pub const RegistryError = endpoint.Error;

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
pub const utils = @import("utils/utils.zig");
pub const QueryMetadata = utils.db.metadata.Query;
pub const Column = utils.db.metadata.Column;
pub const helpers = utils.helpers;
