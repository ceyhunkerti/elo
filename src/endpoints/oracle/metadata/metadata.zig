pub const md = @import("../../shared/db/metadata/metadata.zig");
pub const types = @import("./types.zig");

pub const TableMetadata = @import("./TableMetadata.zig");
pub const Column = md.Column(types.OracleColumnType);
