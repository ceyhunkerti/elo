const md = @import("../../shared/db/metadata/metadata.zig");

pub const pgtype = @import("pgtype.zig");
pub const Column = md.Column(pgtype.PostgresType);
