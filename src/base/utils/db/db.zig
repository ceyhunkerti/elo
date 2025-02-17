const std = @import("std");
pub const metadata = @import("metadata/metadata.zig");

test {
    std.testing.refAllDecls(@This());
}
