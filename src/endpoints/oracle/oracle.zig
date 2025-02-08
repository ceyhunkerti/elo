const std = @import("std");
const Oracle = @import("Oracle.zig");
const StringMap = std.StringHashMap([]const u8);

test {
    std.testing.refAllDecls(@This());
}

// std.mem.Allocator

test "OracleEndpoint" {
    const allocator = std.testing.allocator;
    var o = Oracle.init(allocator);
    defer o.deinit();

    var oracle = o.endpoint();
    var options = StringMap.init(allocator);
    defer options.deinit();

    const source = try oracle.source(options);
    _ = source;
}
