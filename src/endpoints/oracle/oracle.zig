const std = @import("std");
const testing = std.testing;

pub const Oracle = @import("Oracle.zig");
const StringMap = std.StringHashMap([]const u8);

test {
    testing.refAllDecls(@This());
}

// std.mem.Allocator

test "OracleEndpoint" {
    const allocator = std.testing.allocator;
    var o = Oracle.init(allocator);
    defer o.deinit();

    var endpoint = o.endpoint();
    const endpoint_help =
        \\Name: oracle
        \\Description: Oracle database endpoint.
        \\Supports: Source, Sink
    ;

    const endpoint_help_ = try endpoint.help();
    defer allocator.free(endpoint_help_);
    try testing.expectEqualStrings(endpoint_help, endpoint_help_);

    // var options = StringMap.init(allocator);
    // defer options.deinit();

    // const source = try endpoint.source(options);
    // _ = source;
}
