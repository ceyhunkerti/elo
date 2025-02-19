const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn chunks(allocator: Allocator, count: u32, chunk_count: u16) ![]u32 {
    var res = std.ArrayList(u32).init(allocator);
    errdefer res.deinit();

    if (chunk_count >= count) {
        try res.append(count);
        return try res.toOwnedSlice();
    }

    const chunk_size: u32 = @divTrunc(count, @as(u32, chunk_count));
    var current_size: u32 = 0;

    var i: u16 = 0;
    while (i < chunk_count - 1) : (i += 1) {
        try res.append(chunk_size);
        current_size += chunk_size;
    }

    const final_chunk = count - current_size;
    try res.append(final_chunk);

    return try res.toOwnedSlice();
}

test "chunks" {
    const allocator = std.testing.allocator;

    {
        const result = try chunks(allocator, 1, 1);
        defer allocator.free(result);
        try std.testing.expectEqualSlices(u32, result, &[_]u32{1});
    }

    {
        const result = try chunks(allocator, 2, 1);
        defer allocator.free(result);
        try std.testing.expectEqualSlices(u32, result, &[_]u32{2});
    }

    {
        const result = try chunks(allocator, 2, 3);
        defer allocator.free(result);
        try std.testing.expectEqualSlices(u32, result, &[_]u32{2});
    }

    {
        const result = try chunks(allocator, 4, 3);
        defer allocator.free(result);
        try std.testing.expectEqualSlices(u32, result, &[_]u32{ 1, 1, 2 });
    }

    {
        const result = try chunks(allocator, 6, 3);
        defer allocator.free(result);
        try std.testing.expectEqualSlices(u32, result, &[_]u32{ 2, 2, 2 });
    }
}

pub fn selectQueriesFromChunks(allocator: Allocator, chunks: []u32) ![]const []const u8 {}
