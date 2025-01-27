const std = @import("std");
const w = @import("wire.zig");
const p = @import("proto/proto.zig");

const Self = @This();
const M = @import("M.zig");

allocator: std.mem.Allocator,

inbox: []*w.Message = undefined,
inbox_capacity: u32 = 0,
inbox_index: u32 = 0,

metadatabox: std.ArrayList(*w.Message) = undefined,
nilbox: std.ArrayList(*w.Message) = undefined,

pub fn init(allocator: std.mem.Allocator, capacity: u32) !Self {
    return .{
        .allocator = allocator,
        .inbox_capacity = capacity,
        .inbox = try allocator.alloc(*w.Message, capacity),
        .metadatabox = std.ArrayList(*w.Message).init(allocator),
        .nilbox = std.ArrayList(*w.Message).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    defer self.metadatabox.deinit();
    defer self.nilbox.deinit();

    if (self.inboxNotEmpty()) {
        self.clearInbox();
    }
    self.allocator.free(self.inbox);

    for (self.metadatabox.items) |node| {
        M.deinit(self.allocator, node);
    }

    for (self.nilbox.items) |node| {
        defer self.allocator.destroy(node);
    }
}

pub fn clearInbox(self: *Self) void {
    for (self.inbox, 0..) |node, i| {
        if (i == self.inbox_index) break;
        M.deinit(self.allocator, node);
    }
    self.inbox_index = 0;
}

pub fn sendToInbox(self: *Self, node: *w.Message) void {
    self.inbox[self.inbox_index] = node;
    self.inbox_index += 1;
}

pub fn sendSliceToInbox(self: *Self, nodes: []*w.Message) void {
    for (nodes) |node| {
        self.sendToInbox(node);
    }
}

pub fn isInboxFull(self: *Self) bool {
    return self.inbox_index == self.inbox_capacity;
}

pub fn inboxNotEmpty(self: *Self) bool {
    return self.inbox_index > 0;
}

pub fn sendToMetadata(self: *Self, node: *w.Message) void {
    self.metadatabox.append(node) catch unreachable;
}

pub fn sendToNil(self: *Self, node: *w.Message) void {
    self.nilbox.append(node) catch unreachable;
}

test "Mailbox" {
    const allocator = std.testing.allocator;

    var mailbox = try Self.init(allocator, 2);
    defer mailbox.deinit();

    const m1 = p.Record.Message(allocator, &[_]p.Value{
        .{ .Int = 1 },
        .{ .Boolean = true },
    }) catch unreachable;
    const m2 = p.Record.Message(allocator, &[_]p.Value{
        .{ .Int = 2 },
        .{ .Boolean = false },
    }) catch unreachable;
    var messages = [_]*w.Message{ m1, m2 };

    mailbox.sendSliceToInbox(&messages);

    try std.testing.expectEqual(mailbox.inbox_index, 2);
    try std.testing.expect(mailbox.inbox.len == 2);
    try std.testing.expect(mailbox.inboxNotEmpty());
    try std.testing.expect(mailbox.isInboxFull());
}
