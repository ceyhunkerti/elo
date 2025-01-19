const std = @import("std");
const w = @import("wire.zig");
const p = @import("proto.zig");

const Message = @import("Message.zig");

const Self = @This();

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

    if (self.hasData()) {
        self.resetDatabox();
    }
    self.allocator.free(self.inbox);

    for (self.metadatabox.items) |node| {
        defer self.allocator.destroy(node);
        node.data.deinit(self.allocator);
    }

    for (self.nilbox.items) |node| {
        defer self.allocator.destroy(node);
    }
}

pub fn clearInbox(self: *Self) void {
    for (self.inbox, 0..) |node, i| {
        if (i == self.data_index) break;
        node.*.data.deinit(self.allocator);
        self.allocator.destroy(node);
    }
    self.data_index = 0;
}

pub fn sendToInbox(self: *Self, node: *w.Message) void {
    self.inbox[self.data_index] = node;
    self.data_index += 1;
}

pub fn sendSliceToInbox(self: *Self, nodes: []*w.Message) void {
    for (nodes) |node| {
        self.sendToInbox(node);
    }
}

pub fn isInboxFull(self: *Self) bool {
    return self.data_index == self.data_capacity;
}

pub fn hasData(self: *Self) bool {
    return self.data_index > 0;
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

    mailbox.sendSliceToInbox(&[_]*w.Message{
        p.Record.Message(allocator, &[_]p.Value{
            .{ .Int = 1 },
            .{ .Boolean = true },
        }) catch unreachable,
        p.Record.Message(allocator, &[_]p.Value{
            .{ .Int = 2 },
            .{ .Boolean = false },
        }) catch unreachable,
    });

    try std.testing.expectEqual(mailbox.inbox_index, 2);
    try std.testing.expect(mailbox.inbox.len == 2);
    try std.testing.expect(mailbox.hasData());
    try std.testing.expect(mailbox.isInboxFull());
}
