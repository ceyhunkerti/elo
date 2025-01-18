const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");
const assert = std.debug.assert;
const testing = std.testing;
const commons = @import("commons.zig");
const Record = commons.Record;
const Metadata = commons.Metadata;
const Value = commons.Value;

pub fn AtomicBlockingQueue(comptime T: type) type {
    return struct {
        head: ?*Node,
        tail: ?*Node,
        mutex: std.Thread.Mutex,
        cond: std.Thread.Condition,

        pub const Self = @This();
        pub const Node = std.DoublyLinkedList(T).Node;

        pub fn init() Self {
            return Self{
                .head = null,
                .tail = null,
                .mutex = std.Thread.Mutex{},
                .cond = std.Thread.Condition{},
            };
        }

        pub fn put(self: *Self, node: *Node) void {
            node.next = null;

            self.mutex.lock();
            defer self.mutex.unlock();

            node.prev = self.tail;
            self.tail = node;
            if (node.prev) |prev_tail| {
                prev_tail.next = node;
            } else {
                assert(self.head == null);
                self.head = node;
            }

            // Signal one or more waiting threads that an item is available
            self.cond.signal();
        }

        pub fn get(self: *Self) *Node {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.head == null) {
                self.cond.wait(&self.mutex);
            }

            const head = self.head.?;
            self.head = head.next;
            if (head.next) |new_head| {
                new_head.prev = null;
            } else {
                self.tail = null;
            }
            head.prev = null;
            head.next = null;
            return head;
        }

        pub fn isEmpty(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.head == null;
        }

        pub fn dump(self: *Self) void {
            self.dumpToStream(std.io.getStdErr().writer()) catch return;
        }
    };
}

pub const Message = union(enum) {
    Metadata: Metadata,
    Record: Record,
    Nil,

    pub fn deinit(self: *Message, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .Metadata => |metadata| metadata.deinit(allocator),
            .Record => |record| record.deinit(allocator),
            .Nil => {},
        }
    }
};

pub const MessageQueue = AtomicBlockingQueue(Message);

test "MessageQueue" {
    const allocator = testing.allocator;

    const producer = struct {
        pub fn producerThread(allocator_: Allocator, q: *MessageQueue) !void {
            var record = Record.init(allocator_, 2) catch unreachable;
            record.appendSlice(&[_]Value{ .{ .Int = 1 }, .{ .Boolean = true } }) catch unreachable;
            const message = Message{ .Record = record };
            const node = allocator.create(MessageQueue.Node) catch unreachable;
            node.* = .{ .data = message };
            q.put(node);

            const term = allocator.create(MessageQueue.Node) catch unreachable;
            term.* = .{ .data = .Nil };
            q.put(term);
        }
    };
    var queue = MessageQueue.init();

    var producer_thread = try std.Thread.spawn(.{ .allocator = allocator }, producer.producerThread, .{ allocator, &queue });

    break_while: while (true) {
        const node = queue.get();
        defer allocator.destroy(node);
        var message = node.data;
        defer message.deinit(allocator);

        switch (message) {
            .Metadata => |_| {},
            .Record => |record| {
                try testing.expectEqual(1, record.items()[0].Int);
                try testing.expectEqual(true, record.items()[1].Boolean);
            },
            .Nil => break :break_while,
        }
    }

    producer_thread.join();
}

test "MessageQueue sync" {
    const allocator = testing.allocator;

    var queue = MessageQueue.init();
    const record = Record.fromSlice(allocator, &[_]Value{ .{ .Int = 1 }, .{ .Boolean = true } }) catch unreachable;
    const message = Message{ .Record = record };
    var node = MessageQueue.Node{ .data = message, .next = undefined, .prev = undefined };
    queue.put(&node);

    const n = queue.get();
    defer n.data.deinit(allocator);

    try testing.expectEqual(1, n.data.Record.item(0).Int);
}

pub const Mailbox = struct {
    allocator: std.mem.Allocator,
    data_capacity: u32 = 0,
    data_index: u32 = 0,
    databox: []*MessageQueue.Node = undefined,
    metadatabox: std.ArrayList(*MessageQueue.Node) = undefined,
    nilbox: std.ArrayList(*MessageQueue.Node) = undefined,

    pub fn init(allocator: std.mem.Allocator, capacity: u32) !Mailbox {
        return .{
            .allocator = allocator,
            .data_capacity = capacity,
            .databox = try allocator.alloc(*MessageQueue.Node, capacity),
            .metadatabox = std.ArrayList(*MessageQueue.Node).init(allocator),
            .nilbox = std.ArrayList(*MessageQueue.Node).init(allocator),
        };
    }

    pub fn deinit(self: *Mailbox) void {
        defer self.metadatabox.deinit();
        defer self.nilbox.deinit();

        if (self.hasData()) {
            self.resetDatabox();
        }
        self.allocator.free(self.databox);

        for (self.metadatabox.items) |node| {
            defer self.allocator.destroy(node);
            node.data.deinit(self.allocator);
        }

        for (self.nilbox.items) |node| {
            defer self.allocator.destroy(node);
        }
    }

    pub fn resetDatabox(self: *Mailbox) void {
        for (self.databox, 0..) |node, i| {
            if (i == self.data_index) break;
            node.*.data.deinit(self.allocator);
            self.allocator.destroy(node);
        }
        self.data_index = 0;
    }

    pub fn appendData(self: *Mailbox, node: *MessageQueue.Node) void {
        self.databox[self.data_index] = node;
        self.data_index += 1;
    }

    pub fn isDataboxFull(self: *Mailbox) bool {
        return self.data_index == self.data_capacity;
    }

    pub fn hasData(self: *Mailbox) bool {
        return self.data_index > 0;
    }

    pub fn appendMetadata(self: *Mailbox, node: *MessageQueue.Node) void {
        self.metadatabox.append(node) catch unreachable;
    }

    pub fn appendNil(self: *Mailbox, node: *MessageQueue.Node) void {
        self.nilbox.append(node) catch unreachable;
    }
};

test "Mailbox" {
    const allocator = testing.allocator;

    var mailbox = try Mailbox.init(allocator, 2);
    defer mailbox.deinit();

    const node1 = try allocator.create(MessageQueue.Node);
    const message1 = Message{ .Record = try Record.fromSlice(allocator, &[_]Value{ .{ .Int = 1 }, .{ .Boolean = true } }) };
    node1.* = .{ .data = message1 };
    mailbox.appendData(node1);

    const node2 = try allocator.create(MessageQueue.Node);
    const message2 = Message{ .Record = try Record.fromSlice(allocator, &[_]Value{ .{ .Int = 2 }, .{ .Boolean = false } }) };
    node2.* = .{ .data = message2 };
    mailbox.appendData(node2);

    try testing.expectEqual(mailbox.data_index, 2);
    try testing.expect(mailbox.databox[0] == node1);
    try testing.expect(mailbox.databox[1] == node2);
    try testing.expect(mailbox.databox.len == 2);
    try testing.expect(mailbox.hasData());
    try testing.expect(mailbox.isDataboxFull());
}
