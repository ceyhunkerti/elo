const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");
const assert = std.debug.assert;
const testing = std.testing;
const p = @import("proto.zig");

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

pub const Datum = union(enum) {
    Metadata: p.Metadata,
    Record: p.Record,
    Nil,

    pub fn deinit(self: *Datum, allocator: Allocator) void {
        switch (self.*) {
            .Metadata => |metadata| metadata.deinit(allocator),
            .Record => |record| record.deinit(allocator),
            .Nil => {},
        }
    }
};

pub const Wire = AtomicBlockingQueue(Datum);
pub const Message = Wire.Node;

pub fn Term(allocator: Allocator) !*Message {
    const result = allocator.create(Message) catch unreachable;
    result.* = .{ .data = .Nil };
    return result;
}

test "Wire" {
    const allocator = testing.allocator;

    const producer = struct {
        pub fn producerThread(allocator_: Allocator, wire: *Wire) !void {
            const m = try p.Record.Message(allocator_, &[_]p.Value{ .{ .Int = 1 }, .{ .Boolean = true } });
            wire.put(m);
            wire.put(Term(allocator_));
        }
    };

    var wire = Wire.init();

    var producer_thread = try std.Thread.spawn(.{ .allocator = allocator }, producer.producerThread, .{ allocator, &wire });

    break_while: while (true) {
        const message = wire.get();
        defer message.data.deinit(allocator);
        defer allocator.destroy(message);

        switch (message.data) {
            .Metadata => |_| {},
            .Record => |record| {
                try testing.expectEqual(1, record.item(0).Int);
                try testing.expectEqual(true, record.item(1).Boolean);
            },
            .Nil => break :break_while,
        }
    }

    producer_thread.join();
}

test "MessageQueue sync" {
    const allocator = testing.allocator;

    var wire = Wire.init();
    const record = p.Record.fromSlice(allocator, &[_]p.Value{ .{ .Int = 1 }, .{ .Boolean = true } }) catch unreachable;
    const message = Message{ .Record = record };
    var ms = Message{ .data = message, .next = undefined, .prev = undefined };
    wire.put(&ms);

    const mr = wire.get();
    defer mr.data.deinit(allocator);

    try testing.expectEqual(1, mr.data.Record.item(0).Int);
}
