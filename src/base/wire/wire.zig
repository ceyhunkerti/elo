const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");
const assert = std.debug.assert;
const testing = std.testing;

const p = @import("proto/proto.zig");
const Metadata = p.Metadata;
const Record = p.Record;
const Value = p.Value;

pub fn AtomicBlockingQueue(comptime T: type) type {
    return struct {
        head: ?*Node,
        tail: ?*Node,
        mutex: std.Thread.Mutex,
        cond: std.Thread.Condition,
        err: ?anyerror = null,

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

        pub fn drain(self: *Self, allocator: Allocator) void {
            while (self.head != null) {
                const head = self.head.?;
                self.head = head.next;
                if (head.next) |new_head| {
                    new_head.prev = null;
                } else {
                    self.tail = null;
                }
                head.prev = null;
                head.next = null;
                MessageFactory.destroy(allocator, head);
            }
        }

        pub fn interruptWithError(self: *Self, allocator: Allocator, err: anyerror) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.err = err;
            self.drain(allocator);
        }

        pub fn put(self: *Self, node: *Node) !void {
            node.next = null;

            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.err) |err| return err;

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

        pub fn get(self: *Self) !*Node {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.err) |err| return err;

            while (self.head == null) {
                if (self.err) |err| return err;
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
    Metadata: Metadata,
    Record: Record,
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

pub fn Term(allocator: Allocator) *Message {
    const msg = allocator.create(Message) catch unreachable;
    msg.* = .{ .data = .Nil };
    return msg;
}

test "Wire" {
    const allocator = testing.allocator;

    const producer = struct {
        pub fn producerThread(allocator_: Allocator, wire: *Wire) !void {
            const m = try p.Record.Message(allocator_, &[_]p.Value{ .{ .Int = 1 }, .{ .Boolean = true } });
            try wire.put(m);
            try wire.put(Term(allocator_));
        }
    };

    var wire = Wire.init();

    var producer_thread = try std.Thread.spawn(.{ .allocator = allocator }, producer.producerThread, .{ allocator, &wire });

    while (true) {
        const message = try wire.get();
        defer MessageFactory.destroy(allocator, message);
        switch (message.data) {
            .Metadata => |_| {},
            .Record => |record| {
                try testing.expectEqual(1, record.get(0).Int);
                try testing.expectEqual(true, record.get(1).Boolean);
            },
            .Nil => break,
        }
    }

    producer_thread.join();
}

test "MessageQueue sync" {
    const allocator = testing.allocator;

    var wire = Wire.init();
    const record = p.Record.fromSlice(allocator, &[_]p.Value{ .{ .Int = 1 }, .{ .Boolean = true } }) catch unreachable;
    try wire.put(record.asMessage(allocator) catch unreachable);

    const mr = try wire.get();
    defer MessageFactory.destroy(allocator, mr);

    try testing.expectEqual(1, mr.data.Record.get(0).Int);
}

pub const MessageFactory = struct {
    pub fn new(allocator: Allocator, val: anytype) !*Message {
        const message = try allocator.create(Message);
        message.* = .{
            .data = switch (@TypeOf(val)) {
                p.Metadata => .{ .Metadata = val },
                p.Record => .{ .Record = val },

                else => .Nil,
            },
        };
        return message;
    }
    pub fn destroy(allocator: Allocator, message: *Message) void {
        if (message.data != .Nil) {
            message.data.deinit(allocator);
        }
        allocator.destroy(message);
    }
};
