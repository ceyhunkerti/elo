const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");
const assert = std.debug.assert;
const testing = std.testing;
const commons = @import("commons.zig");
const Record = commons.Record;
const Metadata = commons.Metadata;

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
    Metadata: *const Metadata,
    Record: Record,
    Nil,
};

pub const MessageQueue = AtomicBlockingQueue(Message);

test "MessageQueue" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const producer = struct {
        const FieldValue = @import("commons.zig").FieldValue;

        pub fn producerThread(allocator: Allocator, q: *MessageQueue) !void {
            var record: Record = try allocator.alloc(FieldValue, 2);
            record[0] = FieldValue{ .Int = 1 };
            record[1] = FieldValue{ .Boolean = true };
            const message = Message{ .Record = record };

            const node = allocator.create(MessageQueue.Node) catch unreachable;
            node.* = .{
                .prev = undefined,
                .next = undefined,
                .data = message,
            };
            q.put(node);

            const term = allocator.create(MessageQueue.Node) catch unreachable;
            term.* = .{
                .prev = undefined,
                .next = undefined,
                .data = .Nil,
            };
            q.put(term);
        }
    };
    var queue = MessageQueue.init();

    const allocator = arena.allocator();
    var producer_thread = try std.Thread.spawn(.{ .allocator = allocator }, producer.producerThread, .{ allocator, &queue });

    break_while: while (true) {
        switch (queue.get().data) {
            .Metadata => |_| {},
            .Record => |record| {
                try testing.expectEqual(1, record[0].Int);
                try testing.expectEqual(true, record[1].Boolean);
            },
            .Nil => break :break_while,
        }
    }

    producer_thread.join();
}

test "MessageQueue sync" {
    const FieldValue = @import("commons.zig").FieldValue;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var queue = MessageQueue.init();
    const allocator = arena.allocator();

    var record: Record = try allocator.alloc(FieldValue, 2);
    record[0] = FieldValue{ .Int = 1 };
    record[1] = FieldValue{ .Boolean = true };
    const message = Message{ .Record = record };
    var node = MessageQueue.Node{ .data = message, .next = undefined, .prev = undefined };
    queue.put(&node);

    const n = queue.get();
    try testing.expectEqual(1, n.data.Record[0].Int);
}
