const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");
const assert = std.debug.assert;
const testing = std.testing;

const log = std.log;
const p = @import("proto/proto.zig");
const Metadata = p.Metadata;
const Record = p.Record;
const Value = p.Value;

pub const Error = error{
    ProducersNotSet,
    ConsumersNotSet,
    InvalidConsumerCount,
    InvalidProducerCount,
};

pub fn AtomicBlockingQueue(comptime T: type) type {
    return struct {
        head: ?*Node,
        tail: ?*Node,
        mutex: std.Thread.Mutex,
        cond: std.Thread.Condition,
        err: ?anyerror = null,

        producers: u16,
        consumers: u16,

        active_producers: u16 = 0,
        active_consumers: u16 = 0,

        pub const Self = @This();
        pub const Node = std.DoublyLinkedList(T).Node;

        pub fn init(producers: u16, consumers: u16) Self {
            return Self{
                .head = null,
                .tail = null,
                .mutex = std.Thread.Mutex{},
                .cond = std.Thread.Condition{},
                .producers = producers,
                .consumers = consumers,
            };
        }

        pub fn startConsumer(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.active_consumers += 1;
            assert(self.active_consumers <= self.consumers);
        }

        pub fn stopConsumer(self: *Self) !void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.active_consumer -= 1;
            assert(self.active_consumers >= 0);
        }

        pub fn startProducer(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.active_producers += 1;
            assert(self.active_producers <= self.producers);
        }
        pub fn stopProducer(self: *Self) !void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.active_producers -= 1;
            assert(self.active_producers >= 0);

            // if no more producers left broadcast termination message
            if (self.active_producers == 0) {
                for (0..self.consumers) |_| {
                    self.put(Term(self.allocator));
                }
            }
        }

        pub fn validate(self: Self) !void {
            if (self.producers == 0) {
                log.err("Producer count is not set\n", .{});
                return Error.ProducersNotSet;
            }
            if (self.consumers == 0) {
                log.err("Consumer count is not set\n", .{});
                return Error.ConsumersNotSet;
            }
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
            self.message_count = 0;
        }

        pub fn interruptWithError(self: *Self, allocator: Allocator, err: anyerror) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.err = err;
            self.drain(allocator);
        }

        pub fn put(self: *Self, node: *Node) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.err) |err| return err;

            node.next = null;
            node.prev = self.tail;
            self.tail = node;
            if (node.prev) |prev_tail| {
                prev_tail.next = node;
            } else {
                assert(self.head == null);
                self.head = node;
            }
            self.message_count += 1;
            // Signal one or more waiting threads that an item is available
            self.cond.signal();
        }

        pub fn get(self: *Self) !*Node {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.err) |err| return err;

            while (self.head == null) {
                if (self.err) |err| return err;
                if (self.producers == 0 and self.message_count)
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

            self.message_count -= 1;
            assert(self.message_count >= 0);
            return head;
        }

        pub fn isEmpty(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.head == null;
        }

        pub fn isNewMessageExpected(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.message_count == 0 and self.producers == 0;
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
            wire.startProducer();
            defer wire.stopProducer();
            const m = try p.Record.Message(allocator_, &[_]p.Value{ .{ .Int = 1 }, .{ .Boolean = true } });
            try wire.put(m);
            try wire.put(Term(allocator_));
        }
    };

    var wire = Wire.init(1, 1);

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

    var wire = Wire.init(1, 1);
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
