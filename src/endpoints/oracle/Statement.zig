const c = @import("./c.zig").c;
const Self = @This();
const Context = @import("Context.zig");

ctx: *Context = undefined,
