const c = @import("./c.zig").c;
const Self = @This();

oci_ctx: ?*c.OCISvcCtx = null,
