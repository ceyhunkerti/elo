const Context = @import("Context.zig");
const c = @import("c.zig").c;

fn fromContext(ctx: *const Context, result: c_int) !void {
    var info: c.OCIError = undefined;
    var err_code: c.ub4 = undefined;
    var err_buf: [512]u8 = undefined;

    c.OCIErrorGet(
        &info,
        @as(c.ub4, 1),
        null,
        &err_code,
        &err_buf,
        @as(c.ub4, 512),
        c.OCI_HTYPE_ERROR,
    );

    c.OCIBindArrayOfStruct(bindp: ?*OCIBind, errhp: ?*OCIError, pvskip: ub4, indskip: ub4, alskip: ub4, rcskip: ub4)

    // hndlp: ?*anyopaque, recordno: ub4, sqlstate: [*c]OraText, errcodep: [*c]sb4, bufp: [*c]OraText, bufsiz: ub4, @"type": ub4)

    // (void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
    //                     errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
    // (void) printf("Error - %.*s\n", 512, errbuf);

    // var info: c.OCIError = undefined;
    // c.
    //
    //
    // dpiContext_getError(ctx.context, &mut info);
}

pub fn checkError(ctx: *const Context, result: c_int) !void {}
