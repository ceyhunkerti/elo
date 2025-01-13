pub inline fn checkError(result: c_int, err: anytype) !void {
    if (result < 0) return err;
}
