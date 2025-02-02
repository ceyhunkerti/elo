const c = @import("../c.zig").c;

pub const OracleColumnType = struct {
    dpi_oracle_type_num: c.dpiOracleTypeNum,
    dpi_native_type_num: c.dpiNativeTypeNum,
};
