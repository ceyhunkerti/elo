// const c = @import("../c.zig").c;
// const oci = @cImport({
//     @cInclude("oci.h");
// });
// const Connection = @import("../Connection.zig");
// pub const Error = error{
//     Error,
// };

// oci_dpctx_ctl: ?*oci.OCIDirPathCtx = null, // direct path context
// oci_dpca_ctl: ?*oci.OCIDirPathColArray = null, // direct path column array handle
// oci_dpobjca_ctl: ?*oci.OCIDirPathColArray = null, // dp column array handle for obj
// oci_dpnestedobjca_ctl: ?*oci.OCIDirPathColArray = null, // dp col array hndl for nested obj
// oci_dpstr_ctl: ?*oci.OCIDirPathStream = null, // direct path stream handle

// pub fn init(conn: *Connection) !void {
//     var oci_dpctx_ctl: ?*oci.OCIDirPathCtx = null;

//     //create a context for this object type and describe the attributes
//     //that will be loaded for this object.
//     //   oci.OCIHandleAlloc((dvoid *)ctlp->envhp_ctl, (dvoid **)&ctlp->dpctx_ctl,
//     //                            (ub4)OCI_HTYPE_DIRPATH_CTX, (size_t)0, (dvoid **)0));

// }
