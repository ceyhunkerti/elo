const c = @cImport({
    @cInclude("oci.h");
});

//   ub4                 nrow_ctl;            /* number of rows in column array */
//   ub2                 ncol_ctl;         /* number of columns in column array */
//   OCIEnv             *envhp_ctl;                       /* environment handle */
//   OCIServer          *srvhp_ctl;                            /* server handle */
//   OCIError           *errhp_ctl;                             /* error handle */
//   OCIError           *errhp2_ctl;                /* yet another error handle */
//   OCISvcCtx          *svchp_ctl;                          /* service context */
//   OCISession         *authp_ctl;                   /* authentication context */
//   OCIParam           *colLstDesc_ctl;        /* column list parameter handle */
//   OCIDirPathCtx      *dpctx_ctl;                      /* direct path context */
//   OCIDirPathColArray *dpca_ctl;           /* direct path column array handle */
//   OCIDirPathColArray *dpobjca_ctl;          /* dp column array handle for obj*/
//   OCIDirPathColArray *dpnestedobjca_ctl;  /* dp col array hndl for nested obj*/
//   OCIDirPathStream   *dpstr_ctl;                /* direct path stream handle */
//   ub1                *buf_ctl;    /* pre-alloc'd buffer for out-of-line data */
//   ub4                 bufsz_ctl;                 /* size of buf_ctl in bytes */
//   ub4                 bufoff_ctl;                     /* offset into buf_ctl */
//   ub4                *otor_ctl;                  /* Offset to Recnum mapping */
//   ub1                *inbuf_ctl;                 /* buffer for input records */
//   struct pctx         pctx_ctl;                     /* partial field context */
//   boolean             loadobjcol_ctl;             /* load to obj col(s)? T/F */

// number of rows in column array
nrow_ctl: c.ub4,

// number of columns in column array
ncol_ctl: c.ub2,

// environment handle
envhp_ctl: ?*c.OCIEnv,

// server handle
srvhp_ctl: ?*c.OCIServer,

// error handle
errhp_ctl: ?*c.OCIError,
