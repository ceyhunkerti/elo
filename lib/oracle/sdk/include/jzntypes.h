/* Copyright (c) 2015, 2020, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   NAME
     jzntypes.h - JSON Public types

   DESCRIPTION

   RELATED DOCUMENTS
 
   EXPORT FUNCTION(S)

   INTERNAL FUNCTION(S)

   EXAMPLES

   NOTES

   MODIFIED   (MM/DD/YY)
   dmcmahon    07/17/20 - Bug 31748678: use JsonDateTime for JZNVAL_TSTAMP
   dmcmahon    08/12/20 - Bug 31747292: OID/UUID in-line
   dmcmahon    06/28/20 - Define limits
   dmcmahon    06/04/20 - Move definition of jznFatalErrHandler to jznerr.h
   sriksure    03/24/20 - Bug 31075323: Add JZNVAL type, setter for LdiInterval
   sriksure    02/20/20 - Bug 30918101: Unpickle native binary values
   zliu        02/17/20 - bug 30922936: add JZN_ORA_TIMESTAMP_TZ_LEN
   zliu        04/07/19 - Bug 29599067: add JZN_ID_MAX_LEN
   dmcmahon    04/06/19 - Add JZNVAL_ID
   zliu        03/13/19 - Bug 29514638: enable JZNVAL_CANONICAL_FLOAT &
                          JZNVAL_CANONICAL_DOUBLE
   dmcmahon    11/22/18 - Bug 28962560: JZNVAL_BASE64 "signal" type
   zliu        11/10/18 - Bug 28908389: oson needs to handle 7 byte oralce 
                          timestamp storage format, add JZN_ORA_TIMESTAMP7_LEN
   sriksure    09/27/18 - Bug 28717127: Rationalize includes in public jzn*.h
   zliu        08/27/18 - Bug 28559663: add JZN_ORA_FLOAT_LEN, 
                          JZN_ORA_DOUBLE_LEN,JZN_ORA_DATE_LEN,
                          JZN_ORA_TIMESTAMP_LEN,JZN_ORA_YEAR_MON_DUR_LEN,
                          JZN_ORA_DAY_SEC_DUR_LEN   
   sriksure    08/23/18 - Bug 28546488: XDK C JSON APIs enhancements
   dmcmahon    07/08/18 - bug 28300790: JZNVAL_UUID, JZN_BSON_UUID_LEN
   dmcmahon    07/08/18 - bug 28300790: JZNVAL_UUID, JZN_BSON_UUID_LEN
   zliu        02/21/18 - Bug 27574330: add JZN_BSON_OID_LEN, 
                                        JZN_BSON_DEC128_LEN
   dmcmahon    01/29/18 - bug 27450657: decimal128 type
   bhammers    10/23/17 - bug 26767740: add JZNEV_COL_SIBL_NUL
   dmcmahon    04/24/17 - Bug 25945093: JZNVAL_ORA_TIME
   dmcmahon    07/15/16 - Fix position to accomodate large files
   dmcmahon    07/10/15 - Bug 21365846: moved error codes to jznerr.h
   dmcmahon    07/01/15 - DOM referential integrity errors
   bhammers    05/28/15 - add clob and blob
   bhammers    03/31/15 - added JZNERR_DOM_KEY_NOT_EXIST
   dmcmahon    03/12/15 - Creation

*/

#ifndef JZNTYPES_ORACLE
# define JZNTYPES_ORACLE

#ifndef ORATYPES
# include <oratypes.h> 
#endif

#ifndef ORASTRUC
# include <orastruc.h>
#endif

#ifndef XML_ORACLE
# include <xml.h>
#endif

#ifndef JZNERR_ORACLE
# include <jznerr.h>
#endif

/*---------------------------------------------------------------------------
                     PUBLIC TYPES AND CONSTANTS
  ---------------------------------------------------------------------------*/

/*
** Limits
*/
#define JZN_MAX_ARRAY_SLICES  0x4000 /* Maximum array subscripts            */
#define JZN_MAX_PATH_LENGTH   4000   /* Maximum length of a path string     */
#define JZN_MAX_FIELDNAME     256    /* Matches JZN0DOM_FIELDNAME_MAX       */
#define JZN_MAX_DEPTH         1024   /* Maximum number of nested containers */

#define JZN_BSON_DEC128_LEN          16
#define JZN_BSON_OID_LEN             12
#define JZN_BSON_UUID_LEN            16

/* storage length for fixed-length scalar datatypes encoding in OSON */
/* The storage length is taken from Oracle core encoding of scalar datatypes */
/* WARNING: disk persistent data compatible with Oracle scalar datatype
 * storage format and length. 
 */
#define JZN_ORA_DATE_LEN              7 
/* Oracle timestamp datatype has two valid lengths: 7 or 11 
 * 7 bytes is for timestamp without fractional second component
 * 11 bytes is for timestamp with non-zero fractional second component
 *
 * According to oracore3/include/ldi0.h:
 *
 * LDISTAMPZEROARRAYLEN  = 7 which is
 * (2 bytes for year, 1 for month, day, hour, minute, second )
 *
 * LDISTAMPARRAYLEN  = 11 which is
 * (2 bytes for year, 1 for month, day, hour, minute, second, 
 *  4 for fractional seconds)
 */
#define JZN_ORA_TIMESTAMP7_LEN       JZN_ORA_DATE_LEN
#define JZN_ORA_TIMESTAMP_LEN        11
#define JZN_ORA_TIMESTAMP_TZ_LEN     13
#define JZN_ORA_YEAR_MON_DUR_LEN      5
#define JZN_ORA_DAY_SEC_DUR_LEN      11
#define JZN_ORA_FLOAT_LEN             4 
#define JZN_ORA_DOUBLE_LEN            8
#define JZN_ORA_NUM_MAX_LEN          22 /* LNXM_NUM */
#define JZN_ID_MAX_LEN               127

#define JZN_INLINE_MAX_LEN           15

typedef sword jsonerr; /* ### Why bother defining this as a type? ### */

/*
** JSON Event types, max Value is 15 because we store event in 4 bits.
** If you need more, no problem, but jznEng.c needs to be modified!
*/
typedef enum jznevent
{
  JZNEV_START_OBJECT   = 0,
  JZNEV_END_OBJECT     = 1,
  JZNEV_START_PAIR     = 2,
  JZNEV_KEY            = 2,
  JZNEV_END_PAIR       = 3,
  JZNEV_START_ARRAY    = 4,
  JZNEV_END_ARRAY      = 5,
  JZNEV_ITEM           = 6,
  JZNEV_ERROR          = 7,
  JZNEV_END            = 8,
  JZNEV_POS            = 9,
  JZNEV_INVALID        = 10,
  JZNEV_START_COMPOUND = 11,  /* Start of BSON composite value */
  JZNEV_END_COMPOUND   = 12,  /* End of BSON composite value */
  JZNEV_ROW_SEPARATOR  = 13,  /* separate values belonging to different rows */
  JZNEV_COL_NO_VALUE   = 14,  /* no value (null column value is json table)  */
  JZNEV_COL_SIBL_NUL   = 15   /* NULL value for sibling branch */
} jznevent;

/*
** JSON Value types
*/
typedef enum jznvaltype
{
    JZNVAL_UNKNOWN = 0,
    /*
    ** JSON "native" types (all represented as strings)
    */
    JZNVAL_NONE    = 1,   /* set if this isn't a value, e.g. a field name */
    JZNVAL_NULL    = 2,   /* literal null */
    JZNVAL_STRING  = 3,   /* quoted string */
    JZNVAL_NUMBER  = 4,   /* unquoted numeric value */
    JZNVAL_FALSE   = 5,   /* literal false */
    JZNVAL_TRUE    = 6,   /* literal true */
    /*
    ** JSON Type system extensions:
    */
    JZNVAL_SINT    = 7,   /* Signed 32-bit integer - BSON, Avro, Protobuf */
    JZNVAL_SLONG   = 8,   /* Signed 64-bit integer - BSON, Avro, Protobuf */
    JZNVAL_UINT    = 9,   /* Unsigned 32-bit integer - Protobuf */
    JZNVAL_ULONG   = 10,  /* Unsigned 64-bit integer - Protobuf */
    JZNVAL_FLOAT   = 11,  /* 32-bit float - Avro, Protobuf */
                          /* Oracle canonical float DTYBFLOAT is handled
                           * by storing it in oradbfltbin[4]
                           */
    JZNVAL_DOUBLE  = 12,  /* 64-bit float - BSON, Avro, Protobuf */
                          /* Oracle canonical double DTYBDOUBLE is handled
                           * by storing it in oradbdvbin[8]
                           */
    JZNVAL_BINARY  = 13,  /* byte array - BSON, Avro, Protobuf */
    JZNVAL_TSTAMP  = 14,  /* UTC timestamp, in-line form */
    JZNVAL_OID     = 15,  /* MongoDB 12-byte binary object ID */
    JZNVAL_PTR     = 15,  /* MongoDB 12-byte pointer (deprecated) */
    JZNVAL_UUID    = 16,  /* UUID - 16 byte raw */
    /*
    ** ### Mongo also has:
    ** ###   binary sub-types
    ** ###   type for symbol (deprecated)
    ** ###   type for pointer (deprecated)
    ** ###   type for Javascript code
    ** ###   type for internal time stamp
    ** ###   types for min/max keys
    ** ###   compound regex (expression, options)
    ** ###   compound code  (JS code, document)
    */
    /*
    ** Oracle native types
    */
    /* Oracle Compress Tree JSON Binary (OSON) for SQL Scalar datatype */
    /* Note Oracle OSON uses this Oracle native binary encoding that
     * provides efficent code paths to sql/json path engine without any
     * type comparsion.
     * Note the underlying binary value accessible from
     * jzn_sval_union is jzn_sval_union.bval.value and 
     * jzn_val_union.bval.valuelength
     * For in-memory cached OSON byte buffer, the jzn_sval_union.bval.value
     * directly points to the underlying OCGBSON byte  buffer.
     * For large OSON bytes, the jzn_sval_union.bval.value
     * points to obinval.oibintypbuf[].
     */
    JZNVAL_ORA_NUMBER        = 17, /* Oracle DTYNUM format   */
    JZNVAL_ORA_DATE          = 18, /* Oracle DTYDAT format   */
    JZNVAL_ORA_TIMESTAMP     = 19, /* Oracle DTYSTAMP format */
    JZNVAL_ORA_TIMESTAMPTZ   = 20, /* Oracle DTYSTZ format   */
    JZNVAL_ORA_YEARMONTH_DUR = 21, /* Oracle DTYIYM format   */
    JZNVAL_ORA_DAYSECOND_DUR = 22, /* Oracle DTYIDS format   */
    JZNVAL_OPAQUE            = 23, /* DOM instance (PL/SQL only) */
    JZNVAL_CLOB              = 24, /* CLOB (used for setClob)    */
    JZNVAL_CHR_STREAM        = 24, /* orastream * UTF8 string    */
    JZNVAL_BLOB              = 25, /* BLOB (used for setBlob)    */
    JZNVAL_BIN_STREAM        = 25, /* orastream * RAW data       */
    JZNVAL_ORA_TIME          = 26, /* Oracle DTYTIME format  */
                                   /* ### Do we ever need DEC128DPD? ### */
    JZNVAL_DECIMAL           = 27, /* DECIMAL 128 in BID form */
    JZNVAL_DEC128BID         = 27, /* BSON decimal128 BID     */
    /* Note we encode Mongo BSON sb4/sb8/dec128 using Oracle number.
     *
     * The reason is because the entire Oracle ecosystem RDBMS/OCI
     * stacks uses Oracle number format. Using Oracle number format minmizes
     * number format conversions for efficient SQL/JSON support.
     * To name a few:
     *  json_value (returning number requries just memcpy)
     *  sql/json path engine Range comparision just follows Oracle memcmp() 
     *  conversion.
     *
     * If someday, Oracle ecosystem natively supports sb4/sb8/deciaml128 
     * througout its RDBMS/OCI stack, then we can revisist this.
     *
     * However, to keep the BSON fidelity of sb4/sb8/dec128 so that we
     * can recover the provenance of those integer/dec128 datatypes,
     * we use following 3 jznvaltype to remember its type fidelity.
     */
    JZNVAL_SINT_ORANUM       = 28, /* Original Signed 32-bit integer
                                    * encoded using ORA number format
                                    */
    JZNVAL_SLONG_ORANUM      = 29, /* Original Signed 64-bit integer
                                    * encoded using ORA number format
                                    */
    JZNVAL_DEC128BID_ORANUM  = 30, /* Original dec128 format
                                    * encoded using ORA number format
                                    */
    JZNVAL_ID                = 31, /* ID (length + 0-127 bytes)       */
    /*
    ** These types are used only for conveying information to scalar
    ** constructor and serialization calls. They cannot be true events
    ** from the parsers and decoders, nor can they appear as DOM node
    ** scalar types. (See JZNVAL_CLOB, JZNVAL_BLOB for similar examples.)
    */
    JZNVAL_OCI_NUMBER        = 32, /* VARNUM (length byte + OraNum)   */
    JZNVAL_OCI_DATE          = 33, /* ldxg structure date+time        */
    JZNVAL_OCI_DATETIME      = 34, /* LdiDateTime structure           */
    JZNVAL_OCI_STRING        = 35, /* Pro*C style LONG VARCHAR struct */
    JZNVAL_OCI_RAW           = 36, /* Pro*C style LONG VARRAW struct  */
    JZNVAL_BASE64            = 37, /* binary to render in base-64     */
    /* These types are storable in OSON */
    JZNVAL_CANONICAL_FLOAT   = 38, /* Oracle float, canonical format  */
    JZNVAL_CANONICAL_DOUBLE  = 39, /* Oracle double, canonical format */
    /* This is another conveyance type */
    JZNVAL_OCI_INTERVAL      = 40  /* LdiInterval structure           */
} jznvaltype;

/* Timestamp type
** DESCRIPTION:
**   This serves as a helper structure for the types JZNVAL_ORA_DATE,
**   JZNVAL_ORA_TIMESTAMP and JZNVAL_ORA_TIMESTAMPTZ.
*/
typedef struct JsonDateTime
{
  sb2    year_JsonDateTime;
  ub1    month_JsonDateTime;
  ub1    day_JsonDateTime;
  ub1    hour_JsonDateTime;
  ub1    minute_JsonDateTime;
  ub1    second_JsonDateTime;
  ub1    flags_JsonDateTime;
  ub4    fsecond_JsonDateTime;
  sb1    tzHourOffset_JsonDateTime;
  sb1    tzMinuteOffset_JsonDateTime;
  ub2    unused_JsonDateTime;
} JsonDateTime;

/*
** Two bits of the flags are reserved for encoding subtype information.
** By convention, the less-precise types will have 0 values for the
** more precise fields in the structure that they don't use. TSTZ
** (all fields present) is the default.
*/
#define JZN_DATETIME_FLAG_SUBTYPE   0x03  /* Bit mask                  */
#define JZN_DATETIME_FLAG_DATE_ONLY 0x01  /* ANSI date without time   */
#define JZN_DATETIME_FLAG_DATE_TIME 0x02  /* Oracle date with time    */
#define JZN_DATETIME_FLAG_TIMESTAMP 0x03  /* SQL timestamp            */
#define JZN_DATETIME_FLAG_TSTZ      0x00  /* Timestamp with time zone */

/* Interval type - DAY TO SECOND
** DESCRIPTION:
**   This serves as a helper structure for type JZNVAL_ORA_DAYSECOND_DUR
*/
typedef struct JsonDayInterval
{
  sb4  days_JsonDayInterval;
  sb4  hours_JsonDayInterval;
  sb4  minutes_JsonDayInterval;
  sb4  seconds_JsonDayInterval;
  sb4  fseconds_JsonDayInterval;
} JsonDayInterval;

/* Interval type - YEAR TO MONTH
** DESCRIPTION:
**   This serves as a helper structure for type JZNVAL_ORA_YEARMONTH_DUR
*/
typedef struct JsonYearInterval
{
  sb4  years_JsonYearInterval;
  sb4  months_JsonYearInterval;
} JsonYearInterval; 

/*
** Union type for representing all supported scalar events, fields, items.
** This type is used in event records as well as in DOM interfaces.
*/
typedef union jzn_sval_union
{
    struct name_portion  /* Name information (if a field name event) */
    {
      oratext *name;
      ub4      namelen;
      ub1      dummy1;
      ub1      dummy2;
      ub1      dummy3;
      ub1      flags;
    } nam;
    struct string_value  /* JSON native values (as strings) */
    {
      oratext *value;
      ub4      valuelen;
      ub1      dummy1;
      ub1      dummy2;
      ub1      dummy3;
      ub1      flags;
    } val;
    struct container_portion  /* Containers (arrays/objects) */
    {
      size_t   position;   /* Byte offset of start/end */
      ub4      old_pos;    /* Legacy byte offset */
    } cont;
    struct binary_value  /* Value information (if binary) */
    {
      ub1     *value;
      ub4      valuelen;
#define JZN_BSON_BIN_SUBTYPES
      // ### Drop this oddball construct from BSON ?
      ub1      subtype;  /* Binary subtype */
      ub1      dummy2;
      ub1      dummy3;
      ub1      flags;
    } bval;
    union integer_value /* Value information (if signed long/int) */
    {
      sb8      longval;  /* If a 64-bit value */
      sb4      intval;   /* If a 32-bit value */
    } ival;
    union unsigned_value /* Value information (if unsigned long/int) */
    {
      ub8      longval;  /* If a 64-bit value */
      ub4      intval;   /* If a 32-bit value */
    } uval;
    struct float_value   /* Value information (for float) */
    {
      float    ftval;    /* native machine float value */
    } fval;
    struct double_value  /* Value information (for double) */
    {
      double   fpval;    /* native machine double value */
    } dval;
    struct time_value    /* Value information (if UTC or TSTAMP) */
    {
      JsonDateTime dtval;
    } tval;
    struct inline_value  /* Used for in-line Oracle Numbers/Dates/Timestamps */
    {
      /*
      ** To save memory for in-memory DOMs, many Oracle types can be stored
      ** in-line in the DOM node. All the temporal types can be stored
      ** in-line. Not all Oracle Numbers will fit, unfortunately, but all
      ** numbers that represent integers and long integers should fit.
      ** Most other numbers should too (up to 28 digits of precision).
      ** ### To-do: should we restore the in-line canonical representations
      ** ### for double/float?
      */
      ub1 val[JZN_INLINE_MAX_LEN];
      /* The length is last so that the data is aligned */
      ub1 len;
      /*
      ** This also places <len> in an empty space shared by all
      ** the out-of-line types so we can flag it if necessary.
      */
#define JZN_INLINE_FLAG      0x80  /* value is in-lined       */
#define JZN_INLINE_LEN_MASK  0x0F  /* length in bottom 4 bits */
    } inlineval;
    struct ptr_value     /* Value information (if UUID or OID) */
    {
      ub1      oidval[JZN_BSON_UUID_LEN];
    } pval;
    struct dec_value     /* Decimal 128 */
    {
      ub1      mem128[JZN_BSON_DEC128_LEN];  /* memcmp-friendly decimal128 */
    } xval;
    struct obuf_value    /* Oracle native data types */
    {
/* For all Oracle built-in types:
 *   JZNVAL_ORA_NUMBER JZNVAL_ORA_DATE JZNVAL_ORA_TIMESTAMP = 18
 *   JZNVAL_ORA_TIMESTAMPTZ JZNVAL_ORA_YEARMONTH_DUR JZNVAL_ORA_DAYSECOND_DUR
 * implemented by Oracle libcore library.
 * The maximum size is 22 bytes.
 * Since the underlying bin value can be longer than 8 bytes, we set the
 * obinptr_obinval as pointer into the underlying OSON byte array.
 *
 * max(LNXM_NUM =22,  LDITZSTAMPARRAYLEN = 13, LDIYEARMONTHARRAYLEN =5, 
 *   LDIDAYSECONDARRAYLEN =11)
 * So we define JZN_MAX_ORABIN_TYP_SZ as 22.
 *    #define JZN_MAX_ORABIN_TYP_SZ 22
 *    ub1  oibintypbuf[JZN_MAX_ORABIN_TYP_SZ];
 */
      ub1     *obinptr_obinval;
      ub4      oblen_obinval;
      ub1      dummy1;
      ub1      dummy2;
      ub1      dummy3;
      ub1      flags;
    } obinval;
    /*
    ** ### Other types not currently supported. The odd Mongo 2-part
    ** ### "timestamp" type is currently returned as a string.
    */
} jzn_sval_union;

/* Auxiliary Union of helper structures */
typedef union JsonOCIVal
{
  JsonDateTime      dt_JsonOCIVal;
  JsonDayInterval   dayInv_JsonOCIVal;
  JsonYearInterval  yrInv_JsonOCIVal;
  ub1               num_JsonOCIVal[JZN_ORA_NUM_MAX_LEN];
} JsonOCIVal;

/*
** ### This shouldn't be exposed, but it's used in the exposed OSON/IMC
** ### internal structures below. Shouldn't those be encapsulated?
*/
typedef struct jznDomHashTable jznDomHashTable;


#endif                                              /* JZNTYPES_ORACLE */

