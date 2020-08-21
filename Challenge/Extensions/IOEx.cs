using AE.CoreInterface;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;

namespace AE.CoreUtility
{

    /// <summary>
    /// Static class for handling binary serialization
    /// Supporting functions for IOBinary implementation
    /// </summary>
    public static class IOEx
    {
        /// <summary>
        /// Internal singleton class to allow SQL Server compilation to run in SAFE permission level
        /// </summary>
        #region Classes
        /// <summary>
        /// Binary IO Exception thrown when cannot serialize or deserialize
        /// </summary>
        public class IOExException : Exception
        {
            /// <summary>
            /// Default constructor
            /// </summary>
            public IOExException() { }
            /// <summary>
            /// Constructor with error message
            /// </summary>
            /// <param name="msg"></param>
            public IOExException(string msg) : base(msg) { }
        }

        public interface IIONull
        {
            object Get<C>();
        }
        /// <summary>
        /// Supporting struct for binary serialization of nullable objects
        ///
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public struct IONull<T> : IIONull where T : struct
        {
            public static explicit operator T(IONull<T> io) { return Value(io).Value; }
            public static implicit operator T?(IONull<T> io) { return Value(io); }
            public object Get<C>() { return io.HasValue && io.Value is C ? io.Value : (T?)null; }

            public Nullable<T> io;

            /// <summary>
            /// Default constructor
            /// </summary>
            public IONull(Nullable<T> n) { io = n; }

            /// <summary>
            /// Convert an object to a nullable object if it is an IONull object, otherwise assume that it is already the nullable type
            /// </summary>
            /// <param name="io"></param>
            /// <returns></returns>
            public static T? Value(object io) { return (io is IONull<T>) ? ((IONull<T>)io).io : io as T?; }
        }

        /// <summary>
        /// Supporting struct for binary date serialization direct from sql server using sql server native binary format for datetime2
        /// Precision can be 0-7 in sql server datetime2(precision) definition
        /// When precision is zero, milliseconds are excluded, for other values, milliseconds are not approximated
        /// Composition is Precision(1), Ticks(3), Days(3), [Offset(2)] as days since 1/1/0001 and ticks / precision = fractional seconds since midnight, offset minutes
        /// </summary>
        [DataContract(Namespace = "")]
        public struct DateTime2
        {
            /// <summary>
            /// DateTime2(precision) from sql server datatype
            /// Currently assuming a precision of 0 and stripping milliseconds from date to ensure that C# and sql server dates will match
            /// </summary>
            public const int Precision = 0;

            [Flags]
            private enum DateFlags : byte
            {
                /// <summary>
                /// Stored as little endian (sql server default)
                /// </summary>
                None = 0x00,
                /// <summary>
                /// Stored as big endian
                /// </summary>
                Swap = 0x01,
            }
            /// <summary>
            /// Swap endian byte order
            /// </summary>
            public bool Swap { get { return (Flags & DateFlags.Swap) == DateFlags.Swap; } set { Flags = (Flags & ~DateFlags.Swap) | ((value) ? DateFlags.Swap : DateFlags.None); } }

            [DataMember(EmitDefaultValue = false)]
            private DateFlags Flags { get; set; }

            /// <summary>
            /// Datetime to serialize
            /// Milliseconds are stripped off since they cannot be saved when precision is 0
            /// </summary>
            [DataMember(EmitDefaultValue = false)]
            public DateTime Date { get { return _Date; } set { _Date = value.NoMilliseconds(); } }
            private DateTime _Date;

            /// <summary>
            /// Binary IO for object serialization
            /// cast(datetime2(precision) as binary(7-9))
            /// </summary>
            public byte[] IO {
                get { return ExportDateTime2(Date, 0, Swap); }
                set { int ix = 0; Date = Get(value, ref ix, Swap); }
            }

            /// <summary>
            /// Binary IO for object serialization with swapped endian
            /// cast(datetime2(precision) as binary(7-9))
            /// </summary>
            public byte[] SwapIO {
                get { return ExportOne((byte)Flags).Concat(ExportDateTime2(Date, 0, Swap)).ToArray(); }
                set { int ix = 0; Flags = ((int)GetByte(value, ref ix)).AsEnum<DateFlags>(DateFlags.None); Date = Get(value, ref ix, Swap); }
            }

            /// <summary>
            /// Default constructor
            /// swap will serialize as big endian instead little endian
            /// </summary>
            /// <param name="dt"></param>
            /// <param name="swap"></param>
            public DateTime2(DateTime dt, bool swap = false) : this() { Flags = DateFlags.None; _Date = DateTime.MinValue; Date = dt; Swap = swap; }

            /// <summary>
            /// Parse binary serialized DateTime2 object from sql server binary storage
            /// Assuming native sql server byte order (little endian)
            /// </summary>
            /// <param name="buf">binary blob</param>
            /// <param name="ix">starting index</param>
            /// <param name="swap">reading from one endian to opposite endian byte order</param>
            /// <returns>parsed DateTime</returns>
            public static DateTime Get(byte[] buf, ref int ix, bool swap = false) { // cast(datetime2(precision) as binary(7-9)) with swap = false
                byte precision = GetByte(buf, ref ix); // precision 0-7
                byte[] ticks = new byte[8]; // SwapBuffer8 ?? (SwapBuffer8 = new byte[8]);
                double maxsecs = MaxSeconds;
                switch (precision) {
                    case 0x00: maxsecs = Math.Floor(maxsecs); goto case 0x02;
                    case 0x01: maxsecs = Math.Floor(maxsecs * 10) / 10; goto case 0x02;
                    case 0x02:
                        maxsecs = Math.Floor(maxsecs * 100) / 100;
                        ticks[(swap) ? 2 : 0] = buf[ix++];
                        ticks[(swap) ? 1 : 1] = buf[ix++];
                        ticks[(swap) ? 0 : 2] = buf[ix++];
                        ticks[3] =
                        ticks[4] =
                        ticks[5] =
                        ticks[6] =
                        ticks[7] = 0;
                        break;
                    case 0x03: maxsecs = Math.Floor(maxsecs * 1000) / 1000; goto case 0x04;
                    case 0x04:
                        maxsecs = Math.Floor(maxsecs * 10000) / 10000;
                        ticks[(swap) ? 3 : 0] = buf[ix++];
                        ticks[(swap) ? 2 : 1] = buf[ix++];
                        ticks[(swap) ? 1 : 2] = buf[ix++];
                        ticks[(swap) ? 0 : 3] = buf[ix++];
                        ticks[4] =
                        ticks[5] =
                        ticks[6] =
                        ticks[7] = 0;
                        break;
                    case 0x05: maxsecs = Math.Floor(maxsecs * 100000) / 100000; goto case 0x07;
                    case 0x06: maxsecs = Math.Floor(maxsecs * 1000000) / 1000000; goto case 0x07;
                    case 0x07:
                        maxsecs = Math.Floor(maxsecs * 10000000) / 10000000;
                        ticks[(swap) ? 4 : 0] = buf[ix++];
                        ticks[(swap) ? 3 : 1] = buf[ix++];
                        ticks[(swap) ? 2 : 2] = buf[ix++];
                        ticks[(swap) ? 1 : 3] = buf[ix++];
                        ticks[(swap) ? 0 : 4] = buf[ix++];
                        ticks[5] =
                        ticks[6] =
                        ticks[7] = 0;
                        break;
                    default: throw new IOExException("Invalid date precision");
                }
                byte[] days = new byte[4]; // SwapBuffer4 ?? (SwapBuffer4 = new byte[4]);
                days[(swap) ? 2 : 0] = buf[ix++];
                days[(swap) ? 1 : 1] = buf[ix++];
                days[(swap) ? 0 : 2] = buf[ix++];
                days[3] = 0;
                int x = 0;
                double tm = GetUInt64(ticks, ref x, false) / Math.Pow(10, (double)precision); // fractional seconds since midnight
                x = 0;
                uint dt = GetUInt32(days, ref x, false); // days since 1/1/0001
                if (dt <= 0 && tm <= 0) return DateTime.MinValue;
                if (dt >= MaxDays && tm >= maxsecs) return DateTime.MaxValue;
                DateTime ret = DateTime2IOMin.AddDays(dt).AddSeconds(tm);
                if (ret >= DateTime2IOMax) ret = DateTime.MaxValue;
                if (ret <= DateTime2IOMin) ret = DateTime.MinValue;
                return ret;
            }

            /// <summary>
            /// Parse binary serialized DateTime2 object from sql server binary storage into a new DateTime2 object
            /// Extra preceeding 1 byte indicates the endianness
            /// </summary>
            /// <param name="buf">binary blob</param>
            /// <param name="ix">starting index</param>
            /// <returns>DateTime2 object preserving endianness</returns>
            public static DateTime2 GetSwap(byte[] buf, ref int ix) {
                byte flags = GetByte(buf, ref ix);
                bool swap = (flags & (byte)DateTime2.DateFlags.Swap) == (byte)DateTime2.DateFlags.Swap;
                DateTime dt = Get(buf, ref ix, swap);
                return new DateTime2(dt, swap) { Flags = ((int)flags).AsEnum<DateTime2.DateFlags>(DateTime2.DateFlags.None) };
            }

            /// <summary>
            /// Parse binary serialized DateTimeOffset object from sql server binary storage
            /// Assuming native sql server byte order (little endian)
            /// </summary>
            /// <param name="buf">binary blob</param>
            /// <param name="ix">starting index</param>
            /// <param name="swap">reading from one endian to opposite endian byte order</param>
            /// <returns></returns>
            public static DateTimeOffset? GetOffset(byte[] buf, ref int ix) { // cast(datetimeoffset(precision) as binary(11-15)) with swap = true
                DateTime dt = Get(buf, ref ix, false); // sql doesn't swap bytes, so no need for bigendian
                short off = GetInt16(buf, ref ix, false);
                DateTimeOffset ret = new DateTimeOffset(dt.AddMinutes(off).Ticks, new TimeSpan(0, off, 0));
                return (ret < DateTimeOffset.MaxValue) ? ret : (DateTimeOffset?)null;
            }
        }
        #endregion Classes

        #region Enums
        public enum TypeCodeEx : short
        {
            Empty = 0,
            Nullable = 100,
            TimeSpan = 101,
            IOBinary = 102,
            NullableDateTimeOffset = 103,
            NullableTimeSpan = 104,
            NullableDateTime = 105,
            ByteArrays = 106,
            StringArrays = 107,
            DateTime2 = 108,
            NullableDateTime2 = 109,
            Guid = 110,
            GuidArray = 111,
            DAKey = 112,
            DAKeyArray = 123,
            DAKeyVirtual = 124,
            DAKeyVirtualArray = 125,
            NullableDAKey = 126,
            NullableDAKeyVirtual = 127,
            ByteArray = TypeCode.Byte,
            ShortArray = TypeCode.Int16,
            UInt16Array = TypeCode.UInt16,
            IntArray = TypeCode.Int32,
            UInt32Array = TypeCode.UInt32,
            Int64Array = TypeCode.Int64,
            UInt64Array = TypeCode.UInt64,
            DoubleArray = TypeCode.Double,
            StringArray = TypeCode.String,
            DecimalArray = TypeCode.Decimal,
            DateTimeOffset = TypeCode.DateTime,
            DateTimeArray = TypeCode.DateTime,
        };
        #endregion Enums

        #region Statics
        public delegate byte[] ExportIODelegate<T>(T val) where T : IOBinary;
        public delegate T CreateIODelegate<T>(byte[] io) where T : IOBinary;

        /// <summary>
        /// Custom format string indicating that instead of a specified format string, the default Unit format for the current culture should be used
        /// </summary>
        public const string c_eUnits = "[eUnits]"; // if Format == c_eUnits, then use the eUnit default format
        public const uint BigEndianMagic = 0xFFABCDFE;
        public const uint LittleEndianMagic = 0xFECDABFF;
        [Obsolete] public const uint BigEndianMagic1 = BigEndianMagic + 0x00000100;
        public const uint BigEndianMagic2 = BigEndianMagic + 0x00000200;
        [Obsolete] public const uint LittleEndianMagic1 = LittleEndianMagic + 0x00010000;
        public const uint LittleEndianMagic2 = LittleEndianMagic + 0x00020000;
        public readonly static byte[] Empty = new byte[0];

        private readonly static object NullString = new object(); // use this when you want to include a null string
        private readonly static object NullBytes = new object(); // use this when you want to include a null byte[]
        private readonly static object NullShortArray = new object(); // use this when you want to include a null short[]
        private readonly static object NullUShortArray = new object(); // use this when you want to include a null ushort[]
        private readonly static object NullIntArray = new object(); // use this when you want to include a null int[]
        private readonly static object NullUIntArray = new object(); // use this when you want to include a null uint[]
        private readonly static object NullLongArray = new object(); // use this when you want to include a null long[]
        private readonly static object NullULongArray = new object(); // use this when you want to include a null ulong[]
        private readonly static object NullDoubleArray = new object(); // use this when you want to include a null double[]
        private readonly static object NullStringArray = new object(); // use this when you want to include a null string[]
        private readonly static object NullStringArrays = new object(); // use this when you want to include a null IEnumerable<string[]>
        private readonly static object NullByteArrays = new object(); // use this when you want to include a null IEnumerable<byte[]>
        private readonly static object NullGuidArray = new object(); // use this when you want to include a null Guid[]
        private readonly static object NullDAKeyArray = new object(); // use this when you want to include a null DAKey[]
        private readonly static object NullDAKeyVirtualArray = new object(); // use this when you want to include a null DAKey[]
        private readonly static object NullDecimalArray = new object(); // use this when you want to include a null decimal[]
        private readonly static object NullDateTimeArray = new object(); // use this when you want to include a null DateTime[]

        public readonly static DateTime DateTimeIOMin = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public readonly static DateTime DateTimeIOMax = new DateTime(2079, 6, 6, 23, 59, 0, DateTimeKind.Utc);
        public readonly static DateTime DateTime2IOMin = new DateTime(0001, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public readonly static DateTime DateTime2IOMax = new DateTime(9999, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc);
        public readonly static DateTimeOffset DateTimeOffsetIOStart = new DateTimeOffset(1900, 1, 1, 0, 0, 0, TimeSpan.Zero);
        public readonly static ushort MaxMinutes = (ushort)(DateTimeIOMax - DateTimeIOMax.Date).TotalMinutes;
        public readonly static double MaxSeconds = (DateTime2IOMax - DateTime2IOMax.Date).TotalSeconds;
        public readonly static uint MaxDays = (uint)(DateTime2IOMax - DateTime2IOMin).TotalDays;
        private readonly static bool? nonnullable = null;
        public const int BitsInByte = 8;
        #endregion Statics

        #region Endianness
        public static byte[] Swap(this byte[] v, bool swap = true, bool inplace = false) {
            if (!swap) return v;
            int len = v?.Length ?? 0;
            if (len <= 0) return v;
            if (len > sizeof(ulong) || len % 2 != 0) throw new IOExException("Cannot swap buffers greater than 8 bytes or with odd number of bytes");
            byte[] ret = (inplace) ? v : new byte[len];
            for (int i = 0; i < len / 2; i++) {
                byte b = v[i];
                ret[i] = v[len - i - 1];
                ret[len - i - 1] = b;
            }
            return ret;
        }
        public static byte[] Swap(this byte[] buf, int ix, int sz) {
            if (sz > sizeof(ulong) || sz % 2 != 0) throw new IOExException("Cannot swap buffers greater than 8 bytes or with odd number of bytes");
            return Swap(GetBuffer(buf, sz, ref ix), true, true);
        }
        #endregion Endianness

        #region BinData
        public static void PutInt32(MemoryStream ms, int i, bool swap = false) { ms.Write(BinaryEx.GetBytes(i).Swap(swap, true), 0, sizeof(Int32)); }
        #endregion BinData

        #region Put
        public static void Put(ulong v, byte[] buf, ref int ix, bool swap = false, int sz = sizeof(ulong)) {
            if (sz <= 0) return;
            if (swap) v = v.Swap();
            buf[ix++] = (byte)((v & 0x00000000000000ffUL));
            if (--sz <= 0) return;
            buf[ix++] = (byte)((v & 0x000000000000ff00UL) >> 8);
            if (--sz <= 0) return;
            buf[ix++] = (byte)((v & 0x0000000000ff0000UL) >> 16);
            if (--sz <= 0) return;
            buf[ix++] = (byte)((v & 0x00000000ff000000UL) >> 24);
            if (--sz <= 0) return;
            buf[ix++] = (byte)((v & 0x000000ff00000000UL) >> 32);
            if (--sz <= 0) return;
            buf[ix++] = (byte)((v & 0x0000ff0000000000UL) >> 40);
            if (--sz <= 0) return;
            buf[ix++] = (byte)((v & 0x00ff000000000000UL) >> 48);
            if (--sz <= 0) return;
            buf[ix++] = (byte)((v & 0xff00000000000000UL) >> 56);
        }

        public static void PutBuffer(MemoryStream ms, byte[] buf) { PutBuffer(ms, false, buf, false); }
        public static void PutBuffer(MemoryStream ms, bool addsz, byte[] buf, bool swap = false) {
            if (addsz) Put(ms, buf?.Length ?? int.MaxValue, swap);
            else if (buf == null || buf.Length <= 0) throw new IOExException("Cannot export null or empty buffers unless writing the buffer size");
            if (buf?.Length > 0) ms.Write(buf, 0, buf.Length);
        }
        public static void PutTiny(MemoryStream ms, string s) { PutBuffer(ms, ExportStrTiny(s)); }
        public static void Put<T>(MemoryStream ms, T obj, bool swap = false) {
            if (obj == null) throw new IOExException("Cannot export null values");
            byte[] put = ExportOne(obj, swap);
            PutBuffer(ms, put);
        }

        //public static void Put<T>(MemoryStream ms, params T[] vals) { Put(ms, false, false, vals); }
        //public static void Put<T>(MemoryStream ms, bool addcount, bool swap, params T[] vals) {
        //	if (!addcount && vals?.Any() != true) throw new IOExException("Cannot export a null or empty list of objects unless writing the buffer count");
        //	PutBuffer(ms, Export(addcount, vals, swap));
        //}
        //public static void Put<T>(MemoryStream ms, bool addcount, bool swap, IEnumerable<T> vals) {
        //	if (!addcount && vals?.Any() != true) throw new IOExException("Cannot export a null or empty list of objects unless writing the buffer count");
        //	PutBuffer(ms, Export(addcount, vals, swap));
        //}
        #endregion Put

        #region Get
        public static string GetString(byte[] buf, ref int ix, bool swap = false) {
            int sz = GetInt32(buf, ref ix, swap);
            if (sz == int.MaxValue) return null;
            if (ix + sz > buf.Length) throw new IOExException("Invalid string length");
            if (sz <= 0) return String.Empty;
            string ret = Encoding.ASCII.GetString(buf, ix, sz);
            ix += sz;
            return ret;
        }
        public static string GetStringTiny(byte[] buf, ref int ix) {
            byte sz = GetByte(buf, ref ix);
            if (sz == byte.MaxValue) return null;
            if (ix + sz > buf.Length) throw new IOExException("Invalid string length");
            if (sz <= 0) return String.Empty;
            string ret = Encoding.UTF8.GetString(buf, ix, sz);
            ix += sz;
            return ret;
        }

        public static byte GetByte(byte[] buf, ref int ix) { byte ret = buf[ix]; ix += sizeof(byte); return ret; }
        public static bool GetBool(byte[] buf, ref int ix) { bool ret = BinaryEx.ToBoolean(buf, ix); ix += sizeof(bool); return ret; }
        public static sbyte GetSByte(byte[] buf, ref int ix) { sbyte ret = (sbyte)buf[ix]; ix += sizeof(sbyte); return ret; }
        public static short GetInt16(byte[] buf, ref int ix, bool swap = false) { short ret = BinaryEx.ToInt16((swap) ? Swap(buf, ix, sizeof(short)) : buf, (swap) ? 0 : ix); ix += sizeof(short); return ret; } //if (swap) Swap2(ref buf, ix);
        public static ushort GetUInt16(byte[] buf, ref int ix, bool swap = false) { ushort ret = BinaryEx.ToUInt16((swap) ? Swap(buf, ix, sizeof(ushort)) : buf, (swap) ? 0 : ix); ix += sizeof(ushort); return ret; } //if (swap) Swap2(ref buf, ix);
        public static int GetInt32(byte[] buf, ref int ix, bool swap = false) { int ret = BinaryEx.ToInt32((swap) ? Swap(buf, ix, sizeof(int)) : buf, (swap) ? 0 : ix); ix += sizeof(int); return ret; } //if (swap) Swap4(ref buf, ix);
        public static uint GetUInt32(byte[] buf, ref int ix, bool swap = false) { uint ret = BinaryEx.ToUInt32((swap) ? Swap(buf, ix, sizeof(uint)) : buf, (swap) ? 0 : ix); ix += sizeof(uint); return ret; } //if (swap) Swap4(ref buf, ix);
        public static long GetInt64(byte[] buf, ref int ix, bool swap = false) { long ret = BinaryEx.ToInt64((swap) ? Swap(buf, ix, sizeof(long)) : buf, (swap) ? 0 : ix); ix += sizeof(long); return ret; } //if (swap) Swap8(ref buf, ix);
        public static ulong GetUInt64(byte[] buf, ref int ix, bool swap = false) { ulong ret = BinaryEx.ToUInt64((swap) ? Swap(buf, ix, sizeof(ulong)) : buf, (swap) ? 0 : ix); ix += sizeof(ulong); return ret; } //if (swap) Swap8(ref buf, ix);
        /// <summary>
        ///	GetSingle - Deserialize a 32 bit floating point number.
        ///	NOTE: Use GetSingleDouble(…) instead of GetSingle(…) to deserialize into a double.
        ///	Casting a float to a double can cause a slight error to appear, for example:
        ///		(double)7.95f ==> 7.9499998092651367
        ///		Convert.ToDouble(Convert.ToDecimal(7.95f)) ==> 7.95
        /// </summary>
        /// <param name="buf">Block of serialized bytes</param>
        /// <param name="ix">Starting index where binary serialized string is expected</param>
        /// <param name="swap">String length is specified as a bigendian value</param>
        public static float GetSingle(byte[] buf, ref int ix, bool swap = false) { float ret = BinaryEx.ToSingle((swap) ? Swap(buf, ix, sizeof(float)) : buf, (swap) ? 0 : ix); ix += sizeof(float); return ret; } //if (swap) Swap4(ref buf, ix);
        public static double GetDouble(byte[] buf, ref int ix, bool swap = false) { double ret = BinaryEx.ToDouble((swap) ? Swap(buf, ix, sizeof(double)) : buf, (swap) ? 0 : ix); ix += sizeof(double); return ret; } //if (swap) Swap8(ref buf, ix);
        public static decimal GetDecimal(byte[] buf, ref int ix, bool swap = false) { return new Decimal(new int[] { GetInt32(buf, ref ix, swap), GetInt32(buf, ref ix, swap), GetInt32(buf, ref ix, swap), GetInt32(buf, ref ix, swap) }); }
        public static Guid GetGuid(byte[] buf, ref int ix, bool swap = false) { return new Guid(GetBuffer(buf, BinaryEx.GuidSize, ref ix)); }
        public static TimeSpan GetTimeSpan(byte[] buf, ref int ix, bool swap = false) { return new TimeSpan(GetInt64(buf, ref ix, swap)); }
        public static DateTime GetDateTime(byte[] buf, ref int ix, bool swap = false) { long dt = GetInt64(buf, ref ix, swap); return new DateTime(dt, ((int)GetByte(buf, ref ix)).AsEnum<DateTimeKind>(DateTimeKind.Unspecified)); }
        public static DateTimeOffset GetDateTimeOffset(byte[] buf, ref int ix, bool swap = false) { long dt = GetInt64(buf, ref ix, swap); return new DateTimeOffset(dt, new TimeSpan(GetInt64(buf, ref ix, swap))); }
        public static byte[] GetBuffer(byte[] buf, ref int ix, bool swap = false) { return GetBuffer(buf, GetInt32(buf, ref ix, swap), ref ix); }
        public static byte[] GetBufferShort(byte[] buf, ref int ix, bool swap = false) { return GetBuffer(buf, GetInt16(buf, ref ix, swap), ref ix); }
        public static byte[] GetBuffer(byte[] buf, int sz, ref int ix) {
            if (sz == int.MaxValue) return null;
            if (ix + sz > buf.Length) throw new IOExException("Invalid buffer length");
            byte[] ret = new byte[sz];
            Array.Copy(buf, ix, ret, 0, sz);
            ix += sz;
            return ret;
        }
        public static Guid ToGuid(this Version v, bool bigendian = true) {
            return v == null ? Guid.Empty :
                new Guid(v.Major.GetBytes(bigendian)
                .Concat(v.Minor.GetBytes(bigendian))
                .Concat(v.Build.GetBytes(bigendian))
                .Concat(v.Revision.GetBytes(bigendian)).ToArray());
        }
        #endregion Get

        #region Export
        public static Type FromCode(this TypeCode tc) {
            switch (tc) {
                case TypeCode.Boolean: return typeof(bool);
                case TypeCode.Char: return typeof(char);
                case TypeCode.SByte: return typeof(sbyte);
                case TypeCode.Byte: return typeof(byte);
                case TypeCode.Int16: return typeof(short);
                case TypeCode.UInt16: return typeof(ushort);
                case TypeCode.Int32: return typeof(int);
                case TypeCode.UInt32: return typeof(uint);
                case TypeCode.Int64: return typeof(long);
                case TypeCode.UInt64: return typeof(ulong);
                case TypeCode.Single: return typeof(float);
                case TypeCode.Double: return typeof(double);
                case TypeCode.Decimal: return typeof(decimal);
                case TypeCode.String: return typeof(string);
                case TypeCode.DateTime: return typeof(DateTime);
                case TypeCode.Object: return typeof(object);
                default: return null;
            }
        }
        public static TypeCode GetTypeCode(this object obj) {
            if (obj == null) return TypeCode.Object;
            Type t = null;
            if (obj is Type) t = (Type)obj;
            else if (obj is IConvertible) return ((IConvertible)obj).GetTypeCode();
            else t = obj.GetType();
            return Type.GetTypeCode(t.NonNullableType() ?? t);
        }
        public static TypeCode GetTypeCode(ref object io, out TypeCodeEx typ) {
            bool isnullstr = ReferenceEquals(NullString, io);
            bool isnullbyt = ReferenceEquals(NullBytes, io);
            bool isnullshortarr = ReferenceEquals(NullShortArray, io);
            bool isnullushortarr = ReferenceEquals(NullUShortArray, io);
            bool isnullintarr = ReferenceEquals(NullIntArray, io);
            bool isnulluintarr = ReferenceEquals(NullUIntArray, io);
            bool isnulllongarr = ReferenceEquals(NullLongArray, io);
            bool isnullulongarr = ReferenceEquals(NullULongArray, io);
            bool isnulldblarr = ReferenceEquals(NullDoubleArray, io);
            bool isnullstrarr = ReferenceEquals(NullStringArray, io);
            bool isnullstrarrs = ReferenceEquals(NullStringArrays, io);
            bool isnullbytarrs = ReferenceEquals(NullByteArrays, io);
            bool isnullguidarr = ReferenceEquals(NullGuidArray, io);
            bool isnulldecarr = ReferenceEquals(NullDecimalArray, io);
            bool isnulldatearr = ReferenceEquals(NullDateTimeArray, io);
            bool isnullvkeyarr = ReferenceEquals(NullDAKeyVirtualArray, io);
            bool isnullkeyarr = ReferenceEquals(NullDAKeyArray, io);
            bool isnulldtoff = io is IONull<DateTimeOffset>;
            bool isnulldt = io is IONull<DateTime>;
            bool isnulldt2 = io is IONull<DateTime2>;
            bool isnullts = io is IONull<TimeSpan>;
            bool isdt = io is DateTime || io is DateTime?;
            bool isdt2 = io is DateTime2 || io is DateTime2?;
            bool isdtoff = io is DateTimeOffset || io is DateTimeOffset?;
            bool ists = io is TimeSpan || io is TimeSpan?;
            if (isnullstr || isnullbyt || isnullshortarr || isnullushortarr || isnullintarr || isnulluintarr || isnulllongarr || isnullulongarr || isnulldblarr || isnullstrarr || isnullstrarrs || isnullbytarrs || isnullguidarr || isnulldecarr) io = null;
            TypeCode ret = isnullstr ? TypeCode.String :
                (isdtoff || isnulldtoff || isnulldt || ists || isnullts || isdt2 || isnulldt2) ? TypeCode.DateTime :
                GetTypeCode(io);
            TypeCodeEx def = (io == null) ? TypeCodeEx.Nullable : (io is IOBinary) ? TypeCodeEx.IOBinary : TypeCodeEx.Empty;
            object ioval = io;
            Type t = ioval?.GetType();
#if !SQLCLRDLL
            MemberInfo mi = null;
            IIONull _io = io as IIONull;
            if (_io != null) {
                Type dummyt = t.IsGenericType ? t.GetGenericArguments().FirstOrDefault() : null;
                if (dummyt != null && ret == TypeCode.Object) ret = Type.GetTypeCode(dummyt);
                def = TypeCodeEx.Nullable;
                ioval = _io.Get<object>();
                //Type gt = (gen?.Length == 1) ? typeof(IONull<>).MakeGenericType(gen) : null;
                //if (gt?.IsAssignableFrom(t) == true) {
                //	def = TypeCodeEx.Nullable;
                //	mi = ReflectEx.GetMember(io, "io");
                //	if (mi != null) ioval = ReflectEx.GetValue(mi, io);
                //}
            }
            t = ioval?.GetType();
#endif // !SQLCLRDLL
            //if (ioval is IDAKeys<DAKeyVirtual>) ioval = ((IDAKeys<DAKeyVirtual>)ioval).Keys?.Select(z => z.VKEY).ToArray();
            //if (ioval is IDAKeys<DAKey>) ioval = ((IDAKeys<DAKey>)ioval).Nums?.ToArray();
            //if (ioval is IDAKeyVirtual) ioval = ((IDAKeyVirtual)ioval).VKEY;
            //if (ioval is IDAKey) ioval = ((IDAKey)ioval).Num;
            //if (ioval is IEnumerable<IDAKeyVirtual>) ioval = ((IEnumerable<IDAKeyVirtual>)ioval).Select(z => z.VKEY).ToArray();
            io = ioval;
            typ = (isnulldtoff || (isdtoff && io == null)) ? TypeCodeEx.NullableDateTimeOffset :
                (isdtoff) ? TypeCodeEx.DateTimeOffset :
                (isnulldt || (isdt && io == null)) ? TypeCodeEx.NullableDateTime :
                (isdt) ? TypeCodeEx.Empty :
                (isnulldt2 || (isdt2 && io == null)) ? TypeCodeEx.NullableDateTime2 :
                (isdt2) ? TypeCodeEx.DateTime2 :
                (isnullts || (ists && io == null)) ? TypeCodeEx.NullableTimeSpan :
                (io is TimeSpan) ? TypeCodeEx.TimeSpan :
                (io is Guid) ? TypeCodeEx.Guid :
                (io is byte[] || isnullbyt) ? TypeCodeEx.ByteArray :
                (io is short[] || isnullshortarr) ? TypeCodeEx.ShortArray :
                (io is int[] || isnullintarr) ? TypeCodeEx.IntArray :
                (io is ushort[] || isnullushortarr) ? TypeCodeEx.UInt16Array :
                (io is uint[] || isnulluintarr) ? TypeCodeEx.UInt32Array : // || io is IEnumerable<UColor>|| io is UColors
                (io is long[] || isnulllongarr) ? TypeCodeEx.Int64Array : // || io is IEnumerable<DAKey> || io is DAKeys
                (io is ulong[] || isnullulongarr) ? TypeCodeEx.UInt64Array : // || io is IEnumerable<DAKey> || io is DAKeys
                (io is double[] || isnulldblarr) ? TypeCodeEx.DoubleArray :
                (io is string[] || isnullstrarr) ? TypeCodeEx.StringArray :
                (io is IEnumerable<string[]> || isnullstrarrs) ? TypeCodeEx.StringArrays :
                (io is IEnumerable<byte[]> || isnullbytarrs) ? TypeCodeEx.ByteArrays :
                (io is Guid[] || isnullguidarr) ? TypeCodeEx.GuidArray :
                (io is decimal[] || isnulldecarr) ? TypeCodeEx.DecimalArray :
                (io is DateTime[] || isnulldatearr) ? TypeCodeEx.DateTimeArray :
                def;
            Type tt = (ret != TypeCode.Object) ? FromCode(ret) : null;
            if (ret != TypeCode.Object || typ != def || io == null || t == null) return ret;
            // type is not recognized, see if it is a collection and cast it to an array of a recognized underlying type
            IEnumerable ioarr = io as IEnumerable;
            if (ioarr == null) return ret;
            TypeCode tc = TypeCode.Empty;
            if (t.IsArray == true) {
                tc = Type.GetTypeCode(t.GetElementType());
                tt = (tc != TypeCode.Object) ? FromCode(tc) : null;
            }
            if (tt == null) {
                foreach (Type et in t.GetInterfaces().Where(z => z.IsGenericType && z.GetGenericTypeDefinition() == typeof(IEnumerable<>))) {
                    Type[] gen = et.GetGenericArguments();
                    if (gen?.Length != 1) continue;
                    tc = Type.GetTypeCode(gen.First());
                    tt = (tc != TypeCode.Object) ? FromCode(tc) : null;
                    if (tt != null) break;
                }
            }
            if (tt == null) return ret;
            TypeCodeEx ttc = ((int)tc).AsEnum(TypeCodeEx.Empty);
            if (ttc == TypeCodeEx.Empty) return ret;
            IEnumerable<object> objs = ioarr.Enumerate<object>();
            Array arr = Array.CreateInstance(tt, objs.Count());
            io = arr;
            typ = ttc;
            return ret;
            //MethodInfo mm = ReflectEx.GetMethod(typeof(CollectionEx), nameof(CollectionEx.EnumerateArray), false, true, false, true, typeof(IEnumerable), typeof(int))?.MakeGenericMethod(tt);
            //if (mm == null) return ret;
            //ioval = mm.Invoke(null, new object[] { io , 0});
            //if (ioval == null) return ret;
            //io = ioval;
            //typ = ttc;
            //return ret;
        }
        public static bool IsNullable(object obj) { return IsNullable(obj as Type ?? obj?.GetType()); }
        public static bool IsNullable(this Type t) { return t == null || Nullable.GetUnderlyingType(t) != null || (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IONull<>)); }
        public static Type NonNullableType(this Type t) { return t == null ? t : t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IONull<>) ? t.GetGenericArguments()?.FirstOrDefault() ?? t : Nullable.GetUnderlyingType(t) ?? t; }
        public static bool IsNullable(TypeCode code, TypeCodeEx typ) {
            switch (code) {
                case TypeCode.String:
                case TypeCode.Object: return false;
            }
            switch (typ) {
                case TypeCodeEx.Nullable:
                case TypeCodeEx.NullableDateTimeOffset:
                case TypeCodeEx.NullableTimeSpan:
                case TypeCodeEx.NullableDateTime:
                case TypeCodeEx.NullableDateTime2:
                case TypeCodeEx.NullableDAKey:
                case TypeCodeEx.NullableDAKeyVirtual:
                    return true;
                default: return false;
            }
        }

        /// <summary>
        /// Primary Binary IO for a single object.
        /// </summary>
        /// <param name="io">Object to serialize</param>
        /// <param name="swap">Big endian swap</param>
        public static byte[] ExportOne(object io, bool swap = false) { TypeCode code; TypeCodeEx typ; return ExportOne(io, out code, out typ, swap); }

        /// <summary>
        /// Primary Binary IO for a single object that returns detected type and code.
        /// Null objects do not throw an exception, but Put calls to a memory stream do not allow nulls, so nulls must be handled by the calling function.
        /// </summary>
        /// <param name="io">Object to serialize</param>
        /// <param name="code">Type code determined for the object</param>
        /// <param name="typ">Extended type determined for the object</param>
        /// <param name="swap">Big endian swap</param>
        public static byte[] ExportOne(object io, out TypeCode code, out TypeCodeEx typ, bool swap = false) {
            if (io == null) {
                code = TypeCode.Empty;
                typ = TypeCodeEx.Empty;
                return null;
            }
            try {
                code = GetTypeCode(ref io, out typ);
                byte[] nio = ExportNullable((IsNullable(code, typ)) ? io == null : nonnullable);
                switch (code) {
                    case TypeCode.Boolean: return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes((bool)io)).ToArray();
                    case TypeCode.Char: return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes((char)io)).ToArray();
                    // 11/29/2014: Fixed SByte and Byte cases to return byte[1].  Previously, using BinaryEx.GetBytes((byte)io), this returned a two byte array
                    //              because the char version of BinaryEx.GetBytes was being called, and char is 2 bytes.
                    //      case TypeCode.SByte: return BinaryEx.GetBytes((sbyte)io);
                    //      case TypeCode.Byte: return BinaryEx.GetBytes((byte)io);
                    case TypeCode.SByte: return (io == null) ? nio : nio.Concat(unchecked((byte)(sbyte)io).AsArray()).ToArray();
                    case TypeCode.Byte: return (io == null) ? nio : nio.Concat(((byte)io).AsArray()).ToArray();
                    case TypeCode.Int16: return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes((short)io).Swap(swap)).ToArray();
                    case TypeCode.UInt16: return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes((ushort)io).Swap(swap)).ToArray();
                    case TypeCode.Int32: return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes((int)io).Swap(swap)).ToArray();
                    case TypeCode.UInt32: return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes((uint)io).Swap(swap)).ToArray();
                    case TypeCode.Int64: return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes((long)io).Swap(swap)).ToArray();
                    case TypeCode.UInt64: return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes((ulong)io).Swap(swap)).ToArray();
                    case TypeCode.Single: return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes((float)io).Swap(swap)).ToArray();
                    case TypeCode.Double: return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes((double)io).Swap(swap)).ToArray();
                    case TypeCode.Decimal: return (io == null) ? nio : nio.Concat(Decimal.GetBits((decimal)io).SelectMany(z => BinaryEx.GetBytes(z).Swap(swap)).ToArray()).ToArray();
                    case TypeCode.String: return ExportStr((string)io, swap);
                    case TypeCode.DateTime:
                        switch (typ) {
                            case TypeCodeEx.NullableDateTimeOffset:
                            case TypeCodeEx.DateTimeOffset: {
                                DateTimeOffset? dt = IONull<DateTimeOffset>.Value(io);
                                return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes(dt.Value.Ticks).Swap(swap)).Concat(BinaryEx.GetBytes(dt.Value.Offset.Ticks).Swap(swap)).ToArray();
                            }
                            case TypeCodeEx.NullableDateTime2:
                            case TypeCodeEx.DateTime2: {
                                DateTime2? dt = IONull<DateTime2>.Value(io);
                                return (io == null) ? nio : nio.Concat(ExportDateTime2(dt.Value.Date, 0, swap)).ToArray();
                            }
                            case TypeCodeEx.NullableTimeSpan:
                            case TypeCodeEx.TimeSpan: {
                                TimeSpan? ts = IONull<TimeSpan>.Value(io);
                                return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes(ts.Value.Ticks).Swap(swap)).ToArray();
                            }
                            case TypeCodeEx.NullableDateTime:
                            default: {
                                DateTime? dt = IONull<DateTime>.Value(io);
                                return (io == null) ? nio : nio.Concat(BinaryEx.GetBytes(dt.Value.Ticks).Swap(swap).Concat(((byte)(int)dt.Value.Kind).AsArray())).ToArray();
                            }
                        }
                    case TypeCode.Object:
                        switch (typ) {
                            case TypeCodeEx.ByteArray: if (io == null) break; byte[] barr = (byte[])io; return BinaryEx.GetBytes(barr.Length).Swap(swap).Concat(barr).ToArray();
                            case TypeCodeEx.ShortArray: if (io == null) break; short[] isarr = (short[])io; return BinaryEx.GetBytes(isarr.Length).Swap(swap).Concat(isarr.SelectMany(z => BinaryEx.GetBytes(z).Swap(swap))).ToArray();
                            case TypeCodeEx.UInt16Array: if (io == null) break; ushort[] usarr = (ushort[])io; return BinaryEx.GetBytes(usarr.Length).Swap(swap).Concat(usarr.SelectMany(z => BinaryEx.GetBytes(z).Swap(swap))).ToArray();
                            case TypeCodeEx.IntArray: if (io == null) break; int[] iarr = (int[])io; return BinaryEx.GetBytes(iarr.Length).Swap(swap).Concat(iarr.SelectMany(z => BinaryEx.GetBytes(z).Swap(swap))).ToArray();
                            case TypeCodeEx.UInt32Array: if (io == null) break; uint[] uiarr = (uint[])io; return BinaryEx.GetBytes(uiarr.Length).Swap(swap).Concat(uiarr.SelectMany(z => BinaryEx.GetBytes(z).Swap(swap))).ToArray();
                            case TypeCodeEx.UInt64Array: if (io == null) break; ulong[] ularr = (ulong[])io; return BinaryEx.GetBytes(ularr.Length).Swap(swap).Concat(ularr.SelectMany(z => BinaryEx.GetBytes(z).Swap(swap))).ToArray();
                            case TypeCodeEx.Int64Array: if (io == null) break; long[] larr = (long[])io; return BinaryEx.GetBytes(larr.Length).Swap(swap).Concat(larr.SelectMany(z => BinaryEx.GetBytes(z).Swap(swap))).ToArray();
                            case TypeCodeEx.DoubleArray: if (io == null) break; double[] darr = (double[])io; return BinaryEx.GetBytes(darr.Length).Swap(swap).Concat(darr.SelectMany(z => BinaryEx.GetBytes(z).Swap(swap))).ToArray();
                            case TypeCodeEx.DecimalArray: if (io == null) break; decimal[] marr = (decimal[])io; return BinaryEx.GetBytes(marr.Length).Swap(swap).Concat(marr.SelectMany(z => decimal.GetBits(z).SelectMany(x => BinaryEx.GetBytes(x).Swap(swap)).ToArray())).ToArray();
                            case TypeCodeEx.StringArray: if (io == null) break; string[] sarr = (string[])io; return BinaryEx.GetBytes(sarr.Length).Swap(swap).Concat(sarr.SelectMany(z => ExportOne(z.SafeIO(), swap))).ToArray();
                            case TypeCodeEx.StringArrays: if (io == null) break; IEnumerable<string[]> ssarr = (IEnumerable<string[]>)io; return BinaryEx.GetBytes(ssarr.Count()).Swap(swap).Concat(ssarr.SelectMany(z => ExportOne(z.SafeIO(), swap))).ToArray();
                            case TypeCodeEx.ByteArrays: if (io == null) break; IEnumerable<byte[]> bbarr = (IEnumerable<byte[]>)io; return BinaryEx.GetBytes(bbarr.Count()).Swap(swap).Concat(bbarr.SelectMany(z => ExportOne(z.SafeIO(), swap))).ToArray();
                            case TypeCodeEx.IOBinary: return ExportOne((io as IOBinary).IO ?? NullBytes, swap);
                            case TypeCodeEx.Guid: return (io == null) ? nio : nio.Concat(((Guid)io).ToByteArray()).ToArray();
                            case TypeCodeEx.GuidArray: if (io == null) break; Guid[] garr = (Guid[])io; return BinaryEx.GetBytes(garr.Length).Swap(swap).Concat(garr.SelectMany(z => z.ToByteArray())).ToArray();
                            case TypeCodeEx.DateTimeArray: if (io == null) break; DateTime[] dtarr = (DateTime[])io; return BinaryEx.GetBytes(dtarr.Length).Swap(swap).Concat(dtarr.SelectMany(z => ExportOne(z, swap))).ToArray();
                            default: return null;
                        }
                        return BinaryEx.GetBytes(int.MaxValue).Swap(swap);
                    default: return null;
                }
            } catch (Exception e) { throw new IOExException($"Failed to export type {io?.GetType().Name ?? "null"}: {e.Message}"); }
        }

        public static byte[] ExportStr(string s, bool swap = false) {
            byte[] buf = ((s?.Length > 0) ? Encoding.UTF8.GetBytes(s) : null) ?? Empty;
            return ExportOne((s != null) ? buf.Length : int.MaxValue, swap).Concat(buf).ToArray();
        }
        public static byte[] ExportStrTiny(string s) {
            byte[] buf = ((s?.Length > 0) ? Encoding.UTF8.GetBytes((s.Length < byte.MaxValue) ? s : s.Substring(0, byte.MaxValue - 1)) : null) ?? Empty;
            return ExportOne((byte)((s != null) ? buf.Length : byte.MaxValue)).Concat(buf).ToArray();
        }

        public static byte[] ExportDateTime2(DateTime dt, byte precision, bool swap = false) { // cast(datetime2(precision) as binary(7-9))
            if (dt >= DateTime2IOMax) dt = DateTime2IOMax;
            if (dt <= DateTime2IOMin) dt = DateTime2IOMin;
            ulong t = (ulong)Math.Max(0, Math.Min(ulong.MaxValue, (dt - dt.Date).TotalSeconds * Math.Pow(10, (double)precision))); // fractional seconds since midnight
            uint d = (uint)Math.Max(0, Math.Min(uint.MaxValue, (dt.AsUTC() - DateTime2IOMin.Date).TotalDays));
            int p;
            switch (precision) {
                case 0x00:
                case 0x01:
                case 0x02: p = 3; break;
                case 0x03:
                case 0x04: p = 4; break;
                case 0x05:
                case 0x06:
                case 0x07: p = 5; break;
                default: throw new IOExException("Invalid date precision");
            }
            byte[] ticks = BinaryEx.GetBytes(t).Take(p).ToArray();
            byte[] days = BinaryEx.GetBytes(d).Take(3).ToArray();
            if (swap) {
                ticks = ticks.Reverse().ToArray();
                days = days.Reverse().ToArray();
            }
            return precision.AsArray().Concat(ticks).Concat(days).ToArray();
        }
        public static byte[] ExportDateTime2(DateTimeOffset dt, bool swap = false) { return ExportDateTime2(dt.UtcDateTime, swap); } // cast(datetime2(precision) as binary(7-9)) with swap = true

        public static byte[] ExportNullable(bool? isnull) { return (isnull == true) ? ((byte)0x00).AsArray() : (isnull == false) ? ((byte)0x01).AsArray() : Empty; }

        public static object SafeIO(this string obj) { return obj ?? NullString; }
        public static object SafeIO(this string[] obj, bool nullifempty = false) { return obj?.DefaultIfEmpty((nullifempty) ? null : obj) ?? NullStringArray; }
        public static object SafeIO(this byte[] obj, bool nullifempty = false) { return obj?.DefaultIfEmpty((nullifempty) ? null : obj) ?? NullBytes; }
        public static IONull<T> SafeIO<T>(this T? obj) where T : struct { return new IONull<T>(obj); }

        public static byte[] ToBlobMap<SQLType>(this SQLType[] t) where SQLType : struct, IConvertible, IComparable, IFormattable { return t?.Select(z => z.ToByte()).ToArray(); }
        /// <summary>
        /// Check buffer to determine if the block starting at the specified index has a valid binary string length prefix
        /// </summary>
        /// <param name="buf">Block of serialized bytes</param>
        /// <param name="ix">Starting index where binary serialized string is expected</param>
        /// <param name="swap">String length is specified as a bigendian value</param>
        /// <returns>True if the bytes at the specified index would successfully deserialize to a string</returns>
        public static bool TestString(byte[] buf, int ix, bool swap = false) {
            int sz = GetInt32(buf, ref ix, swap);
            if (sz == int.MaxValue) return true;
            if (ix + sz > buf.Length) return false;
            return true;
        }
        #endregion Export


    }
}
