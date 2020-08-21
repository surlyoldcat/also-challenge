using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;

namespace AE.CoreUtility
{
    [DebuggerStepThrough]
    public static class NumberEx
    {
        static NumberEx() {
            CurrencyFormat.CurrencySymbol = "";
            CanadaFormat.CurrencySymbol = "C$";
            YenFormat.CurrencySymbol = "¥";
            BahtFormat.CurrencySymbol = "฿";
        }

        public const string c_serverculture = "en";
        public readonly static int[] Empty = new int[0];

        public readonly static NumberFormatInfo ServerFormat = new CultureInfo(c_serverculture).NumberFormat;
        public readonly static NumberFormatInfo CurrencyFormat = ServerFormat.Clone() as NumberFormatInfo;
        public readonly static NumberFormatInfo CanadaFormat = (new CultureInfo("en-CA")).NumberFormat.Clone() as NumberFormatInfo;
        public readonly static NumberFormatInfo YenFormat = (new CultureInfo("ja-JP")).NumberFormat.Clone() as NumberFormatInfo;
        public readonly static NumberFormatInfo BahtFormat = (new CultureInfo("th-TH")).NumberFormat.Clone() as NumberFormatInfo;

        public static bool TryFromServer(this string s, out double ret, double def) { if (s != null && double.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true; ret = def; return false; }
        public static bool TryFromServer(this string s, string fmt, out double ret, double def) {
            ret = def;
            if (s == null) return false;
            long hex;
            if (!String.IsNullOrEmpty(fmt) && (fmt[0] == 'x' || fmt[0] == 'X' || fmt.IndexOf(":X", StringComparison.OrdinalIgnoreCase) != -1)) {
                if (long.TryParse(s, NumberStyles.HexNumber, ServerFormat, out hex)) {
                    ret = hex;
                    return true;
                }
            }
            if (double.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true;
            if (s.Length > 0 && (s[0] == 'x' || s[0] == 'X')) s = s.Substring(1);
            else if (s.Length > 1 && (s[1] == 'x' || s[1] == 'X')) s = s.Substring(2);
            else return false;
            if (!long.TryParse(s, NumberStyles.HexNumber, ServerFormat, out hex)) return false;
            ret = hex;
            return true;
        }

        public static bool TryFromServer(this string s, out float ret, float def) { if (s != null && float.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true; ret = def; return false; }

        /// <summary>
        /// Trim the 0x, x, 0X, or X from the beginning of the string if needed so the hex parse to a number will work
        /// </summary>
        /// <param name="s"></param>
        public static void HexParsePrep(ref string s) { s = StringEx.UserText(s); if (s != null && s.Length >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) s = StringEx.UserText(s.Substring(2)); }
        public static bool TryFromServer(this string s, out int ret, int def) {
            int ix = (s != null) ? s.IndexOfAny(new char[] { 'x', 'X' }) : -1;
            if (ix == 0 || (ix == 1 && s[0] == '0')) return TryFromServerHex(s.Substring(ix + 1), out ret, def);
            if (s != null && int.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true;
            ret = def;
            return false;
        }
        public static bool TryFromServerHex(this string s, out int ret, int def) { HexParsePrep(ref s); if (s != null && int.TryParse(s, NumberStyles.AllowHexSpecifier | NumberStyles.HexNumber, ServerFormat, out ret)) return true; ret = def; return false; }
        public static string ToServer(this int i) { return i.ToString(ServerFormat); }

        public static bool TryFromServer(this string s, out uint ret, uint def) { if (s != null && uint.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true; ret = def; return false; }

        public static bool TryFromServer(this string s, out decimal ret, decimal def) { if (s != null && Decimal.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true; ret = def; return false; }

        public static bool TryFromServer(this string s, out long ret, long def) {
            int ix = (s != null) ? s.IndexOfAny(new char[] { 'x', 'X' }) : -1;
            if (ix != -1) return TryFromServerHex(s.Substring(ix + 1), out ret, def);
            if (s != null && long.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true;
            ret = def;
            return false;
        }
        public static bool TryFromServerHex(this string s, out long ret, long def) { HexParsePrep(ref s); if (s != null && long.TryParse(s, NumberStyles.AllowHexSpecifier | NumberStyles.HexNumber, ServerFormat, out ret)) return true; ret = def; return false; }

        public static bool TryFromServer(this string s, out ulong ret, ulong def) { if (s != null && ulong.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true; ret = def; return false; }

        public static string ToQuery(this int i) { return i.ToString(ServerFormat); }

        public static bool TryFromServer(this string s, out short ret, short def) { if (s != null && short.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true; ret = def; return false; }

        public static bool TryFromServer(this string s, out ushort ret, ushort def) { if (s != null && ushort.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true; ret = def; return false; }

        public static bool TryFromServer(this string s, out byte ret, byte def) { if (s != null && byte.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true; ret = def; return false; }
        public static string ToServerHex(this byte i, int len = 0, string prefix = "0x") { return String.Concat(prefix, i.ToString(String.Concat("x", (len > 0) ? len.ToServer() : null))); }

        public static bool TryFromServer(this string s, out sbyte ret, sbyte def) { if (s != null && sbyte.TryParse(s, NumberStyles.Any, ServerFormat, out ret)) return true; ret = def; return false; }
        public static string ToServerHex(this byte[] b, string prefix = "0x") { return b == null ? null : b.Length == 0 ? string.Empty : string.Concat(prefix, string.Join(string.Empty, b.Select(z => z.ToServerHex(sizeof(byte) * 2, null)) ?? StringEx.Empty)); }
        public static bool TryFromServerHex(this string s, out byte[] ret) {
            try {
                int len = s?.Length ?? 0;
                int i = 0;
                if (len >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) i = 2;
                if ((len - i) <= 2) {
                    ret = i > 0 ? IOEx.Empty : null;
                    return i > 0;
                }
                if ((len - i) % 2 != 0) {
                    ret = null;
                    return false;
                }
                ret = new byte[(len - i) / 2];
                int ix = 0;
                for (; i < len; i += 2)
                    ret[ix++] = byte.Parse(s.Substring(i, 2), NumberStyles.HexNumber, ServerFormat);
                return true;
            } catch {
                ret = null;
                return false;
            }
        }

        public static bool TryFromServer(this string s, out bool ret, bool def) {
            if (s != null && bool.TryParse(s, out ret)) return true;
            int n;
            if (TryFromServer(s, out n, 0)) {
                ret = n != 0;
                return true;
            }
            if (String.Equals(s, Boolean.TrueString, StringComparison.OrdinalIgnoreCase)) ret = true;
            else if (String.Equals(s, Boolean.FalseString, StringComparison.OrdinalIgnoreCase)) ret = false;
            else {
                ret = def;
                return false;
            }
            return true;
        }

        public static string ToQuery(this IEnumerable<int> ids) { return ToServer(ids); }
        public static string ToServer(this IEnumerable<int> ids) { return ToServer(ids, StringEx.c_sep); }
        public static string ToServer(this IEnumerable<int> ids, char sep) {
            StringBuilder ret = null;
            foreach (int i in ids) {
                if (ret == null) ret = new StringBuilder();
                ret.Append(i.ToQuery());
                ret.Append(sep);
            }
            if (ret == null) return String.Empty;
            ret.Length -= 1;
            return ret.ToString();
        }

        public static int[] TryToInts(this string ids, char sep = StringEx.c_sep) { return TryToInts(StringEx.UserText(ids)?.Split(sep)?.Select(z => z?.Trim())); }
        public static int[] TryToInts(this IEnumerable<string> ids) {
            if (ids == null) return null;
            int i = 0;
            int[] ret = new int[ids.Count()];
            foreach (string id in ids) {
                int n;
                if (String.IsNullOrWhiteSpace(id) || !TryFromServer(id.Trim(), out n, 0)) return null;
                ret[i++] = n;
            }
            return ret;
        }

        public static bool Numeric<T>(object val, out T ret, bool errdef = false, bool parse = false) {
            bool ok = Numeric(ref val, typeof(T), errdef, parse) && val is T;
            ret = ok ? (T)val : default(T);
            return ok;
        }
        public static bool Numeric(ref object val, Type to, bool errdef = true, bool parse = false) {
            object v, def;
            bool nullable = to?.IsNullable() == true;
            switch (Type.GetTypeCode(to?.NonNullableType())) {
                case TypeCode.Boolean: v = GetBool(val, parse); def = nullable ? default(bool?) : default(bool); break;
                case TypeCode.Char: v = (char?)GetShort(val, parse); def = nullable ? default(char?) : default(char); break;
                case TypeCode.SByte: v = GetSByte(val, parse); def = nullable ? default(sbyte?) : default(sbyte); break;
                case TypeCode.Byte: v = GetByte(val, parse); def = nullable ? default(byte?) : default(byte); break;
                case TypeCode.Int16: v = GetShort(val, parse); def = nullable ? default(short?) : default(short); break;
                case TypeCode.UInt16: v = GetUShort(val, parse); def = nullable ? default(ushort?) : default(ushort); break;
                case TypeCode.Int32: v = GetInt(val, parse); def = nullable ? default(int?) : default(int); break;
                case TypeCode.UInt32: v = GetUInt(val, parse); def = nullable ? default(uint?) : default(uint); break;
                case TypeCode.Int64: v = GetLong(val, parse); def = nullable ? default(long?) : default(long); break;
                case TypeCode.UInt64: v = GetULong(val, parse); def = nullable ? default(ulong?) : default(ulong); break;
                case TypeCode.Single: v = GetFloat(val, parse); def = nullable ? default(float?) : default(float); break;
                case TypeCode.Double: v = GetDouble(val, parse); def = nullable ? default(double?) : default(double); break;
                case TypeCode.Decimal: v = GetDecimal(val, parse); def = nullable ? default(decimal?) : default(decimal); break;
                default: return false;
            }
            if (v == null && !errdef) return false;
            val = v ?? def;
            return v != null;
        }
        public static bool IsNumeric(TypeCode tc) { return NumericSize(tc) > 0; }

        public static int NumericSize(TypeCode tc) {
            switch (tc) {
                case TypeCode.Boolean:
                case TypeCode.Char:
                case TypeCode.SByte:
                case TypeCode.Byte: return sizeof(byte);
                case TypeCode.Int16:
                case TypeCode.UInt16: return sizeof(short);
                case TypeCode.Int32:
                case TypeCode.UInt32: return sizeof(int);
                case TypeCode.Int64:
                case TypeCode.UInt64: return sizeof(long);
                case TypeCode.Single: return sizeof(float);
                case TypeCode.Double: return sizeof(double);
                case TypeCode.Decimal: return sizeof(decimal);
                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                case TypeCode.String:
                default: return 0;
            }
        }

        public static double? Get(object val) { return Get(val, true); }
        public static double? Get(object val, bool parse) { return Get(val, parse, null); }
        public static double? Get(object val, bool parse, string fmt) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.Single: return FloatToDouble(Convert.ToSingle(val));
                    case TypeCode.Double: return Convert.ToDouble(val);
                    case TypeCode.String:
                        if (!parse) return null;
                        double n;
                        if (!TryFromServer(Convert.ToString(val), fmt, out n, 0)) return null;
                        return n;
                    default:
                        if (IsNumeric(tc)) return Convert.ToDouble(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(double)) return IOEx.GetDouble(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static float? GetFloat(object val) { return GetFloat(val, true); }
        public static float? GetFloat(object val, bool parse) { return GetFloat(val, parse, null); }
        public static float? GetFloat(object val, bool parse, string fmt) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.Single: return Convert.ToSingle(val);
                    case TypeCode.Double: return DoubleToFloat(Convert.ToDouble(val));
                    case TypeCode.String:
                        if (!parse) return null;
                        float n;
                        if (!TryFromServer(Convert.ToString(val), out n, 0)) return null;
                        return n;
                    default:
                        if (IsNumeric(tc)) return Convert.ToSingle(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(float)) return IOEx.GetSingle(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static double? GetDouble(object val, bool parse) { return GetDouble(val, parse, null); }
        public static double? GetDouble(object val, bool parse, string fmt) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.Double: return Convert.ToDouble(val);
                    case TypeCode.String:
                        if (!parse) return null;
                        double n;
                        if (!TryFromServer(Convert.ToString(val), out n, 0)) return null;
                        return n;
                    default:
                        if (IsNumeric(tc)) return Convert.ToDouble(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(double)) return IOEx.GetDouble(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static decimal? GetDecimal(object val) { return GetDecimal(val, true); }
        public static decimal? GetDecimal(object val, bool parse) { return GetDecimal(val, parse, null); }
        public static decimal? GetDecimal(object val, bool parse, string fmt) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.Decimal: return Convert.ToDecimal(val);
                    case TypeCode.String:
                        if (!parse) return null;
                        decimal n;
                        if (!TryFromServer(Convert.ToString(val), out n, decimal.Zero)) return null;
                        return n;
                    default:
                        if (IsNumeric(tc)) return Convert.ToDecimal(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(decimal)) return IOEx.GetDecimal(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static long? GetLong(object val) { return GetLong(val, true); }
        public static long? GetLong(object val, bool parse) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.String:
                        if (!parse) return null;
                        long n;
                        if (!TryFromServer(Convert.ToString(val), out n, 0)) return null;
                        return n;
                    case TypeCode.UInt64: return unchecked((long)(ulong)val);
                    default:
                        if (IsNumeric(tc)) return Convert.ToInt64(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(long)) return IOEx.GetInt64(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static ulong? GetULong(object val) { return GetULong(val, true); }
        public static ulong? GetULong(object val, bool parse) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.String:
                        if (!parse) return null;
                        ulong n;
                        if (!TryFromServer(Convert.ToString(val), out n, 0)) return null;
                        return n;
                    case TypeCode.Int64: return unchecked((ulong)(long)val);
                    default:
                        if (IsNumeric(tc)) return Convert.ToUInt64(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(ulong)) return IOEx.GetUInt64(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static bool? GetBool(object val) { return GetBool(val, true); }
        public static bool? GetBool(object val, bool parse) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.String:
                        if (!parse) return null;
                        bool n;
                        string s = StringEx.UserText(val as string);
                        if (s == null) return null;
                        if (TryFromServer(s, out n, false)) return n;
                        switch (s[0]) {
                            case 'T': case 't': case '1': return true;
                            case 'F': case 'f': case '0': return false;
                            default: return null;
                        }
                    default:
                        if (IsNumeric(tc)) return Convert.ToBoolean(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(bool)) return IOEx.GetBool(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static byte? GetByte(object val) { return GetByte(val, true); }
        public static byte? GetByte(object val, bool parse) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.String:
                        if (!parse) return null;
                        byte n;
                        if (!TryFromServer(Convert.ToString(val), out n, 0)) return null;
                        return n;
                    case TypeCode.SByte: return unchecked((byte)(sbyte)val);
                    default:
                        if (IsNumeric(tc)) return Convert.ToByte(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(byte)) return IOEx.GetByte(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static sbyte? GetSByte(object val) { return GetSByte(val, true); }
        public static sbyte? GetSByte(object val, bool parse) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.String:
                        if (!parse) return null;
                        sbyte n;
                        if (!TryFromServer(Convert.ToString(val), out n, 0)) return null;
                        return n;
                    case TypeCode.Byte: return unchecked((sbyte)(byte)val);
                    default:
                        if (IsNumeric(tc)) return Convert.ToSByte(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(byte)) return IOEx.GetSByte(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static short? GetShort(object val) { return GetShort(val, true); }
        public static short? GetShort(object val, bool parse) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.String:
                        if (!parse) return null;
                        short n;
                        if (!TryFromServer(Convert.ToString(val), out n, 0)) return null;
                        return n;
                    case TypeCode.UInt16: return unchecked((short)(ushort)val);
                    default:
                        if (IsNumeric(tc)) return Convert.ToInt16(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(short)) return IOEx.GetInt16(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static ushort? GetUShort(object val) { return GetUShort(val, true); }
        public static ushort? GetUShort(object val, bool parse) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.String:
                        if (!parse) return null;
                        ushort n;
                        if (!TryFromServer(Convert.ToString(val), out n, 0)) return null;
                        return n;
                    case TypeCode.Int16: return unchecked((ushort)(short)val);
                    default:
                        if (IsNumeric(tc)) return Convert.ToUInt16(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(ushort)) return IOEx.GetUInt16(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static int? GetInt(object val) { return GetInt(val, true); }
        public static int? GetInt(object val, bool parse) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.String:
                        if (!parse) return null;
                        int n;
                        if (!TryFromServer(Convert.ToString(val), out n, 0)) return null;
                        return n;
                    case TypeCode.UInt32: return unchecked((int)(uint)val);
                    default:
                        if (IsNumeric(tc)) return Convert.ToInt32(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(int)) return IOEx.GetInt32(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static uint? GetUInt(object val) { return GetUInt(val, true); }
        public static uint? GetUInt(object val, bool parse) {
            if (val == null) return null;
            try {
                TypeCode tc = val.GetTypeCode();
                switch (tc) {
                    case TypeCode.String:
                        if (!parse) return null;
                        uint n;
                        if (!TryFromServer(Convert.ToString(val), out n, 0)) return null;
                        return n;
                    case TypeCode.Int32: return unchecked((uint)(int)val);
                    default:
                        if (IsNumeric(tc)) return Convert.ToUInt32(val);
                        byte[] b = val as byte[];
                        int ix = 0;
                        if (b?.Length == sizeof(uint)) return IOEx.GetUInt32(b, ref ix);
                        return null;
                }
            } catch { return null; }
        }

        public static bool IsValid(this double? n, bool zerook = true) { return n?.IsValid(zerook) == true; }
        public static bool IsValid(this float? n, bool zerook = true) { return n?.IsValid(zerook) == true; }
        public static bool IsValid(this double n, bool zerook = true) { return n > double.MinValue && n < double.MaxValue && !double.IsNaN(n) && !double.IsInfinity(n) && (zerook || n != 0); }
        public static bool IsValid(this float n, bool zerook = true) { return n > float.MinValue && n < float.MaxValue && !float.IsNaN(n) && !float.IsInfinity(n) && (zerook || n != 0); }

        public static ulong[] Or(this ulong[] to, ulong[] from, bool dupe = true, int max = 0) {
            int fromlen = from?.Length ?? 0;
            if (max > 0 && fromlen > max) throw new ArgumentException("Array data would be truncated");
            if (to == null) {
                if (from == null) return null;
                to = new ulong[fromlen];
                if (fromlen > 0) Array.Copy(from, to, fromlen);
                return to;
            }
            if (fromlen <= 0) return to;
            int tolen = to.Length;
            if (dupe || tolen < fromlen) {
                ulong[] copy = new ulong[Math.Max(fromlen, tolen)];
                Array.Copy(to, copy, tolen);
                if (tolen < fromlen) Array.Copy(from, tolen, copy, tolen, fromlen - tolen);
                to = copy;
            }
            for (int i = 0; i < Math.Min(fromlen, tolen); i++)
                to[i] |= from[i];
            return to;
        }

        public static ulong[] And(this ulong[] to, ulong[] from, bool dupe = true, int max = 0) {
            int fromlen = from?.Length ?? 0;
            if (max > 0 && fromlen > max) throw new ArgumentException("Array data would be truncated");
            if (to == null) {
                if (from == null) return null;
                to = new ulong[fromlen];
                if (fromlen > 0) Array.Copy(from, to, fromlen);
                return to;
            }
            int tolen = to.Length;
            if (fromlen <= 0) return to = new ulong[tolen];
            if (dupe || tolen < fromlen) {
                ulong[] copy = new ulong[Math.Max(fromlen, tolen)];
                Array.Copy(to, copy, tolen);
                to = copy;
            }
            for (int i = 0; i < Math.Max(fromlen, tolen); i++)
                to[i] &= (i < fromlen) ? from[i] : 0;
            return to;
        }

        public static ulong[] Complement(this ulong[] b, bool dupe = true) {
            int len = b?.Length ?? 0;
            if (dupe) {
                ulong[] copy = new ulong[len];
                Array.Copy(b, copy, len);
                b = copy;
            }
            if (len <= 0) return b;
            for (int i = 0; i < len; i++)
                b[i] = ~b[i];
            return b;
        }

        /// <summary>
        /// Convert float to double without altering the number.  Accomodates float.NaN and other values that fail when simply casting float to decimal to double.
        /// https://msdn.microsoft.com/en-us/library/system.double(v=vs.110).aspx#Precision
        /// http://steve.hollasch.net/cgindex/coding/ieeefloat.html
        /// https://msdn.microsoft.com/en-us/library/0b34tf65.aspx
        /// The values are stored as follows:
        /// Value Stored as
        /// real*4	sign bit, 8-bit exponent, 23-bit mantissa
        /// real*8	sign bit, 11-bit exponent, 52-bit mantissa
        /// In real*4 and real*8 formats, there is an assumed leading 1 in the mantissa that is not stored in memory, so the mantissas are actually 24 or 53 bits, even though only 23 or 52 bits are stored.
        /// The exponents are biased by half of their possible value. This means you subtract this bias from the stored exponent to get the actual exponent. If the stored exponent is less than the bias, it is actually a negative exponent.
        /// The exponents are biased as follows:
        /// Exponent Biased by
        /// 8-bit (real*4)	127
        /// 11-bit (real*8)	1023
        /// The mantissa is stored as a binary fraction of the form 1.XXX… . This fraction has a value greater than or equal to 1 and less than 2. 
        /// Note that real numbers are always stored in normalized form; that is, the mantissa is left-shifted such that the high-order bit of the mantissa is always 1. 
        /// Because this bit is always 1, it is assumed (not stored) in the real*4 and real*8 formats. The binary (not decimal) point is assumed to be just to the right of the leading 1.
        /// 
        /// Case	Sign	Exponent (e)	Fraction (f)	Value
        /// A:		0		00⋯00			00⋯00			+0
        /// B:		0		00⋯00			00⋯01 - 11⋯11	Positive Denormalized Real	0.f × 2(−b+1)
        ///	C:		0		00⋯01 - 11⋯10	XX⋯XX			Positive Normalized Real	1.f × 2(e−b)
        /// D:		0		11⋯11			00⋯00			+∞
        /// E:		0		11⋯11			00⋯01 - 01⋯11	SNaN
        /// F:		0		11⋯11			1X⋯XX			QNaN
        /// G:		1		00⋯00			00⋯00			−0
        /// H:		1		00⋯00			00⋯01 - 11⋯11	Negative Denormalized Real	−0.f × 2(−b+1)
        ///	I:		1		00⋯01 - 11⋯10	XX⋯XX			Negative Normalized Real	−1.f × 2(e−b)
        /// J:		1		11⋯11			00⋯00			−∞
        /// K:		1		11⋯11			00⋯01 - 01⋯11	SNaN
        /// L:		1		11⋯11			1X⋯XX			QNaN
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public static double FloatToDouble(this float f) {
            try {
                return float.IsNaN(f) ? double.NaN : (double)(decimal)f;
                //uint bits = BinaryEx.ToUInt32(BinaryEx.GetBytes(f), 0);
                //bool neg = unchecked((bits >> 31) != 0U);
                //int mant = unchecked((int)(bits & 0x007FFFFFU));
                //int exp = unchecked((int)((bits >> 23) & 0xFFU));
                //if (exp == 0xFF) {
                //	if (mant == 0) return (neg) ? double.NegativeInfinity : double.PositiveInfinity; // D and J
                //	return f; // E, F, K, and L - double.NaN but preserve the SNaN or QNaN details
                //} else if (exp == 0) {
                //	if (mant == 0) return (neg) ? -0D : 0D; // A and G
                //	//exp = -127 + 1; // B and H
                //} else { // C and I
                //	//exp -= 127;
                //	//mant |= 1 << 24;
                //}
                //return (double)(decimal)f;
            } catch {
                return f;
            }
        }

        public static float DoubleToFloat(this double d) { return double.IsNaN(d) ? float.NaN : (float)d; }

        public const long c_maxjslong = 0x001FFFFFFFFFFFFF;
    }
}
