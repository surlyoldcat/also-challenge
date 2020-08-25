using AE.CoreInterface;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace AE.CoreUtility
{
    [DebuggerStepThrough]
    public static class StringEx
    {
        #region Bootstrap
        /// <summary>
        /// Internal singleton class to allow SQL Server compilation to run in SAFE permission level
        /// </summary>
        private class _StringExSingleton
        {
            internal GetLocalizedDelegate GetLocalized = null;
        }
#if !SQLCLRDLL
        private static _StringExSingleton m_singleton { get { return _singleton ?? (_singleton = new _StringExSingleton()); } }
        private static _StringExSingleton _singleton = null;
#else // !SQLCLRDLL
		private readonly static _StringExSingleton m_singleton = null;
#endif // !SQLCLRDLL

        #endregion Bootstrap

        public delegate string GetLocalizedDelegate(string s, string hint = null);
        public static GetLocalizedDelegate GetLocalized { get { return m_singleton?.GetLocalized; } set { if (m_singleton != null) m_singleton.GetLocalized = value; } }
        public static string Localize(string s, string hint = null) { return !string.IsNullOrWhiteSpace(s) && GetLocalized != null ? GetLocalized(s, hint) : s; }

        public const char c_tokenvar = '*';
        public const string Null = null;
        public const string CommaNewLine = ", \r\n";
        public const string NewLine = "\r\n";
        public const string CommaSep = ", ";
        public const string PathVert = "\r\n- ";
        public const char c_sep = ',';
        public readonly static char[] ParamSeps = new char[] { ',', '|', ';' };
        public readonly static char[] FilterSeps = new char[] { ',', '|', ';', ' ' };
        public readonly static char[] CRLF = new char[] { '\r', '\n' };
        public readonly static string[] CRLFSplit = new string[] { "\r\n", "\r", "\n" };
        public readonly static string[] WhiteSpaceSplit = new string[] { "\r\n", "\r", "\n", " ", "\t" };
        public readonly static char[] WhiteSpace = new char[] { '\r', '\n', ' ', '\t' };
        public readonly static char[] c_seps = new char[] { '\\', '/' };
        public readonly static string[] Empty = new string[0];

        public static string ToQueryNoQuotes(this string s) { return (s != null) ? s : "null"; }

        public static string ToQueryNoQuotes(this IEnumerable<string> strs) {
            StringBuilder ret = null;
            foreach (string s in strs) {
                if (String.IsNullOrEmpty(s)) continue;
                if (ret == null) ret = new StringBuilder();
                ret.Append(s.ToQueryNoQuotes());
                ret.Append(c_sep);
            }
            if (ret == null) return String.Empty;
            ret.Length -= 1;
            return ret.ToString();
        }
        public static Guid ToGuid(object val, Guid def = default(Guid)) { Guid ret; return ToGuid(val, out ret, def) ? ret : def; }
        public static bool ToGuid(object val, out Guid ret, Guid def = default(Guid)) {
            if (val is Guid) {
                ret = (Guid)val;
                return true;
            }
            Version v = val as Version;
            if (v != null) {
                ret = v.ToGuid();
                return true;
            }
            byte[] b = val as byte[];
            if (b?.Length == 16) {
                ret = new Guid((byte[])val);
                return true;
            }
            if (b?.Length > 0 && Guid.TryParse(Encoding.UTF8.GetString(b, 0, b.Length), out ret)) return true;
            if (val is string && Guid.TryParse((string)val, out ret)) return true;
            ret = def;
            return false;
        }

        public const int c_namecodelength = 5;
        public const int c_maxextlen = 8;
        public const int c_maxSMSlength = 1000; // txt messages are limited to 160 characters, and are then split into multiple texts, fail if trying to send more than 7 parts
        public const int c_minPhoneLength = 8;
        public const string c_smsEmail = "{number}";


        public readonly static Regex MultiSpaces = new Regex(" {2,}");
        public readonly static Regex MultiWhitespace = new Regex("\\s+");
        public static string[] SplitParams(this string s, bool excludews) {
            if (excludews) s = UserText(s);
            if (s == null) return null;
            if (s.Length <= 0) return s.AsArray();
            string[] ret = s.Split(ParamSeps, (excludews) ? StringSplitOptions.RemoveEmptyEntries : StringSplitOptions.None);
            return (excludews) ? ret.Select(z => UserText(z)).NotNull().ToArray() : ret;
        }
        public static string[] Split(this string s, string delim, bool unquote) {
            if (s == null) return null;
            if (String.IsNullOrEmpty(delim)) return s.AsArray();
            List<string> ret = new List<string>();
            int ix = 0;
            int count = s.Length;
            while (ix < count) {
                string f = null;
                int next = -1;
                int pre = 0;
                if (unquote) {
                    for (int n = ix; n < count; n++) {
                        switch (s[n]) {
                            case ' ': pre++; continue;
                            case '\'': f = "\'"; break;
                            case '\"': f = "\""; break;
                        }
                        break;
                    }
                }
                if (f != null) {
                    next = ix + pre + 1;
                    while (next < count) {
                        next = s.IndexOf(f[0], next); // find end quote
                        if (next == -1) break;
                        if (next + 1 >= count) {
                            next++;
                            break;
                        }
                        if (s[next + 1] != f[0]) break;
                        next += 2; // found ""
                    }
                    if (next >= 0 && next < count) {
                        int nextdelim = s.IndexOf(delim, next); // find delim after end quote
                        while (next < count) {
                            char c = s[next++];
                            if (Char.IsWhiteSpace(c)) continue;
                            if (next == nextdelim) break;
                            next = -1;
                            break;
                        }
                    }
                    if (next == -1) f = null; // not found, treat as unquoted
                }
                if (f == null) next = s.IndexOf(delim, ix);
                if (next == -1) next = count;
                string add = s.Substring(ix, next - ix);
                if (f != null) {
                    string trim = add.Trim();
                    if (trim.Length < 2 || !trim.StartsWith(f) || !trim.EndsWith(f)) {
                        f = null;
                        next = s.IndexOf(delim, ix);
                        if (next == -1) next = count;
                        add = s.Substring(ix, next - ix);
                    }
                    add = trim.Substring(1, trim.Length - 2);
                    if (unquote) add = add.Replace(f + f, f);
                }
                ret.Add(add);
                ix = next + (f ?? delim).Length;
            }
            return ret.ToArray();
        }

        /// <summary>
        /// Return a trimmed string or null if there is only whitespace.
        /// </summary>
        public static string UserText(this string s, bool singlespace = false) { return (String.IsNullOrWhiteSpace(s)) ? null : (singlespace) ? s.SingleSpace() : s.Trim(); }
        public static string SingleSpace(this string s, bool allwhitespace = false, params char[] trim) { return (s == null) ? null : ((allwhitespace) ? MultiWhitespace : MultiSpaces).Replace(s, " ").Trim(trim ?? WhiteSpace).Trim(); }
        public static string TruncateText(this string s, int maxlen, bool adddots = true) { int dotlen = (adddots) ? 1 : 0; return (s == null) ? null : (maxlen < dotlen) ? String.Empty : (s.Length <= maxlen) ? s : String.Concat(s.Substring(0, maxlen - dotlen), (adddots) ? "…" : null); }
        public static string Surround(this string s, string prefix, string suffix = null, string sep = null) { return string.IsNullOrWhiteSpace(s) ? null : string.Concat(prefix, string.IsNullOrWhiteSpace(prefix) ? null : sep, s, string.IsNullOrWhiteSpace(suffix) ? null : sep, suffix); }

        public static string ToString(object obj, string format) {
            if (obj == null) return null;
            try {
                TypeCode tc = Type.GetTypeCode(obj.GetType());
                if (tc == TypeCode.Object && obj is IEnumerable) {
                    string[] param = (format != null) ? format.Split('•') : null;
                    List<string> ret = new List<string>();
                    string fmt = (param != null && param.Length > 0) ? param[0] : null;
                    foreach (object it in (IEnumerable)obj) {
                        string s = (it != null) ? UserText(ToString(it, fmt)) : null;
                        if (s != null) ret.Add(s);
                    }
                    return String.Join((param != null && param.Length > 1) ? param[1] : ", ", ret);
                }
                if (format != null && format.IndexOf("{0", StringComparison.Ordinal) == -1) {
                    switch (tc) {
                        case TypeCode.SByte: return ((sbyte)obj).ToString(format);
                        case TypeCode.Byte: return ((byte)obj).ToString(format);
                        case TypeCode.Int16: return ((short)obj).ToString(format);
                        case TypeCode.UInt16: return ((ushort)obj).ToString(format);
                        case TypeCode.Int32: return ((int)obj).ToString(format);
                        case TypeCode.UInt32: return ((uint)obj).ToString(format);
                        case TypeCode.Int64: return ((long)obj).ToString(format);
                        case TypeCode.UInt64: return ((ulong)obj).ToString(format);
                        case TypeCode.Single: return ((float)obj).ToString(format);
                        case TypeCode.Double: return ((double)obj).ToString(format);
                        case TypeCode.Decimal: return ((decimal)obj).ToString(format);
                        case TypeCode.DateTime:
                            DateTimeOffset? dt = DateTimeEx.GetDate(obj);
                            return (dt != null) ? dt.Value.ToString(format) : null;
                        case TypeCode.String: break;
                        case TypeCode.Object:
                            if (typeof(DateTimeOffset?).IsInstanceOfType(obj) ||
                            typeof(DateTimeOffset).IsInstanceOfType(obj) ||
                            typeof(DateTime?).IsInstanceOfType(obj) ||
                            typeof(DateTime).IsInstanceOfType(obj)) goto case TypeCode.DateTime;
                            if (typeof(TimeSpan?).IsInstanceOfType(obj) ||
                                typeof(TimeSpan).IsInstanceOfType(obj)) {
                                TimeSpan? ts = DateTimeEx.GetTime(obj);
                                return (ts == null) ? null : (!String.IsNullOrWhiteSpace(format)) ? ts.Value.ToString(format) : DateTimeEx.ToTimeString(ts.Value);
                            }
                            break;
                    }
                    //return obj.ToString(); letting the format statement take precedence over the object, so "Hello" and "Hello {0}" will result in "Hello" and "Hello Joe" if "Joe" is format value
                }
                return (!String.IsNullOrWhiteSpace(format)) ? String.Format(format, obj) : obj.ToString();
            } catch { return null; }
        }
        public static object FromString(object obj, Type t, string fmt, bool usedef) { return FromString(obj, t, fmt, usedef: usedef, tryconvert: true); }
        internal static object FromString(object obj, Type t, string fmt, bool usedef, bool tryconvert) {
            if (t == null) return obj;
            try {
                if (EnumEx.IsEnumObject(t)) return EnumEx.AsEnum(t, obj);
                string s = obj as string;
                switch (Type.GetTypeCode(t)) {
                    case TypeCode.Boolean: return NumberEx.GetBool(obj) ?? ((usedef) ? false : (object)null);
                    case TypeCode.SByte: return NumberEx.GetSByte(obj) ?? ((usedef) ? (sbyte)0 : (object)null);
                    case TypeCode.Byte: return NumberEx.GetByte(obj) ?? ((usedef) ? (byte)0 : (object)null);
                    case TypeCode.Int16: return NumberEx.GetShort(obj) ?? ((usedef) ? (short)0 : (object)null);
                    case TypeCode.UInt16: return NumberEx.GetUShort(obj) ?? ((usedef) ? (ushort)0 : (object)null);
                    case TypeCode.Int32:
                        int? n = NumberEx.GetInt(obj) ?? ((usedef) ? (int)0 : (int?)null);
                        if (typeof(int[]).IsAssignableFrom(t)) return n?.AsArray();
                        return n;
                    case TypeCode.UInt32: return NumberEx.GetUInt(obj) ?? ((usedef) ? (uint)0 : (object)null);
                    case TypeCode.Int64: return NumberEx.GetLong(obj) ?? ((usedef) ? (long)0 : (object)null);
                    case TypeCode.UInt64: return NumberEx.GetULong(obj) ?? ((usedef) ? (ulong)0 : (object)null);
                    case TypeCode.Single: return NumberEx.GetFloat(obj) ?? ((obj is string && String.IsNullOrWhiteSpace((string)obj)) ? float.NaN : (usedef) ? (float)0 : (object)null);
                    case TypeCode.Double: return NumberEx.Get(obj) ?? ((obj is string && String.IsNullOrWhiteSpace((string)obj)) ? double.NaN : (usedef) ? (double)0 : (object)null);
                    case TypeCode.Decimal: return NumberEx.Get(obj) ?? ((usedef) ? decimal.Zero : (object)null);
                    case TypeCode.String: return (obj == null) ? (string)null : (obj as string) ?? obj.ToString();
                    case TypeCode.DateTime:
                        DateTimeOffset? dt = DateTimeEx.GetDate(obj);
                        if (typeof(DateTimeOffset?).IsAssignableFrom(t)) return dt;
                        if (typeof(DateTimeOffset).IsAssignableFrom(t)) return (dt != null) ? dt.Value : (usedef) ? DateTimeOffset.Now : (object)null;
                        if (typeof(DateTime?).IsAssignableFrom(t)) return (dt != null) ? dt.Value.DateTime : (usedef) ? (DateTime?)null : (object)null;
                        if (typeof(DateTime).IsAssignableFrom(t)) return (dt != null) ? dt.Value.DateTime : (usedef) ? DateTime.Now : (object)null;
                        return (dt != null) ? (object)dt.Value : null;
                    case TypeCode.Object:
                        if (typeof(DateTimeOffset?).IsAssignableFrom(t) ||
                        typeof(DateTimeOffset).IsAssignableFrom(t) ||
                        typeof(DateTime?).IsAssignableFrom(t) ||
                        typeof(DateTime).IsAssignableFrom(t)) goto case TypeCode.DateTime;
                        if (typeof(TimeSpan?).IsAssignableFrom(t) ||
                            typeof(TimeSpan).IsAssignableFrom(t)) {
                            TimeSpan? ts = DateTimeEx.GetTime(obj);
                            if (typeof(TimeSpan?).IsAssignableFrom(t)) return (ts != null) ? ts.Value : (usedef) ? TimeSpan.Zero : (object)null;
                            if (typeof(TimeSpan).IsAssignableFrom(t)) return (ts != null) ? ts.Value : (usedef) ? TimeSpan.Zero : (object)null;
                            return (ts != null) ? (object)ts.Value : null;
                        }
                        if (t.IsArray) {
                            Type arrt = t.GetElementType();
                            if (s != null) {
                                string[] arr = Split(s, ",", true);
                                if (arr != null) {
                                    int len = arr.Length;
                                    Array many = Array.CreateInstance(arrt, len);
                                    for (int i = 0; i < len; i++)
                                        many.SetValue(FromString(arr[i], arrt, fmt, usedef), i);
                                    return many;
                                }
                            } else {
                                object one = FromString(obj, arrt, fmt, usedef);
                                if (one != null) return one.AsArray();
                            }
                        }
                        break;
                }
            } catch { }
            return (obj != null && t.IsInstanceOfType(obj)) ? obj : (usedef && t.IsValueType) ? Activator.CreateInstance(t) : null;
        }
        public static string SafeDBName(this string name) { return (!String.IsNullOrWhiteSpace(name)) ? SafeName(name, "_", "X", false) ?? "XXX" : null; }
        public static string SafeName(this string name, string okchars, string replace, bool upperfirst, bool numstoend = true) { // replace all non-alphanumeric chars unless listed in okchars and move preceeding numbers to the end; uppercase first letters after non-allowed chars
            if (String.IsNullOrWhiteSpace(name)) return null;
            bool isfirst = true;
            bool ischar = true;
            StringBuilder sb = new StringBuilder(), pre = new StringBuilder();
            foreach (char c in name) {
                if (Char.IsLetter(c)) {
                    sb.Append((!ischar && upperfirst) ? Char.ToUpper(c) : c);
                    isfirst = false;
                    ischar = true;
                } else if (Char.IsNumber(c) || (okchars != null && okchars.IndexOf(c) != -1)) {
                    if (isfirst && numstoend) pre.Append(c);
                    else sb.Append(c);
                    ischar = true;
                } else {
                    if (ischar && !String.IsNullOrEmpty(replace)) sb.Append(replace);
                    ischar = false;
                }
            }
            return (sb.Length <= 0 && pre.Length <= 0) ? null : (pre.Length <= 0) ? sb.ToString() : String.Concat(sb.ToString(), '_', pre.ToString());
        }

        public static string ToJSValueStr(this object obj, Func<double, int, string> fmtnum = null, int precision = 3, string quote = null) {
            if (obj == null) return null;
            try {
                IOEx.TypeCodeEx ext;
                switch (IOEx.GetTypeCode(ref obj, out ext)) {
                    case TypeCode.Single: double f = ((float)obj).FloatToDouble(); return !f.IsValid() ? null : fmtnum != null ? fmtnum(f, precision) : Math.Round(f, precision).ToString();
                    case TypeCode.Double: double d = (double)obj; return !d.IsValid() ? null : fmtnum != null ? fmtnum(d, precision) : Math.Round(d, precision).ToString();
                    case TypeCode.Decimal: return fmtnum != null ? fmtnum((double)decimal.Round((decimal)obj, precision), precision) : decimal.Round((decimal)obj, precision).ToString();
                    case TypeCode.String: return string.Concat(quote, ToString(obj, null), quote).UserText();
                    case TypeCode.DateTime:
                        switch (ext) {
                            case IOEx.TypeCodeEx.NullableTimeSpan:
                            case IOEx.TypeCodeEx.TimeSpan:
                                TimeSpan? ts = DateTimeEx.GetTime(obj, true);
                                return ts == null ? null : string.Concat(quote, DateTimeEx.ToTimeString(ts.Value), quote).UserText();
                            default:
                                DateTimeOffset? dt = DateTimeEx.GetDate(obj, true, true);
                                return dt == null ? null : string.Concat(quote, dt?.UtcDateTime.ToJSDateStr(), quote).UserText();
                        }
                    case TypeCode.Object:
                        switch (ext) {
                            case IOEx.TypeCodeEx.ByteArray: return string.Concat(quote, Convert.ToBase64String(obj as byte[] ?? IOEx.Empty), quote).UserText();
                            case IOEx.TypeCodeEx.Guid: return string.Concat(quote, (Guid)obj, quote).UserText();
                        }
                        object[] arr = obj is IEnumerable ? ((IEnumerable)obj)?.Enumerate(typeof(object)).ToArray() : null;
                        if (arr == null) break;
                        return string.Concat(quote?.Length > 0 ? "[" : null, string.Join(CommaSep, arr.Select(z => ReferenceEquals(z, obj) ? null : ToJSValueStr(z, fmtnum, precision, quote))), quote?.Length > 0 ? "]" : null);
                }
                return ToString(obj, null);
            } catch {
                return null;
            }
        }
    }
}
