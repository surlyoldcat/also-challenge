using System;
using System.Collections.Generic;
using System.Linq;

namespace AE.CoreUtility
{
    public static class EnumEx
    {
        //private readonly static object s_lock = new object();
        //private readonly static Dictionary<Type, IEnumerable<Enum>> s_cache = new Dictionary<Type, IEnumerable<Enum>>();
        public readonly static Enum[] Empty = new Enum[0];

        public static IEnumerable<Enum> Get(this Type t) {
            try {
                if (t == null || !t.IsEnum) return Empty;
                return Enum.GetValues(t).OfType<Enum>().ToArray();
                //try {
                //	if (s_cache.ContainsKey(t)) return s_cache[t];
                //} catch { }
                //lock (s_lock) {
                //	if (s_cache.ContainsKey(t)) return s_cache[t];
                //	return s_cache[t] = (from x in t.GetFields(BindingFlags.Static | BindingFlags.Public) select (Enum)x.GetValue(t));
                //}
            } catch {
                return Empty;
            }
        }

        public static bool IsEnumObject(this Type t) { return t != null && (t.IsEnum || typeof(Enum).IsAssignableFrom(t)); }
        public static Enum AsEnum(this string s, Type t, bool ignorecase = false, bool checkdisplay = false) {
            if (t == null || string.IsNullOrWhiteSpace(s)) return null;
            try {
                object n = s;
                if (NumberEx.Numeric(ref n, t.NumericType(), errdef: false, parse: true)) return Enum.ToObject(t, n) as Enum;
                StringComparison comp = ignorecase ? StringComparison.InvariantCultureIgnoreCase : StringComparison.InvariantCulture;

                Enum ret = string.Equals(s, StringEx.SafeDBName(s), comp) ? Get(t)?.FirstOrDefault(z => string.Equals(z?.ToString(), s, comp)) : null;
                if (ret != null) return ret;
            } catch { }
            return null;
        }

        /// <summary>
        /// Convert a int to the specified enum struct type
        /// </summary>
        /// <typeparam name="T">Enum struct type</typeparam>
        /// <param name="i">Numeric value to convert to the enum type</param>
        /// <param name="def">Default value if an exception occurs during conversion</param>
        /// <param name="flagsok">When false, the default is returned if the specific named instance of the enum is not found. When true numeric values not present in the enum list can be referenced.</param>
        /// <returns>Numeric value cast to the specifid enum struct type</returns>
        public static T AsEnum<T>(this int i, T def, bool flagsok = true) where T : struct, IConvertible, IComparable, IFormattable { try { return flagsok ? (T)Enum.ToObject(typeof(T), i) : NamedEnum((T)Enum.ToObject(typeof(T), i), def); } catch { return def; } }
        /// <summary>
        /// Ensure that the enum value is explicitly specified in the enum or return the default provided
        /// </summary>
        /// <typeparam name="E">Enum struct type</typeparam>
        /// <param name="e">Enum value to locate</param>
        /// <param name="def">Default value if an exception occurs during conversion</param>
        /// <returns>Provided enum or default if the explicit value is not found</returns>
        public static bool IsDefined<E>(this E e) where E : struct, IConvertible, IComparable, IFormattable { try { return Enum.IsDefined(typeof(E), e); } catch { return false; } }

        /// <summary>
        /// Ensure that the enum value is explicitly specified in the enum or return the default provided
        /// </summary>
        /// <typeparam name="E">Enum struct type</typeparam>
        /// <param name="e">Enum value to locate</param>
        /// <param name="def">Default value if an exception occurs during conversion</param>
        /// <returns>Provided enum or default if the explicit value is not found</returns>
        public static E NamedEnum<E>(this E e, E def) where E : struct, IConvertible, IComparable, IFormattable { return IsDefined(e) ? e : def; } // !Char.IsNumber(e.ToString()?.ToCharArray()?.FirstOrDefault() ?? '0')
        private static object ConvertEnum(this object e) { return Convert.ChangeType(e, Enum.GetUnderlyingType(e.GetType()), null); }

        public static Enum AsEnum(Type t, object val, bool ignorecase = false, bool checkdisplay = false) {
            if (val == null || !EnumEx.IsEnumObject(t)) return null;
            if (val is Enum && t.IsInstanceOfType(val)) return (Enum)val;
            if (!t.IsEnum) return null;
            try {
                string v = val as string;
                if (v != null) return v.AsEnum(t, ignorecase, checkdisplay);
                object n = val;
                if (NumberEx.Numeric(ref n, t.NumericType(), false)) return Enum.ToObject(t, n) as Enum;
            } catch { }
            return null;
        }

        /// <summary>
        /// Convert struct enum type to an int
        /// </summary>
        /// <typeparam name="E">Enum struct type</typeparam>
        /// <param name="e">Enum value to convert</param>
        /// <returns>Numeric representation of the enum</returns>
        public static int ToInt<E>(this E e) where E : struct, IConvertible, IComparable, IFormattable { try { return Convert.ToInt32(e.ConvertEnum()); } catch { return 0; } }
        /// <summary>
        /// Convert struct enum type to an byte
        /// </summary>
        /// <typeparam name="E">Enum struct type</typeparam>
        /// <param name="e">Enum value to convert</param>
        /// <returns>Numeric representation of the enum</returns>
        public static byte ToByte<E>(this E e) where E : struct, IConvertible, IComparable, IFormattable { try { return Convert.ToByte(e.ConvertEnum()); } catch { return 0; } }

        /// <summary>
        /// Convert an byte to the specified enum struct type
        /// </summary>
        /// <typeparam name="E">Enum struct type</typeparam>
        /// <param name="i">Numeric value to convert to the enum type</param>
        /// <param name="def">Default value if an exception occurs during conversion</param>
        /// <param name="flagsok">When false, the default is returned if the specific named instance of the enum is not found. When true numeric values not present in the enum list can be referenced.</param>
        /// <returns>Numeric value cast to the specifid enum struct type</returns>
        public static E FromByte<E>(this byte n, E def, bool flagsok = false) where E : struct, IConvertible, IComparable, IFormattable { try { return flagsok ? (E)Convert.ChangeType(n, Enum.GetUnderlyingType(typeof(E)), null) : NamedEnum((E)Convert.ChangeType(n, Enum.GetUnderlyingType(typeof(E)), null), def); } catch { return def; } }

        /// <summary>
        /// Convert a string to the specified enum struct type
        /// </summary>
        /// <typeparam name="E">Enum struct type</typeparam>
        /// <param name="s">String value to convert to the enum type</param>
        /// <param name="def">Default value if an exception occurs during conversion</param>
        /// <returns>String value cast to the specifid enum struct type</returns>
        public static E FromString<E>(this string s, E def, bool ignorecase = false) where E : struct, IConvertible, IComparable, IFormattable { E ret; return (!string.IsNullOrWhiteSpace(s) && Enum.TryParse(s.Trim(), ignorecase, out ret)) ? ret : def; }
        public static Type NumericType(this Type t) {
#if NETSTANDARD || SQLCLRDLL
            return t?.GetEnumUnderlyingType();
#else // NETSTANDARD || SQLCLRDLL
            return t != null ? Enum.GetUnderlyingType(t) : null;
#endif // NETSTANDARD || SQLCLRDLL
        }
    }
}
