using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace AE.CoreUtility
{
    [DebuggerStepThrough]
    public static class CollectionEx
    {
        public static int DoCompare<T>(T x, T y, Comparison<T> dif) { return (x == null && y == null) ? 0 : (x == null) ? -1 : (y == null) ? 1 : (Object.ReferenceEquals(x, y)) ? 0 : dif(x, y); }
        public static int IndexOf<T>(this T[] arr, T find, Comparison<T> dif) where T : IComparable {
            if (arr == null) return 0;
            int count = arr.Length;
            if (count <= 0 || dif == null) return -1;
            for (int i = 0; i < count; i++) {
                if (DoCompare(find, arr[i], dif) == 0) return i;
            }
            return -1;
        }
        public static T[] DefaultIfEmpty<T>(this T[] obj, params T[] ifempty) { return (obj?.Any() == true) ? obj : ifempty; }
        public static IEnumerable<T> NotNull<T>(this IEnumerable<T> obj) where T : class { return obj?.Where(z => z != null); }

        public static T[] AsArray<T>(this T obj) { return new T[] { obj }; }
        public static T[] Fill<T>(this T val, int count) {
            T[] ret = new T[count];
            for (int i = 0; i < count; i++)
                ret[i] = val;
            return ret;
        }
        public static void Fill<T>(this T[] set, T val) {
            for (int i = 0, len = set?.Length ?? 0; i < len; i++)
                set[i] = val;
        }
        public static int[] Sequence(this int count, int start = 0, int inc = 1, bool exclude = true, params int[] ixs) {
            int[] ret = new int[count];
            for (int i = 0; i < count; i++, start += inc)
                ret[i] = start;
            if (ixs?.Any() != true) return ret;
            return exclude ? ret.Except(ixs).ToArray() : ret.Intersect(ixs).ToArray();
        }

        public static IEnumerator<T> EnumerateAll<T>(this IEnumerable e, int recurse = 0) { // 0 = no recurse, -1 = recurse all
            if (e == null) yield break;
            Type t = typeof(T);
            bool iscls = t.IsClass;
            foreach (object obj in e) {
                if (obj == null && !iscls) continue;
                if (obj != null && !t.IsInstanceOfType(obj)) continue;
                T it = (T)obj;
                yield return it;
                IEnumerable k = (recurse != 0) ? it as IEnumerable : null;
                if (k == null) continue;
                IEnumerator<T> kids = k.EnumerateAll<T>((recurse > 0) ? recurse - 1 : recurse);
                while (kids.MoveNext())
                    yield return kids.Current;
            }
        }
        public static IEnumerator<object> EnumerateAll(this IEnumerable e, Type t, int recurse = 0) { // 0 = no recurse, -1 = recurse all
            if (e == null || t == null) yield break;
            foreach (object it in e) {
                if (t.IsInstanceOfType(it)) yield return it;
                IEnumerable k = (recurse != 0) ? it as IEnumerable : null;
                if (k == null) continue;
                IEnumerator kids = k.EnumerateAll(t, (recurse > 0) ? recurse - 1 : recurse);
                while (kids.MoveNext()) {
                    object kid = kids.Current;
                    if (t.IsInstanceOfType(kid)) yield return kid;
                }
            }
        }
        public sealed class EnumerateEnumerable<T> : IEnumerable<T>
        {
            private IEnumerator<T> E;
            public EnumerateEnumerable(IEnumerator<T> e) { E = e; }
            public IEnumerator<T> GetEnumerator() { if (E == null) throw new InvalidOperationException(); return E; }
            IEnumerator IEnumerable.GetEnumerator() { return GetEnumerator(); }
        }
        public static IEnumerable<T> Enumerate<T>(this IEnumerable e, int recurse = 0) { return new EnumerateEnumerable<T>(EnumerateAll<T>(e, recurse)); }
        public static IEnumerable<T> Enumerate<T>(this IEnumerator<T> e) { return new EnumerateEnumerable<T>(e); }
        public static IEnumerable<object> Enumerate(this IEnumerable e, Type t, int recurse = 0) { return new EnumerateEnumerable<object>(EnumerateAll(e, t, recurse)); }

        public static bool ArrayEquals<T>(IEnumerable<T> a, IEnumerable<T> b) { return (a == null) ? b == null : b != null && a.Count() == b.Count() && a.SequenceEqual(b); }


        public readonly static Random Rand = new Random();

    }
}
