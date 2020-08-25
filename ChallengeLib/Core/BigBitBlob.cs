using AE.CoreInterface;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Schema;
using System.Xml.Serialization;

namespace AE.CoreUtility
{
    /// <summary>
    /// Utility to pack a list of flagged values in a master list into a byte array.
    /// The assumption is that the master list contents and order remain the same, so this is not intended for long-term serialization unless the list is maintained as a static or kept unchanged.
    /// Uses include transient client/server communication of permission lists and other lists that are fixed at compile time.
    /// Any update or mismatch between compiled code instances (such as running different versions between client and server applications) will produce unexpected behavior and should not be used in those scenarios.
    /// </summary>
    public class BigBitBlob : IXmlSerializable, IComparable, IEqualityComparer<BigBitBlob>, IEquatable<BigBitBlob>, IOString, IOBinary
    {
        public virtual string Collection { get { return GetType().Name; } }
        public enum CompareBlobs : byte
        {
            All = 0,
            Any = 1,
            Contains = 2
        };

        public static BigBitBlob operator ~(BigBitBlob a) { return (a == null) ? null : new BigBitBlob(a.Max, false, NumberEx.Complement(a.Blob, true)); }
        public static BigBitBlob operator |(BigBitBlob a, BigBitBlob b) { return (a == null && b == null) ? null : (a == null) ? b.Dupe() : (b == null) ? a.Dupe() : new BigBitBlob(Math.Max(a.Max, b.Max), false, NumberEx.Or(a.Blob, b.Blob, true)); }
        public static BigBitBlob operator &(BigBitBlob a, BigBitBlob b) { return (a == null || b == null) ? null : new BigBitBlob(Math.Max(a.Max, b.Max), false, NumberEx.And(a.Blob, b.Blob, true)); }
        public static bool operator ==(BigBitBlob a, BigBitBlob b) { return a?.Equals(b) ?? Object.ReferenceEquals(b, null); }
        public static bool operator !=(BigBitBlob a, BigBitBlob b) { return !(a == b); }
        public override bool Equals(object obj) { return obj is BigBitBlob && Equals(this, (BigBitBlob)obj); }
        public override int GetHashCode() { return IO?.GetHashCode() ?? 0; }
        public override string ToString() { return Str; }

        protected const int MaxLength = 30000;
        private const int ibitsinbyte = 8;
        protected const int ibitsperblob = sizeof(ulong) * ibitsinbyte;
        private const double bitsper = ibitsperblob;
        private const double bitsinbyte = ibitsinbyte;
        protected void CheckReadOnly() { if (m_readonly) throw new Exception($"Editing static bit flags is not permitted: {Str ?? BlobIO.NULL}"); }
        protected bool m_readonly;

        /// <summary>
        /// Blob prefix for binary IO 
        /// </summary>
        protected byte[] Prefix { get { return _Prefix ?? (_Prefix = new byte[prefixlen]); } set { CheckReadOnly(); int len = Math.Min(prefixlen, value?.Length ?? 0); if (len < prefixlen) Prefix.Fill((byte)0); if (len > 0) Array.Copy(value, Prefix, len); } }
        private byte[] _Prefix = null;
        protected virtual byte prefixlen { get { return 0; } }
        protected byte[] OutIO(byte[] blob) { return (blob == null) ? Prefix : (Prefix?.Any() != true) ? blob : Prefix.Concat(blob).ToArray(); }
        protected byte[] InIO(byte[] blob, out byte[] pre) {
            int len = (Prefix?.Length > 0 && blob?.Length >= Prefix.Length) ? Prefix.Length : 0;
            pre = (len > 0) ? blob.Take(len).ToArray() : Prefix;
            return (len > 0) ? blob.Skip(len).ToArray() : blob;
        }

        /// <summary>
        /// Size of the master list
        /// </summary>
        public int Max { get; private set; }
        /// <summary>
        /// Packed bits into a ulong array.
        /// Each item in the master list corresponds to a bit at the same position as the master list index.
        /// </summary>
        public ulong[] Blob { get; protected set; }
        public bool IOOK { get { return Blob != null; } }
        public bool StrOK { get { return IOOK; } }

        /// <summary>
        /// Binary export of packed bits
        /// Each item in the master list corresponds to a bit at the same position as the master list index.
        /// </summary>
        public byte[] FullIO { get { return Blob?.SelectMany(z => BinaryEx.GetBytes(z)).Take((int)Math.Ceiling(Max / bitsinbyte)).ToArray().DefaultIfEmpty(null); } set { Import(value); } }
        /// <summary>
        /// Binary export of packed bits excluding unflagged bits at the end to make the output as short as possible
        /// Each item in the master list corresponds to a bit at the same position as the master list index.
        /// </summary>
        public byte[] IO { get { return OutIO(FullIO?.Take((int)Math.Ceiling((MaxIndex + 1) / bitsinbyte)).ToArray().DefaultIfEmpty(null)); } set { Import(InIO(value, out _Prefix)); } }
        public byte[] SQLIO { get { return IO ?? IOEx.Empty; } set { IO = (value?.Length == 1 && value[0] == 0) ? null : value; } }
        /// <summary>
        /// Base64 encoding of bytes
        /// </summary>
        public string Str { get { return Convert.ToBase64String(IO ?? IOEx.Empty); } set { IO = IOfromStr(value); } }
        public static byte[] IOfromStr(string str) { return (str == null || str.Length <= 0) ? null : (str[0] == '.' || str[0] == '*') ? null : Convert.FromBase64String(str); }

        /// <summary>
        /// Get or set whether the item at specified index is flagged
        /// </summary>
        /// <param name="ix"></param>
        /// <returns></returns>
        public bool? this[int ix] {
            get { return BitFlagged(ix); }
            set {
                CheckReadOnly();
                int at = BitAt(ix);
                if (at == -1) return;
                if (value == true) Blob[at] |= OneBit(ix);
                else Blob[at] &= ~OneBit(ix);
            }
        }
        /// <summary>
        /// Check if all or any of the provided indexes are flagged
        /// </summary>
        /// <param name="all">Require all listed indexes to be flagged, otherwise match if any are flagged when getting, when setting, toggle on when true and off when false</param>
        /// <param name="ixs">Array of indexes to validate</param>
        /// <returns>True of all or any of the provided indexes are flagged</returns>
        public bool this[bool all, params int[] ixs] {
            get { return (all) ? ixs?.All(z => this[z] == true) == true : ixs?.Any(z => this[z] == true) == true; }
            set {
                CheckReadOnly();
                int len = ixs?.Length ?? 0;
                if (len <= 0) return;
                for (int i = 0; i < len; i++)
                    this[ixs[i]] = all;
            }
        }
        /// <summary>
        /// Get the index of the ulong in the Blob containing this index bit
        /// </summary>
        /// <param name="ix">Index of the desired bit</param>
        /// <returns>The index of the ulong in the Blob containing this index bit</returns>
        private int BitAt(int ix) {
            if (ix < 0 || Blob == null) return -1;
            int at = (int)Math.Floor(ix / bitsper);
            if (at >= Blob.Length) return -1;
            return at;
        }
        /// <summary>
        /// Check if the bit at the specified index is on, off, or the Blob is not initialized to the size needed to accommodate the index
        /// </summary>
        /// <param name="ix">Index of the desired bit</param>
        /// <returns>True if the bit is on, False if off, and null if the Blob does is not large enough to have the specified index</returns>
        private bool? BitFlagged(int ix) { int at = BitAt(ix); return at == -1 ? (bool?)null : BitFlagged(Blob[at], ix); }
        /// <summary>
        /// Calculate the bit position for a given index and return a blob ulong with just that bit flagged
        /// i.e. 5 would be 0x0000000000000010
        /// and 69 (69 % 64 = 5) would also be 0x0000000000000010
        /// </summary>
        /// <param name="ix"></param>
        /// <returns>A blob ulong with just that bit flagged</returns>
        private static ulong OneBit(int ix) { return unchecked(1UL << (ix % ibitsperblob)); }
        /// <summary>
        /// Check if the bit at the specified index is on, off, or the Blob is not initialized to the size needed to accommodate the index
        /// Find the bit within the Blob ulong part using the absolute index within the entire Blob set
        /// This is basically the bit at the provided index minus the starting index of this Blob part 
        /// </summary>
        /// <param name="blobat">Blob part retrieved from the Blob at the BitAt location</param>
        /// <param name="ix">Index of the desired bit</param>
        /// <returns>True if the bit is on, False if off, and null if the Blob does is not large enough to have the specified index</returns>
        private static bool BitFlagged(ulong blobat, int ix) { return (blobat & OneBit(ix)) != 0; }
        public bool IsEmpty { get { return Blob?.All(z => z == 0) ?? true; } }
        public int Count {
            get {
                if (Blob == null) return 0;
                int ret = 0;
                for (int at = 0, len = Blob.Length; at < len; at++) {
                    ulong blob = Blob[at];
                    if (blob == 0) continue;
                    for (int i = 0; i < ibitsperblob; i++) {
                        if (BitFlagged(blob, i)) ret++;
                    }
                }
                return ret;
            }
        }
        /// <summary>
        /// Find the last assumed index by locating the last necessary bit
        /// This is not just the size of the Blob parts which is padded by 64 bits because the Blob
        /// </summary>
        private int MaxIndex {
            get {
                if (Blob == null) return -1;
                for (int at = Blob.Length - 1; at >= 0; at--) {
                    ulong blob = Blob[at];
                    if (blob == 0) continue;
                    for (int i = ibitsperblob - 1; i >= 0; i--) {
                        if (BitFlagged(blob, i)) return (at * ibitsperblob) + i;
                    }
                }
                return -1;
            }
        }
        /// <summary>
        /// Ensure that all provided flags are on or off
        /// </summary>
        /// <param name="flag">Set of values to modify</param>
        /// <param name="on">True to turn on and False to clear</param>
        /// <returns></returns>
        protected bool Flag(BigBitBlob flag, bool on) {
            if (Max != flag?.Max) return false;
            Blob = (on) ?
                NumberEx.Or(Blob, flag.Blob, false) :
                NumberEx.And(Blob, NumberEx.Complement(flag.Blob, true));
            return true;
        }
        /// <summary>
        /// Ensure that the specified number of flags starting at the provided index are on or off
        /// </summary>
        /// <param name="ix">Starting index of the values to modify</param>
        /// <param name="len">Starting index of the values to modify</param>
        /// <param name="on">True to turn on and False to clear</param>
        /// <returns></returns>
        public bool Flag(int ix, int len, bool on) {
            if (Blob == null) return false;
            for (int at = BitAt(ix), sz = Math.Min(BitAt(ix + len - 1), Blob.Length - 1); at <= sz; at++) {
                ulong exp = on ? ulong.MaxValue : 0L;
                ulong blobat = Blob[at];
                if (blobat == exp) continue;
                int st = at * ibitsperblob;
                int end = st + ibitsperblob;
                if (ix <= st && ix + len >= end) { // fast option if all bits in this blob are getting flagged
                    blobat = on ? ulong.MaxValue : 0L;
                } else {
                    for (int i = 0; i < ibitsperblob; i++) {
                        if (st + i >= ix + len) break;
                        if (st + i < ix) continue;
                        if (on) blobat |= OneBit(st + i);
                        else blobat &= ~OneBit(st + i);
                    }
                }
                Blob[at] = blobat;
            }
            return true;
        }

        protected BigBitBlob() { }
        protected BigBitBlob(int max, byte[] io) { Init(max); IO = io; }
        protected BigBitBlob(int max, bool dupe, params ulong[] blob) { Init(max, dupe, blob); }
        public BigBitBlob(int max) { Init(max); }
        public BigBitBlob(int max, params ulong[] blob) : this(max, true, blob) { }
        public BigBitBlob(BigBitBlob obj) : this(obj?.Max ?? 0, true, obj?.Blob) { Prefix = obj?.Prefix; }

        private void Init(int max, bool dupe, params ulong[] blob) {
            if (blob != null) {
                Max = max = Math.Min(max, blob.Length * ibitsperblob);
                if (max > 0) {
                    Blob = (dupe) ? new ulong[blob.Length] : blob;
                    if (dupe && blob.Length > 0) Array.Copy(blob, Blob, blob.Length);
                } else Blob = null;
            } else Init(max);
        }
        protected void Init(int max) {
            Max = max;
            int sz = (max > 0) ? (int)Math.Ceiling(max / bitsper) : 0;
            Blob = (sz > 0) ? new ulong[sz] : null;
        }
        public BigBitBlob Dupe() { return new BigBitBlob(this); }

        protected static int MinLength(byte[] io, int prefixlen) {
            int ret = ((io?.Length ?? prefixlen) - prefixlen) * ibitsinbyte;
            if (ret <= 0) return ret;
            for (int i = io.Length - 1; i >= 0; i--) {
                byte b = io[i];
                if (b == 0) {
                    ret -= ibitsinbyte;
                    continue;
                }
                for (int j = ibitsinbyte - 1; j >= 0; j--) { // don't count any zeros at the end of the block as min length
                    byte bit = (byte)(0x01 << j);
                    if ((b & bit) != 0) break;
                    ret--;
                    continue;
                }
                break;
            }
            return ret;
        }
        protected static int MinLength(ulong[] io, int prefixlen) {
            int ret = (((io?.Length * sizeof(ulong)) ?? prefixlen) - prefixlen) * 8;
            if (ret <= 0) return ret;
            for (int i = io.Length - 1; i >= 0; i--) {
                ulong n = io[i];
                if (n == 0) {
                    ret -= ibitsperblob;
                    continue;
                }
                for (int j = ibitsperblob - 1; j >= 0; j--) { // don't count any zeros at the end of the block as min length
                    ulong bit = 1UL << j;
                    if ((n & bit) != 0) break;
                    ret--;
                    continue;
                }
                break;
            }
            return ret;
        }

        public bool IsMatch(BigBitBlob find, CompareBlobs cb = CompareBlobs.Contains) { return IsMatch(Blob, find?.Blob, cb); }
        public bool IsMatch(ulong[] find, CompareBlobs cb = CompareBlobs.Contains) { return IsMatch(Blob, find, cb); }

        public static bool IsMatch(BigBitBlob set, BigBitBlob find, CompareBlobs cb = CompareBlobs.Contains) { return IsMatch(set?.Blob, find?.Blob, cb); }
        public static bool IsMatch(BigBitBlob set, ulong[] find, CompareBlobs cb = CompareBlobs.Contains) { return IsMatch(set?.Blob, find, cb); }
        public static bool IsMatch(ulong[] set, ulong[] find, CompareBlobs cb = CompareBlobs.Contains) {
            int len = set?.Length ?? 0;
            if (len != (find?.Length ?? 0)) return false;
            if (set == null || find == null) return true;
            switch (cb) {
                default:
                case CompareBlobs.All:
                    for (int i = 0; i < len; i++) {
                        if (set[i] != find[i]) return false;
                    }
                    return true;
                case CompareBlobs.Any:
                    for (int i = 0; i < len; i++) {
                        if ((set[i] & find[i]) != 0) return true;
                    }
                    return false;
                case CompareBlobs.Contains:
                    ulong R;
                    for (int i = 0; i < len; i++) {
                        R = find[i];
                        if (R == 0) continue;
                        if ((set[i] & R) != R) return false;
                    }
                    return true;
            }
        }
        public int CompareTo(object obj) { return CompareTo(obj as BigBitBlob); }
        public int CompareTo(BigBitBlob b) {
            if (b == null) return -1;
            if (Object.ReferenceEquals(this, b)) return 0;
            if (b.Max == 0) return (Max == 0) ? 0 : 1;
            if (Max == 0) return -1;
            int tlen = Blob?.Length ?? 0;
            int blen = b.Blob?.Length ?? 0;
            for (int i = 0, len = Math.Min(tlen, blen); i < len; i++) {
                int ret = Blob[i].CompareTo(b.Blob[i]);
                if (ret != 0) return ret;
            }
            if (tlen < blen) return -1;
            if (tlen > blen) return 1;
            return 0;
        }
        public bool Equals(BigBitBlob x, BigBitBlob y) { return x?.Max == y?.Max && CollectionEx.ArrayEquals(x?.Blob, y?.Blob); }
        public int GetHashCode(BigBitBlob obj) { return Blob?.GetHashCode() ?? Max.GetHashCode(); }
        public bool Equals(BigBitBlob other) { return Equals(this, other); }

        /// <summary>
        /// Iterate unpacked list of index numbers that indicate which items in the master list are flagged
        /// </summary>
        public IEnumerator<int> GetFlaggedIndexes() {
            if (Blob == null) yield break;
            for (int at = 0, len = Blob.Length; at < len; at++) {
                ulong blob = Blob[at];
                if (blob == 0) continue;
                for (int i = 0; i < ibitsperblob; i++) {
                    if (BitFlagged(blob, i)) yield return (at * ibitsperblob) + i;
                }
            }
        }
        /// <summary>
        /// Set flagged using unpacked list of index numbers that indicate which items in the master list are flagged
        /// </summary>
        public void SetFlaggedIndexes(IEnumerable<int> ixs, int max = -1, bool clear = true, bool on = true) {
            CheckReadOnly();
            if (Blob == null) return;
            if (clear) {
                if (max >= 0 && max < Blob.Length * ibitsperblob) { // only clear the bits we know about
                    int fast = max / ibitsperblob;
                    for (int i = 0, len = fast; i < len; i++)
                        Blob[i] = 0UL;
                    for (int i = 0, len = max % ibitsperblob; i < len; i++)
                        this[fast + i] = false;
                } else Blob.Fill(0UL);
            }
            if (ixs == null) return;
            foreach (int i in ixs)
                this[i] = on;
        }

        private byte[] Export() {
            int len = Math.Min(Blob?.Length ?? 0, (int)Math.Ceiling(Max / bitsper));
            byte[] ret = new byte[len];
            int ix = 0;
            for (int i = 0; i < len; i++)
                IOEx.Put(Blob[i], ret, ref ix, false, len - ix);
            return ret;
        }
        private void Import(byte[] io) {
            CheckReadOnly();
            if (Blob?.Any() != true) return;
            if (io != null) {
                for (int i = 0, ix = 0, len = Blob.Length; ix < len; ix++) {
                    if (i >= io.Length)
                        Blob[ix] = 0;
                    else if (i + sizeof(ulong) < io.Length)
                        Blob[ix] = IOEx.GetUInt64(io, ref i, false);
                    else {
                        byte[] b = new byte[sizeof(ulong)];
                        Array.Copy(io, i, b, 0, io.Length - i);
                        int ii = 0;
                        Blob[ix] = IOEx.GetUInt64(b, ref ii, false);
                        i = io.Length;
                    }
                }
            } else Blob.Fill(0UL);
        }

        #region IXmlSerializable
        /// <summary>
        /// Required by IXmlSerializable.
        /// Should always be null
        /// </summary>
        public XmlSchema GetSchema() { return null; }

        /// <summary>
        /// IXmlSerializable implementation to export the key as a numeric string content
        /// </summary>
        public void ReadXml(XmlReader reader) { reader.MoveToContent(); Str = StringEx.UserText(reader.ReadElementContentAsString()); }

        /// <summary>
        /// IXmlSerializable implementation to import the key from a numeric string content
        /// </summary>
        public void WriteXml(XmlWriter writer) { writer.WriteValue(Str); }
        #endregion IXmlSerializable

        #region Serializable
        /// <summary>
        /// Binary deserialization for PermIO
        /// </summary>
        public void Read(BinaryReader r) { SQLIO = r.ReadBytes((int)r.BaseStream.Length); }

        /// <summary>
        /// Binary serialization for PermIO
        /// </summary>
        public void Write(BinaryWriter w) { w.Write(SQLIO); }
        #endregion Serializable
    }
}
