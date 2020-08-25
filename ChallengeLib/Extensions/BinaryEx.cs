using System;

namespace AE.CoreInterface
{
    /// <summary>
    /// Helper class to assist with IO operations
    /// </summary>
    public static class BinaryEx
    {
        #region Endianness
        public static byte[] SwapIfNeeded(this byte[] v, bool bigendian = false) { return (NeedSwap(bigendian)) ? v?.Swap() : v; }

        public static bool NeedSwap(bool bigendian = false) { return bigendian == BitConverter.IsLittleEndian; }
        public static byte[] Swap(this byte[] v) {
            if (v?.Length > 0 && v.Length <= GuidSize && (v.Length % 2) == 0) {
                for (int i = 0, len = v.Length / 2; i < len; i++) {
                    byte b = v[i];
                    v[i] = v[v.Length - i - 1];
                    v[v.Length - i - 1] = b;
                }
                return v;
            }
            throw new Exception("Invalid Endian Bit Swap");
        }
        public static byte[] SafeSwap(this byte[] v) {
            try { return Swap(v); } catch { };
            return null;
        }

        public static ulong Swap(this ulong v) {
            return unchecked(
                ((v & 0xff00000000000000UL) >> 56) |
                ((v & 0x00ff000000000000UL) >> 40) |
                ((v & 0x0000ff0000000000UL) >> 24) |
                ((v & 0x000000ff00000000UL) >> 8) |
                ((v & 0x00000000ff000000UL) << 8) |
                ((v & 0x0000000000ff0000UL) << 24) |
                ((v & 0x000000000000ff00UL) << 40) |
                ((v & 0x00000000000000ffUL) << 56));
        }

        public static byte[] GetBytes(this bool v, bool bigendian = false) { return BitConverter.GetBytes(v); }
        public static byte[] GetBytes(this char v, bool bigendian = false) { return BitConverter.GetBytes(v).SwapIfNeeded(bigendian); }
        public static byte[] GetBytes(this short v, bool bigendian = false) { return BitConverter.GetBytes(v).SwapIfNeeded(bigendian); }
        public static byte[] GetBytes(this ushort v, bool bigendian = false) { return BitConverter.GetBytes(v).SwapIfNeeded(bigendian); }
        public static byte[] GetBytes(this int v, bool bigendian = false) { return BitConverter.GetBytes(v).SwapIfNeeded(bigendian); }
        public static byte[] GetBytes(this uint v, bool bigendian = false) { return BitConverter.GetBytes(v).SwapIfNeeded(bigendian); }
        public static byte[] GetBytes(this long v, bool bigendian = false) { return BitConverter.GetBytes(v).SwapIfNeeded(bigendian); }
        public static byte[] GetBytes(this ulong v, bool bigendian = false) { return BitConverter.GetBytes(v).SwapIfNeeded(bigendian); }
        public static byte[] GetBytes(this float v, bool bigendian = false) { return BitConverter.GetBytes(v).SwapIfNeeded(bigendian); }
        public static byte[] GetBytes(this double v, bool bigendian = false) { return BitConverter.GetBytes(v).SwapIfNeeded(bigendian); }
        public static byte[] GetBytes(byte[] buf, int ix, int sz) {
            if (sz == int.MaxValue) return null;
            if (ix + sz > buf.Length) throw new Exception("Invalid buffer length");
            byte[] ret = new byte[sz];
            Array.Copy(buf, ix, ret, 0, sz);
            return ret;
        }

        public static bool ToBoolean(this byte[] v, int ix, bool bigendian = false) { bool swap = NeedSwap(bigendian); return BitConverter.ToBoolean((swap) ? GetBytes(v, ix, sizeof(bool)).Swap() : v, (swap) ? 0 : ix); }
        public static short ToInt16(this byte[] v, int ix, bool bigendian = false) { bool swap = NeedSwap(bigendian); return BitConverter.ToInt16((swap) ? GetBytes(v, ix, sizeof(short)).Swap() : v, (swap) ? 0 : ix); }
        public static ushort ToUInt16(this byte[] v, int ix, bool bigendian = false) { bool swap = NeedSwap(bigendian); return BitConverter.ToUInt16((swap) ? GetBytes(v, ix, sizeof(ushort)).Swap() : v, (swap) ? 0 : ix); }
        public static int ToInt32(this byte[] v, int ix, bool bigendian = false) { bool swap = NeedSwap(bigendian); return BitConverter.ToInt32((swap) ? GetBytes(v, ix, sizeof(int)).Swap() : v, (swap) ? 0 : ix); }
        public static uint ToUInt32(this byte[] v, int ix, bool bigendian = false) { bool swap = NeedSwap(bigendian); return BitConverter.ToUInt32((swap) ? GetBytes(v, ix, sizeof(uint)).Swap() : v, (swap) ? 0 : ix); }
        public static long ToInt64(this byte[] v, int ix, bool bigendian = false) { bool swap = NeedSwap(bigendian); return BitConverter.ToInt64((swap) ? GetBytes(v, ix, sizeof(long)).Swap() : v, (swap) ? 0 : ix); }
        public static ulong ToUInt64(this byte[] v, int ix, bool bigendian = false) { bool swap = NeedSwap(bigendian); return BitConverter.ToUInt64((swap) ? GetBytes(v, ix, sizeof(ulong)).Swap() : v, (swap) ? 0 : ix); }
        public static float ToSingle(this byte[] v, int ix, bool bigendian = false) { bool swap = NeedSwap(bigendian); return BitConverter.ToSingle((swap) ? GetBytes(v, ix, sizeof(float)).Swap() : v, (swap) ? 0 : ix); }
        public static double ToDouble(this byte[] v, int ix, bool bigendian = false) { bool swap = NeedSwap(bigendian); return BitConverter.ToDouble((swap) ? GetBytes(v, ix, sizeof(double)).Swap() : v, (swap) ? 0 : ix); }
        #endregion Endianness

        #region Binary Blob Operations
        public const int GuidSize = 16;
        public const int MaxSQLInterfaceBytes = 8000;
        public const int MaxSQLInterfaceString = MaxSQLInterfaceBytes / 2;
        public const int MaxIndexableBytes = 1700;
        public const int MaxClusteredIndexableBytes = 900;
        public const int MaxColumnsPerSelect = 4096;
        public const int MaxIndexableString = MaxIndexableBytes / 2;
        public const int MaxClusteredIndexableString = MaxClusteredIndexableBytes / 2;
        public const int MaxCompoundIndexableBytes = MaxIndexableBytes - 100; // leave room for other index columns in compound index
        public const int MaxCompoundIndexableString = MaxIndexableString - 100; // leave room for other index columns in compound index
        #endregion Binary Blob Operations
    }
}
