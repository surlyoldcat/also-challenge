using AE.CoreUtility;
using System;
using System.Linq;
using System.Text;
using Xunit;

namespace ChallengeTest
{
    public class ChallengeTest
    {
        public readonly static Guid TestGuid = new Guid(new string('F', 32));

        [Fact]
        public void IOEx_BlobIO() {
            string[] map = new string[] { "Int", "Int", "NVarChar", "VarBinary" };
            object[] val = new object[] { 1234, IOEx.SafeIO<int>(null), "初めまして", Encoding.UTF8.GetBytes("This is a binary string") };
            BlobIO b = new BlobIO(map, val);
            byte[] m = b.Map;
            Assert.Equal(b.GetN<int>(0), val[0]); // Failed to get value");
            b.Set(0, 9876);
            Assert.Equal(b.GetN<int>(0), 9876); // Failed to set value");
            b.Set(0, 9876.0);
            Assert.Equal(b.GetN<int>(0), 9876); // Failed to set value");
            b.Clear(0);
            Assert.Equal(b.GetN<int>(0), null); // Failed to clear value");
            b = BlobIO.Data(NumberEx.Numeric(Math.PI, out decimal? v) ? v : null);
            Assert.Equal((decimal)Math.PI, b.GetDecimal(0));
            b = BlobIO.Data(val);
            Assert.Equal(b.GetN<int>(0), val[0]); // Failed to get value");
            Assert.Equal(b.GetN<int>(1), val[1] as int?); // Failed to get value");
            Assert.Equal(b.Get<string>(2), val[2]); // Failed to get value");
            Assert.True(CollectionEx.ArrayEquals(b.Get<byte[]>(3), val[3] as byte[]), "Failed to get value");
            Assert.True(CollectionEx.ArrayEquals(m, b.Map), "Failed to get value");
            byte[] io;
            Assert.True(NumberEx.TryFromServerHex("0x0000000308080C001111111100111111110000000000", out io), "Failed to parse IO");
            b = new BlobIO(io);
            Assert.Null(b.PK);
            string str = b.Str;
            b.PK = 0;
            Assert.True(NumberEx.TryFromServerHex("0x000100030808000000000C001111111100111111110000000000", out io), "Failed to parse IO with PK");
            Assert.NotEqual(str, b.Str);
            Assert.Equal(0, b.PK);
            b.PK = null;
            Assert.True(NumberEx.TryFromServerHex("0x0000000308080C001111111100111111110000000000", out io), "Failed to parse IO with PK");
            Assert.Equal(str, b.Str);
            Assert.Null(b.PK);
            b.PK = -1;
            Assert.True(NumberEx.TryFromServerHex("0x0001000308080FFFFFFFFC001111111100111111110000000000", out io), "Failed to parse IO with PK");
            Assert.NotEqual(str, b.Str);
            Assert.Equal(-1, b.PK);

            b = null;
            b += 1;
            b += (short)1;
            b += "初めまして";
            b += Math.PI;
            b += Math.PI.DoubleToFloat();
            b += IOEx.SafeIO<long>(null);
            b += (bool?)null;
            b += TestGuid;
            b += io;
            b += DateTimeEx.s_Epoch;
            string friendly = "1, 1, '初めまして', 3.142, 3.142, NULL, NULL, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 'AAEAAwgID/////wAEREREQARERERAAAAAAA=', '01/01/1970 00:00:00'";
            string bstr = "AAMACggQDAYNAAIOFSEAAAAAAQAAAQAAAAAP5Yid44KB44G+44GX44GmAEAJIftURC0YAEBJD9sBAQD/////////////////////AAAAABoAAQADCAgP/////AARERERABEREREAAAAAAAAIn3/197WAAAD/AAAABQAAAMD/";
            Assert.Null(b.PK);
            Assert.Equal(bstr, b.Str); // Failed to get value");
            Assert.Equal("Int, SmallInt, NVarChar, Float, Real, BigInt, Bit, UniqueIdentifier, VarBinary, DateTime2", b.StrMap); // Failed to get value");
            Assert.Equal("1, 1, '初めまして', 3.1415927, 3.141593, NULL, NULL, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 0x0001000308080ffffffffc001111111100111111110000000000, '1970-01-01T00:00:00.000'", b.StrData); // Failed to get value");
            Assert.Equal(friendly, b.Friendly); // Failed to get value");
            b.Name = "V";
            b.Cols = "a,b,c,d,e,f,g,h,i,j".Split(',');
            Assert.Equal("a, b, c, d, e, f, g, h, i, j", b.StrCols); // Failed to get value");
            Assert.Equal("\"a\", \"b\", \"c\", \"d\", \"e\", \"f\", \"g\", \"h\", \"i\", \"j\"", b.Select); //, "Failed to get value");
            Assert.Equal("\"a\" = 1, \"b\" = 1, \"c\" = 0x1d5281307e3057306630, \"d\" = 3.1415927, \"e\" = 3.141593, \"f\" = NULL, \"g\" = NULL, \"h\" = 'ffffffff-ffff-ffff-ffff-ffffffffffff', \"i\" = 0x0001000308080ffffffffc001111111100111111110000000000, \"j\" = '1970-01-01T00:00:00.000'", b.Update); //, "Failed to get value");
            Assert.Equal("\"a\" = V.GetInt32(0), \"b\" = V.GetInt16(1), \"c\" = V.GetString(2), \"d\" = V.GetDouble(3), \"e\" = V.GetSingle(4), \"f\" = V.GetInt64(5), \"g\" = V.GetBool(6), \"h\" = V.GetGuid(7), \"i\" = V.GetBytes(8), \"j\" = V.GetDateTime(9)", b.UpdateVar); //, "Failed to get value");
            Assert.Equal(" (\"a\", \"b\", \"c\", \"d\", \"e\", \"f\", \"g\", \"h\", \"i\", \"j\") VALUES (1, 1, 0x1d5281307e3057306630, 3.1415927, 3.141593, NULL, NULL, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 0x0001000308080ffffffffc001111111100111111110000000000, '1970-01-01T00:00:00.000')", b.Insert); //, "Failed to get value");
            Assert.Equal(" (\"a\", \"b\", \"c\", \"d\", \"e\", \"f\", \"g\", \"h\", \"i\", \"j\") VALUES (V.GetInt32(0), V.GetInt16(1), V.GetString(2), V.GetDouble(3), V.GetSingle(4), V.GetInt64(5), V.GetBool(6), V.GetGuid(7), V.GetBytes(8), V.GetDateTime(9))", b.InsertVar); //, "Failed to get value");
            Assert.Equal("V", b.Name); // Failed to get value");

            byte[] bio = BlobIO.ExportBlobList(b);

            b = BlobIO.Data(1, (short)1, "初めまして", Math.PI, Math.PI.DoubleToFloat(), IOEx.SafeIO<long>(null), (bool?)null, TestGuid, io, DateTimeEx.s_Epoch);
            Assert.Equal(friendly, b.Friendly); // Failed to get value");
            Assert.True(b.Concat(b));
            Assert.Equal(string.Join(", ", friendly, friendly), b.Friendly); // Failed to get value");
            b = new BlobIO(bio);
            Assert.Equal(friendly, b.Friendly); // Failed to get value");
            Assert.True(b.Set(4, BlobIO.Data(float.NaN, 0L, false, Guid.Empty, (byte[])null)));
            Assert.Equal("1, 1, '初めまして', 3.142, NULL, 0, False, '00000000-0000-0000-0000-000000000000', NULL, '01/01/1970 00:00:00'", b.Friendly); // Failed to get value");

            b = new BlobIO(bio);
            io = b.IO;
            Assert.True(CollectionEx.ArrayEquals(bio, io));
            bio = BlobIO.ExportBlobList();
            Assert.Null(bio);
            bio = BlobIO.ExportBlobList(b, b, b, b);
            Assert.True(CollectionEx.ArrayEquals(bio.Take(io.Length), io));
            Assert.True(bio?.Length > io.Length);
            Assert.True(bio?.Length < (io.Length * 4));
            int ix = 0;
            foreach (var pair in BlobIO.ToBlobList(bio)) {
                Assert.Equal(--ix, pair.Key);
                Assert.True(CollectionEx.ArrayEquals(pair.Value, io.Take(pair.Value.Length)));
            }
            byte[] dataio, prefixio, suffixio, ixsio, sbioio;
            string data = "0000000001000001000000000fe5889de38281e381bee38197e381a600400921fb54442d180040490fdb010100ffffffffffffffffffffffffffffffff000000001a0001000308080ffffffffc00111111110011111111000000000000089f7ff5f7b5800000";
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + data, out dataio) ? dataio : null, b.DataIO));
            string prefix = "0x0003000a08100c060d00020e1521";
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex(prefix, out prefixio) ? prefixio : null, b.PrefixIO));
            string presuffix = "ff000a016101620163016401650166016701680169016a00";
            string suffix = presuffix + "05000000c0ff";
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + suffix, out suffixio) ? suffixio : null, b.SuffixIO));
            string ixs = "0002" + "00000066" + "000000cc";
            Assert.Equal(sizeof(short) + (sizeof(int) * 2), NumberEx.TryFromServerHex("0x" + ixs, out ixsio) ? ixsio?.Length : null);
            string sio = prefix + data + suffix;
            string sbio = sio + ixs + data + data + data;
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex(sbio, out sbioio) ? sbioio : null, bio), "BlobList serialized");

            b = new BlobIO(bstr);
            Assert.Equal(friendly, b.Friendly); // Failed to get value");

            b.PK = 0;
            Assert.Equal(0, b.PK);
            Assert.Equal("AQMAAAAAAAoIEAwGDQACDhUhAAAAAAEAAAEAAAAAD+WIneOCgeOBvuOBl+OBpgBACSH7VEQtGABASQ/bAQEA/////////////////////wAAAAAaAAEAAwgID/////wAEREREQARERERAAAAAAAACJ9/9fe1gAAA/wAAAAUAAADA/w==", b.Str); // Failed to get value");
            Assert.Equal("Int, SmallInt, NVarChar, Float, Real, BigInt, Bit, UniqueIdentifier, VarBinary, DateTime2", b.StrMap); // Failed to get value");
            Assert.Equal("1, 1, '初めまして', 3.1415927, 3.141593, NULL, NULL, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 0x0001000308080ffffffffc001111111100111111110000000000, '1970-01-01T00:00:00.000'", b.StrData); // Failed to get value");
            Assert.Equal(friendly, b.Friendly); // Failed to get value");
            b.Name = "V";
            b.Cols = "a,b,c,d,e,f,g,h,i,j".Split(',');
            Assert.Equal("a, b, c, d, e, f, g, h, i, j", b.StrCols); // Failed to get value");
            Assert.Equal("\"a\", \"b\", \"c\", \"d\", \"e\", \"f\", \"g\", \"h\", \"i\", \"j\"", b.Select); //, "Failed to get value");
            Assert.Equal("\"a\" = 1, \"b\" = 1, \"c\" = 0x1d5281307e3057306630, \"d\" = 3.1415927, \"e\" = 3.141593, \"f\" = NULL, \"g\" = NULL, \"h\" = 'ffffffff-ffff-ffff-ffff-ffffffffffff', \"i\" = 0x0001000308080ffffffffc001111111100111111110000000000, \"j\" = '1970-01-01T00:00:00.000'", b.Update); //, "Failed to get value");
            Assert.Equal("\"a\" = V.GetInt32(0), \"b\" = V.GetInt16(1), \"c\" = V.GetString(2), \"d\" = V.GetDouble(3), \"e\" = V.GetSingle(4), \"f\" = V.GetInt64(5), \"g\" = V.GetBool(6), \"h\" = V.GetGuid(7), \"i\" = V.GetBytes(8), \"j\" = V.GetDateTime(9)", b.UpdateVar); //, "Failed to get value");
            Assert.Equal(" (\"a\", \"b\", \"c\", \"d\", \"e\", \"f\", \"g\", \"h\", \"i\", \"j\") VALUES (1, 1, 0x1d5281307e3057306630, 3.1415927, 3.141593, NULL, NULL, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 0x0001000308080ffffffffc001111111100111111110000000000, '1970-01-01T00:00:00.000')", b.Insert); //, "Failed to get value");
            Assert.Equal(" (\"a\", \"b\", \"c\", \"d\", \"e\", \"f\", \"g\", \"h\", \"i\", \"j\") VALUES (V.GetInt32(0), V.GetInt16(1), V.GetString(2), V.GetDouble(3), V.GetSingle(4), V.GetInt64(5), V.GetBool(6), V.GetGuid(7), V.GetBytes(8), V.GetDateTime(9))", b.InsertVar); //, "Failed to get value");
            Assert.Equal("V", b.Name); // Failed to get value");

            int seed = 0;
            io = b.Export(false, ref seed);
            bio = BlobIO.ExportBlobList(false, b);
            Assert.True(CollectionEx.ArrayEquals(bio, io));
            bio = BlobIO.ExportBlobList(false);
            Assert.Null(bio);
            bio = BlobIO.ExportBlobList(false, b, b, b, b);
            Assert.True(CollectionEx.ArrayEquals(bio.Take(io.Length), io));
            Assert.True(bio?.Length > io.Length);
            Assert.True(bio?.Length < (io.Length * 4));
            ix = 0;
            foreach (var pair in BlobIO.ToBlobList(bio)) {
                Assert.Equal(--ix, pair.Key);
                Assert.True(CollectionEx.ArrayEquals(pair.Value, io.Take(pair.Value.Length)));
            }
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + data, out dataio) ? dataio : null, b.DataIO));
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex(prefix, out prefixio) ? prefixio : null, b.PrefixIO));
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + suffix, out suffixio) ? suffixio : null, b.SuffixIO));
            Assert.Equal(sizeof(short) + (sizeof(int) * 2), NumberEx.TryFromServerHex("0x" + ixs, out ixsio) ? ixsio?.Length : null);
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex(sbio, out sbioio) ? sbioio : null, bio), "BlobList serialized");


            Assert.Equal(io.Length + sizeof(int), b.IO.Length);
            io = b.IO;
            bio = BlobIO.ExportBlobList(true, b);
            Assert.True(CollectionEx.ArrayEquals(bio, io));
            bio = BlobIO.ExportBlobList(true);
            Assert.Null(bio);
            bio = BlobIO.ExportBlobList(true, b, b, b, b);
            Assert.True(CollectionEx.ArrayEquals(bio.Take(io.Length), io));
            Assert.True(bio?.Length > io.Length);
            Assert.True(bio?.Length < (io.Length * 4));
            io = b.PrefixIO.Concat(b.DataIO).ToArray();
            foreach (var pair in BlobIO.ToBlobList(bio)) {
                Assert.Equal(0, pair.Key);
                Assert.True(CollectionEx.ArrayEquals(pair.Value, io));
            }
            ixs = "0002" + "0000006a" + "000000d4";
            Assert.Equal(sizeof(short) + (sizeof(int) * 2), NumberEx.TryFromServerHex("0x" + ixs, out ixsio) ? ixsio?.Length : null);
            prefix = "0x0103" + "00000000" + "000a08100c060d00020e1521";
            sio = prefix + data + suffix;
            sbio = sio + ixs + "00000000" + data + "00000000" + data + "00000000" + data;
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex(sbio, out sbioio) ? sbioio : null, bio), "BlobList serialized");


            b = new BlobIO(bstr);
            b.PK = -1;
            Assert.Equal(-1, b.PK);

            io = b.IO;
            bio = BlobIO.ExportBlobList(true, b);
            Assert.True(CollectionEx.ArrayEquals(bio, io));
            bio = BlobIO.ExportBlobList(true);
            Assert.Null(bio);
            bio = BlobIO.ExportBlobList(true, b, b, b, b);
            Assert.True(CollectionEx.ArrayEquals(bio.Take(io.Length), io));
            Assert.True(bio?.Length > io.Length);
            Assert.True(bio?.Length < (io.Length * 4));
            io = b.PrefixIO.Concat(b.DataIO).ToArray();
            foreach (var pair in BlobIO.ToBlobList(bio)) {
                Assert.Equal(-1, pair.Key);
                Assert.True(CollectionEx.ArrayEquals(pair.Value, io));
            }
            ixs = "0002" + "0000006a" + "000000d4";
            Assert.Equal(sizeof(short) + (sizeof(int) * 2), NumberEx.TryFromServerHex("0x" + ixs, out ixsio) ? ixsio?.Length : null);
            prefix = "0x0103" + "FFFFFFFF" + "000a08100c060d00020e1521";
            sio = prefix + data + "ff00000005000000c0ff";
            sbio = sio + ixs + "FFFFFFFF" + data + "FFFFFFFF" + data + "FFFFFFFF" + data;
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex(sbio, out sbioio) ? sbioio : null, bio), "BlobList serialized");


            b.Name = "V";
            b.Cols = "a,b,c,d,e,f,g,h,i,j".Split(',');
            io = b.IO;
            bio = BlobIO.ExportBlobList(true, b);
            Assert.True(CollectionEx.ArrayEquals(bio, io));
            bio = BlobIO.ExportBlobList(true);
            Assert.Null(bio);
            bio = BlobIO.ExportBlobList(true, b, b, b, b);
            Assert.True(CollectionEx.ArrayEquals(bio.Take(io.Length), io));
            Assert.True(bio?.Length > io.Length);
            Assert.True(bio?.Length < (io.Length * 4));
            io = b.PrefixIO.Concat(b.DataIO).ToArray();
            foreach (var pair in BlobIO.ToBlobList(bio)) {
                Assert.Equal(-1, pair.Key);
                Assert.True(CollectionEx.ArrayEquals(pair.Value, io));
            }
            ixs = "0002" + "0000006a" + "000000d4";
            Assert.Equal(sizeof(short) + (sizeof(int) * 2), NumberEx.TryFromServerHex("0x" + ixs, out ixsio) ? ixsio?.Length : null);
            prefix = "0x0103" + "FFFFFFFF" + "000a08100c060d00020e1521";
            sio = prefix + data + suffix;
            sbio = sio + ixs + "FFFFFFFF" + data + "FFFFFFFF" + data + "FFFFFFFF" + data;
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex(sbio, out sbioio) ? sbioio : null, bio), "BlobList serialized");



            b.PK = null;
            Assert.Null(b.PK);

            seed = 0;
            io = b.Export(true, ref seed);
            bio = BlobIO.ExportBlobList(true, b);
            Assert.True(CollectionEx.ArrayEquals(bio, io));
            bio = BlobIO.ExportBlobList(true);
            Assert.Null(bio);
            bio = BlobIO.ExportBlobList(true, b, b, b, b);
            Assert.True(CollectionEx.ArrayEquals(bio.Take(io.Length), io));
            Assert.True(bio?.Length > io.Length);
            Assert.True(bio?.Length < (io.Length * 4));
            io = b.PrefixIO.Concat(b.DataIO).ToArray();
            ix = 0;
            foreach (var pair in BlobIO.ToBlobList(bio)) {
                Assert.Equal(--ix, pair.Key);
                Assert.True(CollectionEx.ArrayEquals(pair.Value, io));
            }
            ixs = "0002" + "0000006a" + "000000d4";
            Assert.Equal(sizeof(short) + (sizeof(int) * 2), NumberEx.TryFromServerHex("0x" + ixs, out ixsio) ? ixsio?.Length : null);
            prefix = "0x0103" + "FFFFFFFF" + "000a08100c060d00020e1521";
            sio = prefix + data + suffix;
            sbio = sio + ixs + "FFFFFFFE" + data + "FFFFFFFD" + data + "FFFFFFFC" + data;
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex(sbio, out sbioio) ? sbioio : null, bio), "BlobList serialized");



            b.PK = null;
            Assert.Null(b.PK);
            b.Indexes = new int[] { 1, 3, 5 };
            Assert.Equal("1,3,5", b.StrIxs); // Failed to get indexes");
            Assert.Null(b.StrNulls); // Failed to get null oks");
            Assert.Null(b.StrOuts); // Failed to get outputs");

            seed = 0;
            io = b.Export(true, ref seed);
            bio = BlobIO.ExportBlobList(true, b);
            Assert.True(CollectionEx.ArrayEquals(bio, io));
            bio = BlobIO.ExportBlobList(true);
            Assert.Null(bio);
            bio = BlobIO.ExportBlobList(true, b, b, b, b);
            Assert.True(CollectionEx.ArrayEquals(bio.Take(io.Length), io));
            Assert.True(bio?.Length > io.Length);
            Assert.True(bio?.Length < (io.Length * 4));
            io = b.PrefixIO.Concat(b.DataIO).ToArray();
            ix = 0;
            foreach (var pair in BlobIO.ToBlobList(bio)) {
                Assert.Equal(--ix, pair.Key);
                Assert.True(CollectionEx.ArrayEquals(pair.Value, io));
            }
            ixs = "0002" + "0000006a" + "000000d4";
            Assert.Equal(sizeof(short) + (sizeof(int) * 2), NumberEx.TryFromServerHex("0x" + ixs, out ixsio) ? ixsio?.Length : null);
            prefix = "0x0103" + "FFFFFFFF" + "000a08100c060d00020e1521";
            suffix = presuffix + "052a0000c0ff";
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + suffix, out suffixio) ? suffixio : null, b.SuffixIO));
            sio = prefix + data + suffix;
            sbio = sio + ixs + "FFFFFFFE" + data + "FFFFFFFD" + data + "FFFFFFFC" + data;
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex(sbio, out sbioio) ? sbioio : null, bio), "BlobList serialized");

            b.Indexes = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            Assert.Equal("0,1,2,3,4,5,6,7,8,9", b.StrIxs); // Failed to get indexes");
            Assert.Null(b.StrNulls); // Failed to get null oks");
            Assert.Null(b.StrOuts); // Failed to get outputs");
            suffix = presuffix + "05ff0300c0ff";
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + suffix, out suffixio) ? suffixio : null, b.SuffixIO));

            b.Indexes = new int[] { 0, 24 };
            Assert.Equal("0", b.StrIxs); // Failed to get indexes");
            Assert.Null(b.StrNulls); // Failed to get null oks");
            Assert.Null(b.StrOuts); // Failed to get outputs");
            suffix = presuffix + "05010000c0ff";
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + suffix, out suffixio) ? suffixio : null, b.SuffixIO));

            b.Indexes = new int[] { 0 };
            Assert.Equal("0", b.StrIxs); // Failed to get indexes");
            Assert.Null(b.StrNulls); // Failed to get null oks");
            Assert.Null(b.StrOuts); // Failed to get outputs");
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + suffix, out suffixio) ? suffixio : null, b.SuffixIO));

            b.Indexes = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            b.Nulls = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            b.Outs = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            b.Ins = new int[] { 6 };
            Assert.Equal("0,1,2,3,4,5,6,7,8,9", b.StrIxs); // Failed to get indexes");
            Assert.Equal("0,1,2,3,4,5,6,7,8,9", b.StrNulls); // Failed to get null oks");
            Assert.Equal("0,1,2,3,4,5,6,7,8,9", b.StrOuts); // Failed to get outputs");
            suffix = presuffix + "05ffffff3f10";
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + suffix, out suffixio) ? suffixio : null, b.SuffixIO));

            b.Indexes = null;
            b.Nulls = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            b.Outs = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            Assert.Null(b.StrIxs); // Failed to get indexes");
            Assert.Equal("0,1,2,3,4,5,6,7,8,9", b.StrNulls); // Failed to get null oks");
            Assert.Equal("0,1,2,3,4,5,6,7,8,9", b.StrOuts); // Failed to get outputs");
            suffix = presuffix + "0500fcff3f10";
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + suffix, out suffixio) ? suffixio : null, b.SuffixIO));

            b.Nulls = null;
            b.Outs = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            Assert.Null(b.StrIxs); // Failed to get indexes");
            Assert.Null(b.StrNulls); // Failed to get null oks");
            Assert.Equal("0,1,2,3,4,5,6,7,8,9", b.StrOuts); // Failed to get outputs");
            suffix = presuffix + "050000f03f10";
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + suffix, out suffixio) ? suffixio : null, b.SuffixIO));

            b.Nulls = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            b.Outs = null;
            Assert.Null(b.StrIxs); // Failed to get indexes");
            Assert.Equal("0,1,2,3,4,5,6,7,8,9", b.StrNulls); // Failed to get null oks");
            Assert.Null(b.StrOuts); // Failed to get outputs");
            suffix = presuffix + "0500fc0f0010";
            Assert.True(CollectionEx.ArrayEquals(NumberEx.TryFromServerHex("0x" + suffix, out suffixio) ? suffixio : null, b.SuffixIO));

            Assert.True(NumberEx.TryFromServerHex("0x0100" + "FFFFFFFF" + "000a08100c060d00020e1521" + data +
                presuffix +
                ixs + "FFFFFFFE" + data + "FFFFFFFD" + data + "FFFFFFFC" + data, out bio));
            ix = 0;
            foreach (var pair in BlobIO.ToBlobList(bio))
                Assert.Equal(--ix, pair.Key);
        }
    }
}
