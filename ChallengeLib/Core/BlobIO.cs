using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using AE.CoreInterface;
#if NETSTANDARD || SQLCLRDLL
using SQLDBT = System.Data.SqlDbType;
#endif //  NETSTANDARD || SQLCLRDLL

namespace AE.CoreUtility
{
    public class BlobIO : IOBinary, IOString
    {
#if !NETSTANDARD && !SQLCLRDLL
        // lifted from System.Data.SqlDbType because portable class library does not support
        public enum SQLDBT : byte
        {
            BigInt = 0,
            Binary = 1,
            Bit = 2,
            Char = 3,
            DateTime = 4,
            Decimal = 5,
            Float = 6,
            Image = 7,
            Int = 8,
            Money = 9,
            NChar = 10,
            NText = 11,
            NVarChar = 12,
            Real = 13,
            UniqueIdentifier = 14,
            SmallDateTime = 15,
            SmallInt = 16,
            SmallMoney = 17,
            Text = 18,
            Timestamp = 19,
            TinyInt = 20,
            VarBinary = 21,
            VarChar = 22,
            Variant = 23,
            Xml = 25,
            Udt = 29,
            Structured = 30,
            Date = 31,
            Time = 32,
            DateTime2 = 33,
            DateTimeOffset = 34
        }
#endif // !NETSTANDARD && !SQLCLRDLL
        public readonly static BlobIO Empty = new BlobIO();
        public static implicit operator byte[](BlobIO b) { return b?.IO; }
        public static explicit operator BlobIO(byte[] b) { return b?.Any() == true ? new BlobIO(b) : null; }
        public static BlobIO operator +(BlobIO b, bool? v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, byte? v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, short? v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, int? v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, long? v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, float? v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, double? v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, decimal? v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, Guid? v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, DateTime? v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, TimeSpan? v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, bool v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, byte v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, short v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, int v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, long v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, float v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, double v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, decimal v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, Guid v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, DateTime v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, TimeSpan v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, string v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, byte[] v) { return (b ?? (b = new BlobIO())).Concat(v) ? b : null; }
        public static BlobIO operator +(BlobIO b, BlobIO v) { return b == null ? v?.Dupe() : v == null ? v : b.Concat(v) ? b : null; }

        internal const bool c_usebigendian = true; // use big endian for binary serialization to help sorting
        private const int c_minlen = sizeof(byte) + sizeof(short);
        private const int c_maxinc = 1000;
        public const byte IOREAD = 0x00; // version is followed by the map and data
        public const byte IOREADPK = 0x01; // version is followed by the PK, then the map and data
        public const byte IOWRITE = 0x00; // blob ends with the data, no suffix information
        public const byte IOWRITETBL = 0x01; // blob data is followed by the table name 
        public const byte IOWRITECOLS = 0x02; // blob data is followed by the table name and then column names
        public const byte IOWRITEIXS = 0x03; // blob data is followed by the table name, the column names and a bit flag indicating which columns should be compared as part of a merge statement
        public const string c_name = "V";
        public const string c_sourceprefix = "s";
        public const string c_targetprefix = "t";
        private const int c_multioff = 2;
        public const string NULL = "NULL";

        /// <summary>
        /// Note that all values are serialized as BigEndian for SQL compatibility
        /// 1 byte - IO version
        /// 4 bytes short - [Map length]
        /// 1 byte * [Map length] - Map sql data type list
        /// 1 + [Map[i] data length] * [Map length] - null = 1, non-null = 0 followed by data bytes
        /// </summary>
        public byte[] Value = null;

        private VersionIO _VER;
        private SQLDBT[] _Map;
        private object[] _Data; // optional
        private string[] _Cols; // optional
        private string _Tbl; // optional
        private int _PK; // optional
        private BigBitBlob _Ixs; // optional
        private bool _IncPK { get { return IncPK(_VER); } }
        private byte _Read { get { return Read(_VER, PK); } }
        private byte _Write { get { return Write(_VER, tbl: true, cols: true, ixs: true); } }
        private static bool IncPK(VersionIO v) { return v?.HasRead == true && v.Read == IOREADPK; }
        private static byte Read(VersionIO v, int? pk, bool change = false) {
            byte r = v?.HasRead == true ? v.Read : pk != null ? IOREADPK : IOREAD;
            if (!change) return r;
            switch (r) {
                case IOREAD:
                case IOREADPK:
                default: return pk != null ? IOREADPK : IOREAD;
            }
        }
        private static byte Write(VersionIO v, bool tbl, bool cols, bool ixs, bool change = false) {
            byte d = ixs ? IOWRITEIXS : cols ? IOWRITECOLS : tbl ? IOWRITETBL : IOWRITE;
            byte w = v?.HasWrite == true ? v.Write : d;
            if (!change) return w;
            switch (w) {
                case IOWRITE:
                case IOWRITETBL:
                case IOWRITECOLS:
                case IOWRITEIXS:
                default: return d > w ? d : w;
            }
        }

        public bool IOOK { get { return Length >= 0; } }
        public bool StrOK { get { return IOOK; } }
        public int Length { get { return _Map?.Length ?? 0; } }
        public int? PK { get { return _IncPK ? _PK : (int?)null; } set { _PK = value ?? 0; byte r = Read(_VER, value, true); VER.SetRead(r); } }
        public VersionIO VER { get { return _VER ?? (_VER = new VersionIO(_Read, _Write, true)); } }
        public int Count { get; private set; }

        /// <summary>
        /// Note that all values are serialized as BigEndian for SQL compatibility
        /// </summary>
        public byte[] IO { get { return Value ?? Export(prefix: true, map: true, data: true, cols: true, tbl: true, pk: _IncPK, ixs: true); } set { Import(value); } }

        /// <summary>
        /// Just the Prefix and Map portion of the IO without the data
        /// Note that all values are serialized as BigEndian for SQL compatibility
        /// </summary>
        public byte[] MapIO {
            get { return _Map?.ToBlobMap(); }
            set {
                //if (value?.Length > c_maxvals) throw new Exception($"Invalid index, value must be between 0 and {c_maxvals}");
                Value = null;
                _Map = value?.Select(z => z.FromByte(SQLDBT.Variant)).ToArray();
                _Data = new object[Length];
                InsOn = true.Fill(Length);
            }
        }

        /// <summary>
        /// Just the Prefix and Map portion of the IO without the data or column details
        /// The PK is included if the version indicates it should be
        /// Note that all values are serialized as BigEndian for SQL compatibility
        /// </summary>
        public byte[] PrefixIO { get { return Export(prefix: true, map: true, data: false, cols: false, tbl: false, pk: false, ixs: false); } }

        /// <summary>
        /// Just the Suffic portion of the IO with the column details
        /// Note that all values are serialized as BigEndian for SQL compatibility
        /// </summary>
        public byte[] SuffixIO { get { return Export(prefix: false, map: false, data: false, cols: true, tbl: true, pk: false, ixs: true); } }

        /// <summary>
        /// Just the Data portion of the IO without the map
        /// Note that all values are serialized as BigEndian for SQL compatibility
        /// </summary>
        public byte[] DataIO { get { return Export(prefix: false, map: false, data: true, cols: false, tbl: false, pk: false, ixs: false); } set { int ix = 0; Import(ref ix, value, prefix: false, map: false, data: true, cols: false, tbl: false, pk: false, ixs: false); } }

        public byte[] Map { get { return Import() ? _Map?.DefaultIfEmpty(null).ToBlobMap() : null; } }
        /// <summary>
        /// Indexes of columns to test with the MERGE ON clause
        /// </summary>
        public int[] Indexes { get { return GetIndexes(IndexKey.Where); } set { SetIndexes(IndexKey.Where, value); } }
        public bool[] IxsOn { get { return GetIndexOn(IndexKey.Where); } set { SetIndexOn(IndexKey.Where, value); } }
        /// <summary>
        /// Indexes of columns that should test for null matches in addition to data equality
        /// </summary>
        public int[] Nulls { get { return GetIndexes(IndexKey.Null); } set { SetIndexes(IndexKey.Null, value); } }
        public bool[] NullsOn { get { return GetIndexOn(IndexKey.Null); } set { SetIndexOn(IndexKey.Null, value); } }
        /// <summary>
        /// Indexes of columns to insert/update with the MERGE ON clause match
        /// </summary>
        public int[] Ins { get { return GetIndexes(IndexKey.Upsert); } set { SetIndexes(IndexKey.Upsert, value); } }
        public bool[] InsOn { get { return GetIndexOn(IndexKey.Upsert); } set { SetIndexOn(IndexKey.Upsert, value); } }
        /// <summary>
        /// Indexes of columns to output
        /// </summary>
        public int[] Outs { get { return GetIndexes(IndexKey.Output); } set { SetIndexes(IndexKey.Output, value); } }
        public bool[] OutsOn { get { return GetIndexOn(IndexKey.Output); } set { SetIndexOn(IndexKey.Output, value); } }
        private void IXResize(int len) {
            BigBitBlob old = _Ixs;
            int prev = Length;
            _Ixs = new BigBitBlob((int)IndexKey.Length * len);
            for (IndexKey key = IndexKey.Where; key < IndexKey.Length; key++) {
                int poff = (int)key * prev;
                int off = (int)key * len;
                for (int i = 0; i < len; i++)
                    _Ixs[off + i] = i < prev && old != null ? old[poff + i] : key == IndexKey.Upsert;
            }
        }
        public byte[] IndexesIO { get { return Import() ? _Ixs?.IO?.DefaultIfEmpty(null) : null; } set { _Ixs = new BigBitBlob(IndexLen) { IO = value }; } }
        private int IndexLen { get { return (int)IndexKey.Length * Length; } }
        private int[] GetIndexes(IndexKey key) { return Import() ? _Ixs?.GetFlaggedIndexes().Enumerate().Select(z => z - ((int)key * Length)).Where(z => z >= 0 && z < Length).ToArray().DefaultIfEmpty(null) : null; }
        private bool[] GetIndexOn(IndexKey key) {
            int[] ixs = GetIndexes(key);
            bool[] ret = CollectionEx.Fill(false, Length);
            if (ixs?.Any() != true) return ret;
            foreach (int i in ixs) {
                if (i < 0 || i >= Length) continue;
                ret[i] = true;
            }
            return ret;
        }
        private bool GetIndexOn(IndexKey key, int ix) { return ix >= 0 && ix < Length && _Ixs?[(key.ToInt() * Length) + ix] == true; }
        private void SetIndexes(IndexKey key, params int[] ixs) {
            int len = IndexLen;
            int off = (int)key * Length;
            if (_Ixs == null) _Ixs = new BigBitBlob(len);
            else _Ixs.Flag(off, Length, false);
            if (ixs?.Any() != true) return;
            foreach (int ix in ixs) {
                if (ix < 0 || ix >= Length) continue;
                _Ixs[off + ix] = true;
            }
        }
        private void SetIndexOn(IndexKey key, params bool[] on) {
            int len = IndexLen;
            int off = (int)key * Length;
            if (_Ixs == null) _Ixs = new BigBitBlob(len);
            else _Ixs.Flag(off, Length, false);
            if (on?.Any() != true) return;
            for (int ix = 0, _len = Math.Min(on.Length, Length); ix < _len; ix++) {
                if (on[ix]) _Ixs[off + ix] = true;
            }
        }
        private bool SetIndexOn(IndexKey key, int ix, bool on) {
            if (ix < 0 || ix >= Length) return false;
            int off = (int)key * Length;
            if (_Ixs == null) _Ixs = new BigBitBlob(IndexLen);
            _Ixs[off + ix] = on;
            return true;
        }
        private string GetStrIxs(IndexKey key) { return Import() ? GetIndexes(key)?.ToQuery() : null; }
        public bool IsWhere(int ix) { return GetIndexOn(IndexKey.Where, ix); }
        public bool SetWhere(int ix, bool on) { return SetIndexOn(IndexKey.Where, ix, on); }
        public bool IsUpsert(int ix) { return GetIndexOn(IndexKey.Upsert, ix); }
        public bool SetUpsert(int ix, bool on) { return SetIndexOn(IndexKey.Upsert, ix, on); }
        public bool IsNullOK(int ix) { return GetIndexOn(IndexKey.Null, ix); }
        public bool SetNullOK(int ix, bool on) { return SetIndexOn(IndexKey.Null, ix, on); }
        public bool IsOutput(int ix) { return GetIndexOn(IndexKey.Output, ix); }
        public bool SetOutput(int ix, bool on) { return SetIndexOn(IndexKey.Output, ix, on); }
        private void SetStrIxs(IndexKey key, string ixs) {
            int[] split = ixs?.TryToInts();
            //if (split?.Length > c_maxvals) throw new Exception($"Invalid index, value must be between 0 and {c_maxvals}");
            SetIndexes(key, split);
        }
        private enum IndexKey
        {
            Where = 0,
            Null = 1,
            Output = 2,
            Upsert = 3,

            Length = 4
        };

        public string Str { get { return Convert.ToBase64String(IO ?? IOEx.Empty); } set { IO = !string.IsNullOrWhiteSpace(value) ? Convert.FromBase64String(StringEx.UserText(value)) : null; } }
        public string StrIO { get { return Import() ? IO?.ToServerHex() : null; } set { byte[] io; IO = NumberEx.TryFromServerHex(value, out io) ? io : null; } }

        public string StrMap {
            get { return Import() ? string.Join(", ", _Map?.Select(z => z.ToString()).ToArray() ?? StringEx.Empty) : null; }
            set {
                string[] split = value?.SplitParams(true);
                //if (split?.Length > c_maxvals) throw new Exception($"Invalid index, value must be between 0 and {c_maxvals}");
                Value = null;
                _Map = split?.Select(z => z.FromString(SQLDBT.Variant, true)).ToArray();
                _Data = new object[Length];
                InsOn = true.Fill(Length);
            }
        }

        public string StrData { get { return Import() ? string.Join(StringEx.CommaSep, DataToSql(false) ?? StringEx.Empty) : null; } }
        public string StrIxs { get { return GetStrIxs(IndexKey.Where); } set { SetStrIxs(IndexKey.Where, value); } }
        public string StrIns { get { return GetStrIxs(IndexKey.Upsert); } set { SetStrIxs(IndexKey.Upsert, value); } }
        public string StrNulls { get { return GetStrIxs(IndexKey.Null); } set { SetStrIxs(IndexKey.Null, value); } }
        public string StrOuts { get { return GetStrIxs(IndexKey.Output); } set { SetStrIxs(IndexKey.Output, value); } }
        public string StrCols {
            get { return Import() ? string.Join(StringEx.CommaSep, _Cols ?? StringEx.Empty) : null; }
            set {
                string[] split = value?.SplitParams(true);
                //if (split?.Length > c_maxvals) throw new Exception($"Invalid index, value must be between 0 and {c_maxvals}");
                _Cols = split;
            }
        }
        /// <summary>
        /// Column names to SQL string for value
        /// !!! WARNING - Do not use this to build queries - since it is not parameterized, sql injection is possible !!!
        /// i.e. MyDate, '1/1/2019'
        /// </summary>
        public KeyValuePair<string, string>[] Pairs { get { int ix = -1, len = Cols?.Length ?? 0; return DataToSql()?.Select(z => new KeyValuePair<string, string>(++ix < len ? SelectCol(_Cols[ix]) : null, z)).ToArray(); } }
        /// <summary>
        /// Column names to Blob value extractor where the index of the column is used to pull the correct column value from the serialized blob
        /// i.e. MyDate, V.GetDateTime(0)
        /// </summary>
        public KeyValuePair<string, string>[] PairsVar { get { int ix = -1, len = Cols?.Length ?? 0; return _Map?.Select(z => new KeyValuePair<string, string>(++ix < len ? SelectCol(_Cols[ix]) : null, string.Concat(Name, '.', DataVar(z), '(', ix, ')'))).ToArray(); } }
        /// <summary>
        /// Column name to qualified source name
        /// i.e. MyDate, sV.MyDate
        /// </summary>
        public KeyValuePair<string, string>[] PairsSource { get { int ix = -1; return _Cols?.Select(z => new KeyValuePair<string, string>(SelectCol(z, DefSource(++ix)), SelSource(z, ix))).ToArray(); } }
        /// <summary>
        /// Column name to boolean for whether the value comparison should test for both values equal to null
        /// i.e. MyDate, true
        /// </summary>
        public KeyValuePair<string, bool>[] PairWhere { get { int ix = -1; return _Cols?.Select(z => new KeyValuePair<string, bool>(IsWhere(++ix) ? SelectCol(z, DefSource(ix)) : null, IsNullOK(ix))).Where(z => z.Key != null).ToArray(); } }
        /// <summary>
        /// Column name to boolean for whether the column should be returned in the output
        /// i.e. MyDate, true
        /// </summary>
        public KeyValuePair<string, bool>[] PairOutput { get { int ix = -1; return _Cols?.Select(z => new KeyValuePair<string, bool>(SelectCol(z, DefSource(++ix)), IsOutput(ix))).ToArray(); } }
        /// <summary>
        /// Column name to boolean for whether the column should be returned in the output
        /// i.e. MyDate, true
        /// </summary>
        public KeyValuePair<string, bool>[] PairDeclare { get { int ix = -1, len = Cols?.Length ?? 0; return _Map?.Select(z => new KeyValuePair<string, bool>(StringEx.UserText(string.Concat(++ix < len ? SelectCol(_Cols[ix], DefSource(ix)) : null, ' ', DataDeclare(z, true))), IsOutput(ix))).ToArray(); } }
        /// <summary>
        /// Data represented as a json string
        /// </summary>
        public string[] Strs { get { return Import() ? _Data?.Select(z => StringEx.ToJSValueStr(z, null, quote: "\'")).ToArray() : null; } }
        /// <summary>
        /// Optional column names to enable blob to be automatically built into a query
        /// </summary>
        public string[] Cols { get { return Import() ? _Cols : null; } set { _Cols = value?.Select(z => StringEx.UserText(z?.TruncateText(byte.MaxValue - 1, false))).ToArray(); } } // .Take(c_maxvals)
        /// <summary>
        /// Optional name to use when representing the table
        /// Default of V is used if not specified
        /// </summary>
        public string Name { get { return Import() ? StringEx.UserText(_Tbl) ?? c_name : c_name; } set { _Tbl = StringEx.UserText(value?.TruncateText(byte.MaxValue - 1, false)); if (string.Equals(_Tbl, c_name, StringComparison.Ordinal)) _Tbl = null; } }
        /// <summary>
        /// Differentiate tables for source and target in merge statements by prefixing table name with an s for source and t for target
        /// </summary>
        public string NameSource { get { return $"{c_sourceprefix}{Name ?? string.Empty}"; } }
        /// <summary>
        /// Differentiate tables for source and target in merge statements by prefixing table name with an s for source and t for target
        /// </summary>
        public string NameTarget { get { return $"{c_targetprefix}{Name ?? string.Empty}"; } }
        /// <summary>
        /// Debugging friendly output
        /// </summary>
        public string Friendly { get { return string.Join(StringEx.CommaSep, Strs?.Select(z => z ?? NULL) ?? StringEx.Empty); } }

        /// <summary>
        /// Comma-delimited list of column names
        /// i.e. ColA, ColB, ColC
        /// </summary>
        public string Select { get { return SelectStr(Cols); } }
        public string Upsert { get { int ix = -1; return SelectStr(Cols?.Where(z => IsUpsert(++ix) || IsWhere(ix)).ToArray()); } }
        /// <summary>
        /// Comma-delimited list of column names only including or excluding the specified indexes
        /// i.e. ColA, ColC
        /// </summary>
        /// <param name="inc">Include the specified indexes</param>
        /// <param name="ixs">Indexes to include or exclude</param>
        public string Selects(bool exclude, params int[] ixs) { return ixs?.Any() != true ? Select : Delimit(CollectionEx.Sequence(Cols?.Length ?? 0, exclude: exclude, ixs: ixs).Select(z => SelectCol(Cols[z]))); }
        /// <summary>
        /// Insert statement names and SQL string for values
        /// !!! WARNING - Do not use this to build queries - since it is not parameterized, sql injection is possible !!!
        /// i.e. (ColA, ColB, ColC) VALUES (1234, '1/1/2019', 'Malicious SQL Injection Attack...')
        /// </summary>
        public string Insert { get { int ix = -1; return InsertStr(Pairs?.Where(z => IsUpsert(++ix) || IsWhere(ix)).ToArray()); } }
        /// <summary>
        /// Comma-delimited list of column/value pairs useful for update set lists
        /// !!! WARNING - Do not use this to build queries - since it is not parameterized, sql injection is possible !!!
        /// i.e. ColA = 1234, ColB = '1/1/2019', ColC = 'Malicious SQL Injection Attack...'
        /// </summary>
        public string Update { get { int ix = -1; return UpdateStr(Pairs?.Where(z => IsUpsert(++ix)).ToArray()); } }
        /// <summary>
        /// AND-delimited list of tests between target and source columns
        /// i.e. sV.ColA = tV.ColA AND sV.ColB = tV.ColB AND (sV.ColC = tV.ColC OR (sV.ColC is null and tV.ColC is null))
        /// </summary>
        public string Where { get { return WhereStr(NameTarget, NameSource, PairWhere); } }
        /// <summary>
        /// Target vs. Source column test where target and source table and column names are provided
        /// If no source column is included, assumed to be the same name in both the target and the source
        /// i.e.
        /// sV.ColA = tV.ColA
        /// or if null test is ok then 
        /// (sV.ColA = tV.ColA OR (sV.ColA is null and tV.ColA is null))
        /// </summary>
        public string WhereMatch(string tcol, string scol = null, bool nullok = false) { return WhereStr(NameTarget, NameSource, tcol, scol, nullok); }
        /// <summary>
        /// Comma-delimited list of columns to output
        /// i.e. ColA, ColB, ColC
        /// </summary>
        public string Output { get { return JoinStr(PairOutput?.Where(z => z.Value).Select(z => z.Key).ToArray()); } }
        /// <summary>
        /// Comma-delimited list of output columns declared as corellating column type
        /// i.e. ColA [int], ColB [datetime] NOT NULL, ColC [nvarchar](max)
        /// </summary>
        public string OutputDeclare { get { return JoinStr(PairDeclare?.Where(z => z.Value).Select(z => z.Key).ToArray()); } }

        /// <summary>
        /// Comma-delimited list of blob data variables
        /// i.e. V.GetInt32(0), V.GetDateTime(1), V.GetString(2)
        /// </summary>
        public string SelectVar { get { int ix = -1; return JoinStr(_Map?.Select(z => SelVar(z, ++ix)).ToArray()); } }
        /// <summary>
        /// Comma-delimited list of blob data variables only including or excluding the specified indexes
        /// i.e. V.GetInt32(0), V.GetString(2)
        /// </summary>
        /// <param name="inc">Include the specified indexes</param>
        /// <param name="ixs">Indexes to include or exclude</param>
        public string SelectVars(bool exclude, params int[] ixs) { return ixs?.Any() != true ? SelectVar : JoinStr(CollectionEx.Sequence(_Map?.Length ?? 0, exclude: exclude, ixs: ixs).Select(z => SelVar(_Map[z], z)).ToArray()); }
        public string SelVar(SQLDBT z, int ix) { return string.Concat(Name, '.', DataVar(z), '(', ix, ')'); }

        /// <summary>
        /// Comma-delimited list of blob data variables
        /// i.e. V.GetInt32(0) AS ColA, V.GetDateTime(1) AS ColB, V.GetString(2) AS ColC
        /// </summary>
        public string SelectVarAs { get { int ix = -1, len = Cols?.Length ?? 0; return JoinStr(_Map?.Select(z => SelVarAs(z, ++ix)).ToArray()); } }
        public string SelectVarsAs(bool exclude, params int[] ixs) { return ixs?.Any() != true ? SelectVarAs : JoinStr(CollectionEx.Sequence(_Map?.Length ?? 0, exclude: exclude, ixs: ixs).Select(z => SelVarAs(_Map[z], z)).ToArray()); }
        public string SelVarAs(SQLDBT z, int ix) { return string.Concat(SelVar(z, ix), " AS \"", (ix < (Cols?.Length ?? 0) ? StringEx.UserText(Cols[ix]) : null) ?? DefSource(ix), '\"'); }
        public string DefSource(int ix) { return $"Col{ix}"; }

        /// <summary>
        /// Insert statement names and blob data variables
        /// i.e. (ColA, ColB, ColC) VALUES (V.GetInt32(0), V.GetDateTime(1), V.GetString(2))
        /// </summary>
        public string InsertVar { get { return InsertStr(PairsVar); } }
        /// <summary>
        /// Comma-delimited list of column/value pairs useful for update set lists
        /// i.e. ColA = V.GetInt32(0), ColB = V.GetDateTime(1), ColC = V.GetString(2)
        /// </summary>
        public string UpdateVar { get { return UpdateStr(PairsVar); } }

        /// <summary>
        /// Source clause for reading the blob as a pk and dbo.BlobIO object which can then be queried for specific column values
        /// i.e. (SELECT id as pkVar, dbo.BlobIO::FromIO(blob) as V FROM dbo.ToBlobs(@V) sV) AS sV
        /// Note that dbo.ToBlobs turns the bytes into a dbo.BlobList which has an id and blob column of int and byte[] types
        /// The blob column is then cast as a dbo.BlobIO for easy retrieval of variable column values
        /// </summary>
        public string FromSource(string pkvar = null, string blobvar = null, string alias = null) { return FromBlobs(Name, alias ?? NameSource, pkvar, blobvar); }
        public static string FromBlobs(string name = null, string alias = null, string pkvar = null, string blobvar = null) {
            name = StringEx.UserText(name) ?? c_name;
            alias = StringEx.UserText(alias) ?? c_sourceprefix + c_name;
            pkvar = StringEx.UserText(pkvar?.Trim('@'));
            blobvar = StringEx.UserText(blobvar?.Trim('@')) ?? name;
            return $@" FROM (
    SELECT {(pkvar == null ? "id" : $"isnull(@{pkvar},id)")} as ""{pkvar ?? "PK"}"", dbo.BlobIO::FromIO(blob) as {name} 
    FROM dbo.ToBlobs(@{blobvar})
) {alias}";
        }
        /// <summary>
        /// Source clause for reading the blob as a pk and dbo.BlobIO object from a BlobList
        /// i.e. FROM @sV CROSS APPLY (select dbo.BlobIO::FromIO(blob) as sV) as v
        /// </summary>
        public string FromList(string blobvar = null, string alias = null) {
            blobvar = StringEx.UserText(blobvar?.Trim('@')) ?? Name;
            return $@" FROM @{blobvar} CROSS APPLY (select dbo.BlobIO::FromIO(blob) as {Name}) {alias ?? "v"}";
        }
        /// <summary>
        /// Comma-delimited list of qualified source column names usedful in merge select blocks
        /// i.e. sV.ColA, sV.ColB, sV.ColC
        /// </summary>
        public string SelectSource { get { int ix = -1; return JoinStr(Cols?.Select(z => SelSource(z, ++ix)).ToArray()); } }
        /// <summary>
        /// Comma-delimited list of blob data variables only including or excluding the specified indexes
        /// i.e. V.GetInt32(0), V.GetString(2)
        /// </summary>
        /// <param name="inc">Include the specified indexes</param>
        /// <param name="ixs">Indexes to include or exclude</param>
        public string SelectSources(bool exclude, params int[] ixs) { return ixs?.Any() != true ? Select : Delimit(CollectionEx.Sequence(Cols?.Length ?? 0, exclude: exclude, ixs: ixs).Select(z => SelSource(Cols[z], z))); }
        public string SelSource(string z, int ix) { return SelectCol(z, DefSource(ix))?.Surround(NameSource, null, "."); }
        public string UpsertSource { get { int ix = -1; return JoinStr(Cols?.Select(z => IsUpsert(++ix) || IsWhere(ix) ? SelSource(z, ix) : null).NotNull().ToArray()); } }
        /// <summary>
        /// Insert statement names and blob data variables usedful in merge insert blocks
        /// i.e. (ColA, ColB, ColC) VALUES (sV.ColA, sV.ColB, sV.ColC)
        /// </summary>
        public string InsertSource { get { int ix = -1; return InsertStr(PairsSource?.Where(z => IsUpsert(++ix) || IsWhere(ix)).ToArray()); } }
        /// <summary>
        /// Comma-delimited list of column/value pairs useful for update set lists
        /// i.e. ColA = sV.ColA, ColB = sV.ColB, ColC = sV.ColC
        /// </summary>
        public string UpdateSource { get { int ix = -1; return UpdateStr(PairsSource?.Where(z => IsUpsert(++ix)).ToArray()); } }
        /// <summary>
        /// Comma-delimited list of columns to output from target
        /// i.e. COALESCE(INSERTED.ColA, DELETED.ColA), COALESCE(INSERTED.ColB, DELETED.ColB), COALESCE(INSERTED.ColC, DELETED.ColC)
        /// </summary>
        public string OutputCoalesce { get { return CoalesceStr(PairOutput?.Where(z => z.Value).Select(z => z.Key).ToArray()); } }

        public bool IsValid { get { return Value?.Length >= c_minlen || (Import() && IOOK && _Data?.Length == Length); } }
        public bool IsEmpty { get { return (Map?.Length ?? 0) <= 0; } }

        /// <summary>
        /// Fill the data from an external blob starting at a specified index
        /// </summary>
        public bool Fill(ref int ix, byte[] io) { return Import(ref ix, Value, prefix: false, map: false, data: true, cols: false, tbl: false, pk: false, ixs: false); }
        /// <summary>
        /// Fill the data from an external blob starting at a specified index and assuming there will be a 4 byte pk preceeding the data
        /// </summary>
        public bool FillPK(ref int ix, byte[] io) { return Import(ref ix, Value, prefix: false, map: false, data: true, cols: false, tbl: false, pk: true, ixs: false); }
        public bool Import(byte[] io) {
            _Map = null; _Data = null; _Cols = null; _Tbl = null; _Ixs = null;
            _PK = Count = -1;
            int ix = 0;
            if (!Import(ref ix, io, prefix: true, map: true, data: true, cols: true, tbl: true, pk: true, ixs: true)) return false;
            if (Ins?.Any() != true) InsOn = true.Fill(Length);
            bool swap = BinaryEx.NeedSwap(c_usebigendian);
            int len = io?.Length ?? 0;
            Count = (ix + sizeof(short) >= len) ? 0 : IOEx.GetInt16(io, ref ix, swap) + c_multioff; // count of additional rows - 1
            return true;
        }
        private bool Import() { int ix = 0; return Import(ref ix, Value, prefix: true, map: true, data: true, cols: true, tbl: true, pk: true, ixs: false); }
        /// <summary>
        /// 1 byte for read version
        /// 1 byte for write version
        /// 4 byte pk if IncPK
        /// byte array of type enums (short length + 1 byte per length)
        /// bool + binary IO per data type
        /// byte len + blob tbl string
        /// byte len + column tbl string
        /// </summary>
        private bool Import(ref int ix, byte[] io, bool prefix, bool map, bool data, bool cols, bool tbl, bool pk, bool ixs, StringBuilder debug = null) {
            if (map && _Map != null) return true;
            int len = io?.Length ?? 0;
            if (debug != null) debug.AppendLine($"Length: {len}");
            int min = 0;
            if (prefix) min += sizeof(byte) + sizeof(byte);
            if (map) min += sizeof(short);
            if (debug != null) debug.AppendLine($"Min: {min}");
            if (len < min) return false;
            bool swap = BinaryEx.NeedSwap(c_usebigendian);
            if (debug != null) debug.AppendLine($"Swap: {swap}");
            byte r = _Read, w = _Write;
            if (prefix || _VER == null) _VER = new VersionIO(r, w, false);
            if (debug != null) debug.AppendLine(_VER.ToString());
            switch (_VER.Peek(io, 0)) {
                default: return false;
                case IOREAD:
                case IOREADPK:
                    if (prefix) {
                        r = _VER.GetRead(io, ref ix); // 1 byte for read version
                        w = _VER.GetWrite(io, ref ix); // 1 byte for write version
                    }
                    if (debug != null) debug.AppendLine($"{ix} Read/Write: {r}/{w}");
                    int n = (prefix ? _IncPK : pk) ? IOEx.GetInt32(io, ref ix, swap) : 0;
                    if (pk) _PK = n; // 4 byte for pk
                    if (debug != null) debug.AppendLine($"{ix} PK: {n} {pk}");
                    if (map) MapIO = IOEx.GetBufferShort(io, ref ix, swap); // byte array of type enums (short length + 1 byte per length)
                    n = Length;
                    if (debug != null) debug.AppendLine($"{ix} Map: {NumberEx.ToServerHex(MapIO) ?? NULL} Length: {n} {map}");
                    if (data) { // bool + binary IO per data type
                        _Data = new object[n];
                        for (int i = 0; i < n; i++) {
                            if (ix >= len) break;
                            if (IOEx.GetBool(io, ref ix)) continue;
                            SQLDBT t = _Map[i];
                            if (debug != null) debug.AppendLine($"{ix} {t}: {NumberEx.ToServerHex(MapIO) ?? NULL} Length: {n} {map}");
                            switch (t) {
                                case SQLDBT.Bit: _Data[i] = IOEx.GetBool(io, ref ix); break;
                                case SQLDBT.Decimal: _Data[i] = IOEx.GetDecimal(io, ref ix, swap); break;
                                case SQLDBT.Real: _Data[i] = IOEx.GetSingle(io, ref ix, swap); break;
                                case SQLDBT.Float: _Data[i] = IOEx.GetDouble(io, ref ix, swap); break;
                                case SQLDBT.TinyInt: _Data[i] = IOEx.GetByte(io, ref ix); break;
                                case SQLDBT.SmallInt: _Data[i] = IOEx.GetInt16(io, ref ix, swap); break;
                                case SQLDBT.Int: _Data[i] = IOEx.GetInt32(io, ref ix, swap); break;
                                case SQLDBT.BigInt: _Data[i] = IOEx.GetInt64(io, ref ix, swap); break;
                                case SQLDBT.SmallMoney:
                                case SQLDBT.Money: _Data[i] = IOEx.GetDecimal(io, ref ix, swap); break;
                                case SQLDBT.UniqueIdentifier: _Data[i] = IOEx.GetGuid(io, ref ix, swap); break;
                                case SQLDBT.Char:
                                case SQLDBT.NChar:
                                case SQLDBT.Xml:
                                case SQLDBT.Text:
                                case SQLDBT.NText:
                                case SQLDBT.VarChar:
                                case SQLDBT.NVarChar: _Data[i] = IOEx.GetString(io, ref ix, swap); break;
                                case SQLDBT.SmallDateTime:
                                case SQLDBT.Date:
                                case SQLDBT.Timestamp:
                                case SQLDBT.DateTime2:
                                case SQLDBT.DateTime: _Data[i] = IOEx.GetDateTime(io, ref ix, swap); break;
                                case SQLDBT.DateTimeOffset: _Data[i] = IOEx.GetDateTimeOffset(io, ref ix, swap); break;
                                case SQLDBT.Time: _Data[i] = IOEx.GetTimeSpan(io, ref ix, swap); break;
                                case SQLDBT.Variant:
                                case SQLDBT.Udt:
                                case SQLDBT.Structured:
                                case SQLDBT.VarBinary:
                                case SQLDBT.Image:
                                case SQLDBT.Binary: _Data[i] = IOEx.GetBuffer(io, ref ix, swap); break;
                            }
                            if (debug != null) debug.AppendLine($"{ix} {t}: {_Data[i]?.ToString() ?? NULL}");
                        }
                    }
                    if (tbl) _Tbl = ix < len && w >= IOWRITETBL ? StringEx.UserText(IOEx.GetStringTiny(io, ref ix)) : null; // byte len + blob tbl string
                    if (debug != null) debug.AppendLine($"{ix} Table: {_Tbl ?? NULL} {tbl}");
                    if (cols) { // byte len + column tbl string
                        int clen = ix < len && w >= IOWRITECOLS ? IOEx.GetInt16(io, ref ix, swap) : 0;
                        _Cols = clen > 0 ? new string[clen] : null;
                        for (int i = 0; i < clen; i++) {
                            if (ix >= len) break;
                            _Cols[i] = StringEx.UserText(IOEx.GetStringTiny(io, ref ix));
                        }
                        if (debug != null) debug.AppendLine($"{ix} Columns: {clen} {StringEx.ToQueryNoQuotes(_Cols ?? StringEx.Empty) ?? NULL} {cols}");
                    }
                    if (ixs) IndexesIO = ix < len && w >= IOWRITEIXS ? IOEx.GetBufferShort(io, ref ix, swap) : null; // byte array of index bits (short length + 1 byte per length)
                    if (debug != null) debug.AppendLine($"{ix} Indexes: {NumberEx.ToServerHex(IndexesIO) ?? NULL} {ixs}");
                    break;
            }
            Value = null;
            return true;
        }
        /// <summary>
        /// 1 byte for read version
        /// 1 byte for write version
        /// 4 byte pk for pk
        /// byte array of type enums (short length + 1 byte per length)
        /// bool + binary IO per data type
        /// byte len + blob tbl string
        /// byte len + column tbl string
        /// </summary>
        private byte[] Export(bool prefix, bool map, bool data, bool cols, bool tbl, bool pk, bool ixs, ref int seed) {
            Import();
            bool swap = BinaryEx.NeedSwap(c_usebigendian);
            VersionIO v = _VER ?? new VersionIO(_Read, _Write, true);
            byte r = Read(v, pk ? _PK : (int?)null, change: true);
            byte w = Write(v, tbl: tbl, cols: cols, ixs: ixs, change: true);
            using (MemoryStream ms = new MemoryStream()) {
                switch (r) { // DB may serve apps with different versions, so need to be able to write compatible output for the version known to the app
                    default: return null;
                    case IOREAD:
                    case IOREADPK:
                        if (prefix) {
                            ms.WriteByte(r); // 1 byte for read version
                            ms.WriteByte(w); // 1 byte for write version
                        }
                        if (pk) IOEx.PutInt32(ms, PK ?? --seed, swap); // 4 byte pk
                        if (map) {
                            byte[] m = MapIO;
                            short mlen = (short)(m?.Length ?? 0);
                            IOEx.Put(ms, mlen, swap); // short length
                            if (mlen > 0) IOEx.PutBuffer(ms, false, m, swap); // byte array of type enums
                        }
                        int n = Length;
                        if (data) { // bool + binary IO per data type
                            int dlen = _Data?.Length ?? 0;
                            for (int i = 0, len = n; i < len; i++) {
                                object val = dlen > i ? _Data[i] : null;
                                IOEx.Put(ms, val == null, swap);
                                if (val == null) continue;
                                IOEx.Put(ms, val, swap);
                            }
                        }
                        if (tbl) IOEx.PutTiny(ms, _Tbl); // byte len + blob tbl string
                        if (cols) { // byte len + column tbl string
                            int clen = _Cols?.Any(z => z?.Length > 0) == true ? Math.Min(n, _Cols.Length) : 0;
                            IOEx.Put(ms, (short)clen, swap);
                            for (int i = 0; i < clen; i++)
                                IOEx.PutTiny(ms, _Cols[i]);
                        }
                        if (ixs) {
                            byte[] x = IndexesIO;
                            short xlen = (short)(x?.Length ?? 0);
                            IOEx.Put(ms, xlen, swap); // short length
                            if (xlen > 0) IOEx.PutBuffer(ms, false, x, swap); // byte array of type enums
                        }
                        return ms.ToArray();
                }
            }
        }
        private byte[] Export(bool prefix, bool map, bool data, bool cols, bool tbl, bool pk, bool ixs) { int seed = 0; return Export(prefix: prefix, map: map, data: data, cols: cols, tbl: tbl, pk: pk, ixs: ixs, seed: ref seed); }

        public byte[] Export(bool incpk, ref int seed) { return Export(prefix: true, map: true, data: true, cols: true, tbl: true, pk: incpk, ixs: true, seed: ref seed); }

        private string[] DataToSql(bool paramaterize = true) {
            Import();
            int len = Math.Min(Length, _Data?.Length ?? 0);
            string[] ret = new string[len];
            for (int i = 0; i < len; i++)
                ret[i] = DataToSql(_Map[i], _Data[i], paramaterize);
            return ret;
        }
        private static string DataToSql(SQLDBT t, object v, bool paramaterize = true) {
            switch (t) {
                default: return null;
                case SQLDBT.Bit: bool? b = NumberEx.GetBool(v); return b == true ? "1" : b == false ? "0" : NULL;
                case SQLDBT.Decimal: return NumberEx.GetDecimal(v)?.ToString() ?? NULL;
                case SQLDBT.Real: return NumberEx.Get(v)?.ToString() ?? NULL;
                case SQLDBT.Float: return NumberEx.GetFloat(v)?.ToString() ?? NULL;
                case SQLDBT.TinyInt: return NumberEx.GetByte(v)?.ToString() ?? NULL;
                case SQLDBT.SmallInt: return NumberEx.GetShort(v)?.ToString() ?? NULL;
                case SQLDBT.Int: return NumberEx.GetInt(v)?.ToString() ?? NULL;
                case SQLDBT.BigInt: return NumberEx.GetLong(v)?.ToString() ?? NULL;
                case SQLDBT.SmallMoney:
                case SQLDBT.Money: return NumberEx.GetDecimal(v)?.ToString() ?? NULL;
                case SQLDBT.UniqueIdentifier: return v == null ? NULL : string.Concat('\'', StringEx.ToGuid(v), '\'');
                case SQLDBT.Char:
                case SQLDBT.NChar:
                case SQLDBT.Xml:
                case SQLDBT.Text:
                case SQLDBT.NText:
                case SQLDBT.VarChar:
                case SQLDBT.NVarChar: return v == null ? NULL : paramaterize ? Encoding.Unicode.GetBytes(v as string ?? v.ToString())?.ToServerHex() ?? NULL : string.Concat('\'', v, '\''); // this could be unsafe and is not escaped, so either use the Ver version or convert the string to unicode binary to avoid quotes
                case SQLDBT.SmallDateTime:
                case SQLDBT.Date:
                case SQLDBT.Timestamp:
                case SQLDBT.DateTime2:
                case SQLDBT.DateTime: DateTime? dt = DateTimeEx.GetDateTime(v)?.AsLogical(); return dt == null ? NULL : string.Concat('\'', t == SQLDBT.Date ? dt?.ToString(DateTimeEx.c_iso8601_date) : dt?.ToString(DateTimeEx.c_iso8601_sitedatetime), '\''); // make the datetime unspecified kind to prevent conversions
                case SQLDBT.DateTimeOffset: DateTimeOffset? d = DateTimeEx.GetDate(v); return d == null ? NULL : string.Concat('\'', d?.ToString(DateTimeEx.c_datetimeroundtripoffset), '\'');
                case SQLDBT.Time: TimeSpan? ts = DateTimeEx.GetTime(v); return ts == null ? NULL : string.Concat('\'', ts?.ToString(DateTimeEx.c_iso8601_sitetime), '\'');
                case SQLDBT.Variant:
                case SQLDBT.Udt:
                case SQLDBT.Structured:
                case SQLDBT.VarBinary:
                case SQLDBT.Image:
                case SQLDBT.Binary: byte[] blob = v as byte[]; return blob == null ? NULL : blob.ToServerHex() ?? NULL;
            }
        }
        public static string DataVar(SQLDBT t, bool set = false) {
            switch (t) {
                case SQLDBT.Bit: return $"{(set ? "Set" : "Get")}Bool";
                case SQLDBT.Decimal: return $"{(set ? "Set" : "Get")}Decimal";
                case SQLDBT.Real: return $"{(set ? "Set" : "Get")}Single";
                case SQLDBT.Float: return $"{(set ? "Set" : "Get")}Double";
                case SQLDBT.TinyInt: return $"{(set ? "Set" : "Get")}Byte";
                case SQLDBT.SmallInt: return $"{(set ? "Set" : "Get")}Int16";
                case SQLDBT.Int: return $"{(set ? "Set" : "Get")}Int32";
                case SQLDBT.BigInt: return $"{(set ? "Set" : "Get")}Int64";
                case SQLDBT.SmallMoney:
                case SQLDBT.Money: return $"{(set ? "Set" : "Get")}Decimal";
                case SQLDBT.UniqueIdentifier: return $"{(set ? "Set" : "Get")}Guid";
                case SQLDBT.Char:
                case SQLDBT.NChar:
                case SQLDBT.Xml:
                case SQLDBT.Text:
                case SQLDBT.NText:
                case SQLDBT.VarChar:
                case SQLDBT.NVarChar: return $"{(set ? "Set" : "Get")}String";
                case SQLDBT.SmallDateTime:
                case SQLDBT.Date:
                case SQLDBT.Timestamp:
                case SQLDBT.DateTime2:
                case SQLDBT.DateTime: return $"{(set ? "Set" : "Get")}DateTime";
                case SQLDBT.DateTimeOffset: return $"{(set ? "Set" : "Get")}DateTimeOffset";
                case SQLDBT.Time: return $"{(set ? "Set" : "Get")}TimeSpan";
                case SQLDBT.Variant:
                case SQLDBT.Udt:
                case SQLDBT.Structured:
                case SQLDBT.VarBinary:
                case SQLDBT.Image:
                case SQLDBT.Binary: return $"{(set ? "Set" : "Get")}Bytes";
                default: return null;
            }
        }
        public static string DataDeclare(SQLDBT t, bool nullok = false) {
            switch (t) {
                case SQLDBT.Image: t = SQLDBT.VarBinary; goto case SQLDBT.VarBinary;
                case SQLDBT.Text: t = SQLDBT.VarChar; goto case SQLDBT.VarChar;
                case SQLDBT.NText: t = SQLDBT.NVarChar; goto case SQLDBT.NVarChar;
                case SQLDBT.Udt:
                case SQLDBT.Structured: t = SQLDBT.VarBinary; goto case SQLDBT.VarBinary;
                case SQLDBT.Time:
                case SQLDBT.Variant:
                case SQLDBT.Xml:
                case SQLDBT.Bit:
                case SQLDBT.Decimal:
                case SQLDBT.Real:
                case SQLDBT.Float:
                case SQLDBT.TinyInt:
                case SQLDBT.SmallInt:
                case SQLDBT.Int:
                case SQLDBT.BigInt:
                case SQLDBT.SmallMoney:
                case SQLDBT.Money:
                case SQLDBT.UniqueIdentifier:
                case SQLDBT.SmallDateTime:
                case SQLDBT.Date:
                case SQLDBT.Timestamp:
                case SQLDBT.DateTime:
                case SQLDBT.DateTime2:
                case SQLDBT.DateTimeOffset: return $"{t.ToString().ToLower()}{(nullok ? string.Empty : " NOT NULL")}";
                case SQLDBT.Char:
                case SQLDBT.NChar:
                case SQLDBT.VarChar:
                case SQLDBT.NVarChar:
                case SQLDBT.VarBinary:
                case SQLDBT.Binary: return $"{t.ToString().ToLower()}(max){(nullok ? string.Empty : " NOT NULL")}";
                default: return null;
            }
        }

        /// <summary>
        /// Delimit a column list with the provided separator or a comma by default
        /// [a, b, c] -> "a, b, c"
        /// </summary>
        /// <param name="cols">List of column names</param>
        /// <param name="sep">Separator or a comma by default</param>
        /// <returns>Delimited list of column names</returns>
        public static string Delimit(IEnumerable<string> cols, string sep = null) => cols?.Any() == true ? StringEx.UserText(string.Join(sep ?? ", ", cols)) : null;
        /// <summary>
        /// Comma-delimited list of provided columns useful for select lists
        /// i.e. ColA, ColB, ColC
        /// [a, b, c] -> "a, b, c]
        /// </summary>
        /// <param name="cols">List of column names</param>
        /// <param name="sep">Separator or a comma by default</param>
        /// <returns>Delimited list of column names</returns>
        public static string JoinStr(params string[] cols) { return Delimit(cols?.Select(z => string.IsNullOrWhiteSpace(z) ? NULL : $"{z}")); }
        /// <summary>
        /// Comma-delimited list of provided columns useful for select lists
        /// i.e. ColA, ColB, ColC
        /// [a, b.IO, c] -> "a", "b".IO, "c"
        /// </summary>
        /// <param name="cols">List of column names</param>
        /// <param name="sep">Separator or a comma by default</param>
        /// <returns>Delimited list of column names</returns>
        public static string SelectStr(params string[] cols) { return Delimit(cols?.Select(z => SelectCol(z))); }
        public static string SelectCol(string col, string def = NULL) { if (string.IsNullOrWhiteSpace(col)) return def; int ix = col.IndexOf('.'); return ix == -1 ? $"\"{col}\"" : string.Concat('\"', col.Substring(0, ix), '\"', col.Substring(ix, col.Length - ix)); }
        /// <summary>
        /// Comma-delimited list of provided parameter names useful for select or update lists
        /// i.e. ColA, ColB, ColC
        /// [a, b, c] -> "@a, @b, @c"
        /// </summary>
        /// <param name="cols">List of column names</param>
        /// <param name="sep">Separator or a comma by default</param>
        /// <returns>Delimited list of column names</returns>
        public static string ParamStr(params string[] cols) { return Delimit(cols?.Select(z => ParamCol(z))); }
        public static string ParamCol(string col, string def = NULL) { return string.IsNullOrWhiteSpace(col) ? def : $"@{col}"; }
        /// <summary>
        /// Insert statement names and values
        /// i.e. (ColA, ColB, ColC) VALUES (ValA, ValB, ValC)
        /// </summary>
        public static string InsertStr(params KeyValuePair<string, string>[] pairs) { return pairs?.Any() == true ? string.Concat(" (", string.Join(StringEx.CommaSep, pairs?.Select(z => z.Key) ?? StringEx.Empty), ") VALUES (", string.Join(StringEx.CommaSep, pairs?.Select(z => z.Value) ?? StringEx.Empty), ")") : null; }
        /// <summary>
        /// Comma-delimited list of column/value pairs useful for update set lists
        /// i.e. ColA = ValA, ColB = ValB, ColC = ValC
        /// </summary>
        public static string UpdateStr(params KeyValuePair<string, string>[] pairs) { return Delimit(pairs?.Select(z => $"{z.Key} = {z.Value}") ?? StringEx.Empty); }
        /// <summary>
        /// AND-delimited list of column source and target tests useful for where, join and merge on lists
        /// Pairs provided can indicate true to also permit equality through both columns being null
        /// i.e. sV.ColA = tV.ColA AND sV.ColB = tV.ColB AND (sV.ColC = tV.ColC OR (sV.ColC is null and tV.ColC is null))
        /// </summary>
        public static string WhereStr(string t, string s, params KeyValuePair<string, bool>[] colnullok) { return StringEx.UserText(string.Join(" AND ", colnullok?.Select(z => WhereStr(t, s, z.Key, z.Key, z.Value)) ?? StringEx.Empty)); }
        /// <summary>
        /// Target vs. Source column test where target and source table and column names are provided
        /// If no source column is included, assumed to be the same name in both the target and the source
        /// i.e.
        /// sV.ColA = tV.ColA
        /// or if null test is ok then 
        /// (sV.ColA = tV.ColA OR (sV.ColA is null and tV.ColA is null))
        /// </summary>
        public static string WhereStr(string t, string s, string tcol, string scol = null, bool nullok = false) { return nullok ? $"({t}.{tcol} = {s}.{scol ?? tcol} OR ({t}.{tcol} is null AND {s}.{scol ?? tcol} is null))" : $"{t}.{tcol} = {s}.{scol ?? tcol}"; }
        /// <summary>
        /// Comma-delimited list of coalesced inserted or deleted columns for output clauses
        /// i.e. COALESCE(INSERTED.ColA, DELETED.ColA), COALESCE(INSERTED.ColB, DELETED.ColB), COALESCE(INSERTED.ColC, DELETED.ColC)
        /// </summary>
        public static string CoalesceStr(params string[] cols) { return Delimit(cols?.Select(z => $"COALESCE(INSERTED.{z}, DELETED.{z})") ?? StringEx.Empty); }
        /// <summary>
        /// Concatenated list of columns combined into a blob
        /// i.e. dbo.BlobIO::FromVar(ColA).Concat(ColB).Concat(ColC.IO)
        /// </summary>
        public static string BlobConcatStr(params string[] cols) { return string.Concat($"dbo.BlobIO::{nameof(Empty)}", string.Join(string.Empty, cols?.Select(z => z?.Length > 0 && z[0] == '.' ? z : z?.Surround($".{nameof(Concat)}(", ")")).NotNull() ?? StringEx.Empty)); }

        public BlobIO() { }
        public BlobIO(byte[] io) { IO = io; }
        public BlobIO(string str) { Str = str; }
        public BlobIO(byte[] map, object[] data, string[] cols = null, string tbl = null) { Init(map?.Select(z => z.FromByte(SQLDBT.Variant)).ToArray(), data, cols, tbl); }
        public BlobIO(SQLDBT[] map, object[] data = null, string[] cols = null, string tbl = null) { Init(map, data, cols, tbl); }
        public BlobIO(string[] map, object[] data = null, string[] cols = null, string tbl = null) { Init(map?.Select(z => z.FromString(SQLDBT.Variant, true)).ToArray(), data, cols, tbl); }
        public BlobIO(string map, object[] data = null, string[] cols = null, string tbl = null) { Init(map?.SplitParams(true)?.Select(z => z.FromString(SQLDBT.Variant, true)).ToArray(), data, cols, tbl); }
        private void Init(SQLDBT[] map, object[] data, string[] cols, string tbl) {
            int dlen = data?.Length ?? 0;
            int len = Math.Max(map?.Length ?? 0, dlen);
            //if (len > c_maxvals) throw new Exception($"Invalid index, value must be between 0 and {c_maxvals}");
            _Map = map ?? SQLDBT.Variant.Fill(len);
            _Data = new object[len];
            for (int i = dlen - 1; i >= 0; i--)
                Ensure(i, data[i] == null, data[i]);
            Cols = cols;
            Name = tbl;
            InsOn = true.Fill(len);
        }
        public override string ToString() { return Str; }
        public BlobIO Minified { get { return Dupe(true, true); } } // remove all but the data from the blob
        public BlobIO Dupe(bool min = false, bool shallow = false) { return !min ? new BlobIO(IO) : shallow ? new BlobIO(_Map, _Data) : new BlobIO(Export(prefix: true, map: true, data: true, cols: false, tbl: false, pk: false, ixs: false)); }
        public static BlobIO Schema(params byte[] map) { return new BlobIO(map); }
        public static BlobIO Schema(params SQLDBT[] map) { return new BlobIO(map); }
        public static BlobIO Data(params object[] data) { return new BlobIO((byte[])null, data); }
        public static BlobIO Data(BlobIO template, params object[] data) { return Data(template, false, false, data); }
        public static BlobIO Data(BlobIO template, bool min, bool shallow, params object[] data) {
            int dlen = data?.Length ?? 0;
            if (template == null) return new BlobIO((byte[])null, data);
            BlobIO ret = template.Dupe(min: min, shallow: shallow);
            for (int i = dlen - 1; i >= 0; i--)
                ret.Ensure(i, data[i] == null, data[i]);
            return ret;
        }
        public object[] GetData() { return Import() ? _Data : null; }

        public int IX(string name) { return Cols?.IndexOf(name, StringComparer.InvariantCultureIgnoreCase.Compare) ?? -1; }
        public T Get<T>(int ix) where T : class { Import(); return _Data?.Length > ix ? _Data[ix] as T : null; }
        public T? GetN<T>(int ix) where T : struct { Import(); object ret = _Data?.Length > ix ? _Data[ix] : null; return ret is T ? (T)_Data[ix] : (T?)null; }
        public bool Get<T>(int ix, out T ret, bool convert = false) {
            Import();
            object _ret = _Data?.Length > ix ? _Data[ix] : null;
            bool ok = _ret is T;
            ret = ok ? (T)_ret : default(T);
            return ok;
        }
        public object Get(int ix) {
            if (ix < 0 || ix >= Length) {
                object ret;
                return Get<object>(ix, out ret) ? ret : null;
            }
            switch (_Map[ix]) {
                case SQLDBT.Bit: return GetBool(ix);
                case SQLDBT.Decimal: return GetDecimal(ix);
                case SQLDBT.Real: return GetSingle(ix);
                case SQLDBT.Float: return GetDouble(ix);
                case SQLDBT.TinyInt: return GetByte(ix);
                case SQLDBT.SmallInt: return GetInt16(ix);
                case SQLDBT.Int: return GetInt32(ix);
                case SQLDBT.BigInt: return GetInt64(ix);
                case SQLDBT.SmallMoney:
                case SQLDBT.Money: return GetDecimal(ix);
                case SQLDBT.UniqueIdentifier: return GetGuid(ix);
                case SQLDBT.Char:
                case SQLDBT.NChar:
                case SQLDBT.Xml:
                case SQLDBT.Text:
                case SQLDBT.NText:
                case SQLDBT.VarChar:
                case SQLDBT.NVarChar: return GetString(ix);
                case SQLDBT.SmallDateTime:
                case SQLDBT.Date:
                case SQLDBT.Timestamp:
                case SQLDBT.DateTime2:
                case SQLDBT.DateTime: return GetDateTime(ix);
                case SQLDBT.DateTimeOffset: return GetDateTimeOffset(ix);
                case SQLDBT.Time: return GetTime(ix);
                case SQLDBT.Variant:
                case SQLDBT.Udt:
                case SQLDBT.Structured:
                case SQLDBT.VarBinary:
                case SQLDBT.Image:
                case SQLDBT.Binary: return GetBytes(ix);
                default: object ret; return Get<object>(ix, out ret) ? ret : null;
            }
        }

        public bool Set(int ix, BlobIO v) { return v == null ? Clear(ix, v?.Length ?? 0) : _Set(ix, v, v?.Length ?? 0); }
        public bool Set<T>(int ix, bool clear, T v) { return clear ? Clear(ix) : _Set(ix, v); }
        public bool Set<T>(int ix, T v) { return v == null ? Clear(ix) : _Set(ix, v); }
        public bool Set<T>(int ix, T? v) where T : struct { return !v.HasValue ? Clear(ix) : _Set(ix, v); }

        public bool Concat(BlobIO v) { return _Ensure(Length, v); }
        public bool Concat<T>(bool clear, T v) { return Ensure(Length, clear, v); }
        public bool Concat<T>(T v) { return Ensure(Length, v == null, v); }
        public bool Concat<T>(T? v) where T : struct { return Ensure(Length, !v.HasValue, v); }

        public bool Ensure<T>(int ix, bool clear, T v) { return _Ensure(ix, clear, v); }
        public bool Ensure<T>(int ix, T v) { return _Ensure(ix, v == null, v); }
        public bool Ensure<T>(int ix, T? v) where T : struct { return _Ensure(ix, !v.HasValue, v); }

        public static bool CanIO(ref object obj, out SQLDBT dbt, Type def = null) {
            IOEx.TypeCodeEx t = IOEx.TypeCodeEx.Nullable;
            switch (obj != null ? IOEx.GetTypeCode(ref obj, out t) : def != null ? IOEx.GetTypeCode(def) : TypeCode.Empty) {
                case TypeCode.Empty: dbt = SQLDBT.Variant; return false;
                case TypeCode.Boolean: dbt = SQLDBT.Bit; return true;
                case TypeCode.Char: dbt = SQLDBT.NChar; return true;
                case TypeCode.SByte: dbt = SQLDBT.TinyInt; return true;
                case TypeCode.Byte: dbt = SQLDBT.TinyInt; return true;
                case TypeCode.Int16: dbt = SQLDBT.SmallInt; return true;
                case TypeCode.UInt16: dbt = SQLDBT.SmallInt; return true;
                case TypeCode.Int32: dbt = SQLDBT.Int; return true;
                case TypeCode.UInt32: dbt = SQLDBT.Int; return true;
                case TypeCode.Int64: dbt = SQLDBT.BigInt; return true;
                case TypeCode.UInt64: dbt = SQLDBT.BigInt; return true;
                case TypeCode.Single: dbt = SQLDBT.Real; return true;
                case TypeCode.Double: dbt = SQLDBT.Float; return true;
                case TypeCode.Decimal: dbt = SQLDBT.Decimal; return true;
                case TypeCode.String: dbt = SQLDBT.NVarChar; return true;
                case TypeCode.DateTime: dbt = SQLDBT.DateTime2; return true;
                case TypeCode.Object:
                default:
                    switch (t) {
                        case IOEx.TypeCodeEx.ByteArray: dbt = SQLDBT.VarBinary; return true;
                        case IOEx.TypeCodeEx.DateTimeOffset:
                        case IOEx.TypeCodeEx.NullableDateTimeOffset: dbt = SQLDBT.DateTimeOffset; return true;
                        case IOEx.TypeCodeEx.NullableDateTime: dbt = SQLDBT.DateTime2; return true;
                        case IOEx.TypeCodeEx.TimeSpan:
                        case IOEx.TypeCodeEx.NullableTimeSpan: dbt = SQLDBT.Time; return true;
                        case IOEx.TypeCodeEx.DAKey:
                        case IOEx.TypeCodeEx.DAKeyArray:
                        case IOEx.TypeCodeEx.DAKeyVirtual:
                        case IOEx.TypeCodeEx.DAKeyVirtualArray: dbt = SQLDBT.VarBinary; return true;
                        case IOEx.TypeCodeEx.Guid: dbt = SQLDBT.UniqueIdentifier; return true;
                        default: dbt = SQLDBT.Variant; return false;
                    }
            }
        }
        public bool _Ensure(int ix, BlobIO v) {
            Import();
            if (ix < 0) return false;
            int vlen = v?.Length ?? 0;
            int vdlen = v?._Data?.Length ?? 0;
            int len = Length;
            if (_Map == null || len < ix + vlen) {
                int max = len + c_maxinc;
                if (ix > max) throw new Exception($"Invalid index, value must be between 0 and {max}");
                IXResize(ix + vlen);
                SQLDBT[] map = SQLDBT.Variant.Fill(ix + vlen);
                if (_Map?.Any() == true) Array.Copy(_Map, 0, map, 0, len);
                _Map = map;
            }
            if (vlen > 0) Array.Copy(v._Map, 0, _Map, ix, vlen);
            int dlen = _Data?.Length ?? 0;
            if (dlen < Length) {
                object[] data = new object[Length];
                if (_Data?.Any() == true) Array.Copy(_Data, 0, data, 0, dlen);
                _Data = data;
            }
            if (vdlen < vlen) return false;
            if (v == null) return Clear(ix, vlen);
            return _Set(ix, v, vlen);
        }
        private bool _Ensure<T>(int ix, bool clear, T v) {
            Import();
            if (ix < 0) return false;
            object obj = v;
            SQLDBT dbt;
            if (!CanIO(ref obj, out dbt, typeof(T)) && (dbt != SQLDBT.Variant || v != null)) return false;
            int len = Length;
            if (_Map == null || len <= ix) {
                int max = len + c_maxinc;
                if (ix > max) throw new Exception($"Invalid index, value must be between 0 and {max}");
                IXResize(ix + 1);
                SQLDBT[] map = SQLDBT.Variant.Fill(ix + 1);
                if (_Map?.Any() == true) Array.Copy(_Map, 0, map, 0, len);
                _Map = map;
            }
            if (obj is T || _Map[ix] == SQLDBT.Variant) _Map[ix] = dbt;
            int dlen = _Data?.Length ?? 0;
            if (dlen < Length) {
                object[] data = new object[Length];
                if (_Data?.Any() == true) Array.Copy(_Data, 0, data, 0, dlen);
                _Data = data;
            }
            if (obj is T) v = (T)obj;
            else if (obj == null || clear) return Clear(ix);
            return _Set(ix, v);
        }
        private bool _Set(int ix, BlobIO v, int len) {
            Import();
            if (ix < 0) return false;
            int vlen = v?.Length ?? 0;
            if (vlen < len) return false;
            if (ix + len > Length) return false;
            int dlen = _Data?.Length ?? 0;
            if (ix + len > dlen) return false;
            int vdlen = v?._Data?.Length ?? 0;
            if (vdlen < len || vdlen < len) return false;
            for (int i = 0; i < len; i++) {
                if (_Map[ix + i] == SQLDBT.Variant) _Map[ix + i] = v._Map[i];
                if (v._Map[i] != _Map[ix + i] && v._Map[i] != SQLDBT.Variant) return false;
            }
            if (len > 0) Array.Copy(v._Data, 0, _Data, ix, len);
            return true;
        }
        private bool _Set<T>(int ix, T v) {
            Import();
            if (ix < 0 || ix >= Length) return false;
            int dlen = _Data?.Length ?? 0;
            if (ix >= dlen) return false;
            bool yisnull = v == null;
            SQLDBT t = _Map[ix];
            switch (t) {
                case SQLDBT.Bit: _Data[ix] = NumberEx.GetBool(v); return yisnull == (_Data[ix] == null);
                case SQLDBT.Decimal: _Data[ix] = NumberEx.GetDecimal(v); return yisnull == (_Data[ix] == null);
                case SQLDBT.Real: float? f = NumberEx.GetFloat(v); _Data[ix] = f.IsValid() ? f : null; return yisnull == (_Data[ix] == null);
                case SQLDBT.Float: double? d = NumberEx.Get(v); _Data[ix] = d.IsValid() ? d : null; return yisnull == (_Data[ix] == null);
                case SQLDBT.TinyInt: _Data[ix] = NumberEx.GetByte(v); return yisnull == (_Data[ix] == null);
                case SQLDBT.SmallInt: _Data[ix] = NumberEx.GetShort(v); return yisnull == (_Data[ix] == null);
                case SQLDBT.Int: _Data[ix] = NumberEx.GetInt(v); return yisnull == (_Data[ix] == null);
                case SQLDBT.BigInt: _Data[ix] = NumberEx.GetLong(v); return yisnull == (_Data[ix] == null);
                case SQLDBT.SmallMoney:
                case SQLDBT.Money: _Data[ix] = NumberEx.GetDecimal(v); return yisnull == (_Data[ix] == null);
                case SQLDBT.UniqueIdentifier: Guid g; _Data[ix] = StringEx.ToGuid(v, out g) ? g : (Guid?)null; return yisnull == (_Data[ix] == null);
                case SQLDBT.Char:
                case SQLDBT.NChar:
                case SQLDBT.Xml:
                case SQLDBT.Text:
                case SQLDBT.NText:
                case SQLDBT.VarChar:
                case SQLDBT.NVarChar: _Data[ix] = v?.ToString(); return yisnull == (_Data[ix] == null);
                case SQLDBT.SmallDateTime:
                case SQLDBT.Date:
                case SQLDBT.Timestamp:
                case SQLDBT.DateTime2:
                case SQLDBT.DateTime: _Data[ix] = DateTimeEx.GetDateTime(v)?.AsLogical(); return yisnull == (_Data[ix] == null); // make the datetime unspecified kind to prevent conversions
                case SQLDBT.DateTimeOffset: _Data[ix] = DateTimeEx.GetDate(v); return yisnull == (_Data[ix] == null);
                case SQLDBT.Time: _Data[ix] = DateTimeEx.GetTime(v); return yisnull == (_Data[ix] == null);
                case SQLDBT.Variant:
                case SQLDBT.Udt:
                case SQLDBT.Structured:
                case SQLDBT.VarBinary:
                case SQLDBT.Image:
                case SQLDBT.Binary:
                    TypeCode code;
                    IOEx.TypeCodeEx typ;
                    _Data[ix] = v as byte[] ?? IOEx.ExportOne(v, out code, out typ, false);
                    return yisnull == (_Data[ix] == null);
                default: return false;
            }
        }
        public bool Clear(int ix, int len = 1) {
            Import();
            if (ix < 0 || len < 1) return false;
            int sz = _Data?.Length ?? 0;
            if (sz >= ix + len) {
                for (int i = ix; i < ix + len; i++)
                    _Data[i] = null;
                return true;
            } else return false;
        }

        public bool? GetBool(int ix, bool convert = false) { bool ret; return Get(ix, out ret, convert) ? ret : (bool?)null; }
        public byte? GetByte(int ix, bool convert = false) { byte ret; return Get(ix, out ret, convert) ? ret : (byte?)null; }
        public short? GetInt16(int ix, bool convert = false) { short ret; return Get(ix, out ret, convert) ? ret : (short?)null; }
        public int? GetInt32(int ix, bool convert = false) { int ret; return Get(ix, out ret, convert) ? ret : (int?)null; }
        public E GetEnum<E>(int ix, E def = default(E), bool convert = false) where E : struct, IConvertible, IComparable, IFormattable { return GetInt32(ix, convert)?.AsEnum<E>(def) ?? def; }
        public long? GetInt64(int ix, bool convert = false) { long ret; return Get(ix, out ret, convert) ? ret : (long?)null; }
        public float? GetSingle(int ix, bool convert = false) { float ret; return Get(ix, out ret, convert) ? ret : (float?)null; }
        public double? GetDouble(int ix, bool convert = false) { double ret; return Get(ix, out ret, convert) ? ret : (double?)null; }
        public decimal? GetDecimal(int ix, bool convert = false) { decimal ret; return Get(ix, out ret, convert) ? ret : (decimal?)null; }
        public Guid? GetGuid(int ix, bool convert = false) { Guid ret; return Get(ix, out ret, convert) ? ret : (Guid?)null; }
        public DateTime? GetDateTime(int ix, bool convert = false) { DateTime ret; return Get(ix, out ret, convert) ? ret : (DateTime?)null; }
        public DateTimeOffset? GetDateTimeOffset(int ix, bool convert = false) { DateTimeOffset ret; return Get(ix, out ret, convert) ? ret : (DateTimeOffset?)null; }
        public TimeSpan? GetTime(int ix, bool convert = false) { TimeSpan ret; return Get(ix, out ret, convert) ? ret : (TimeSpan?)null; }
        public string GetString(int ix, bool convert = false) { string ret; return Get(ix, out ret, convert) ? ret : null; }
        public byte[] GetBytes(int ix, bool convert = false) { byte[] ret; return Get(ix, out ret, convert) ? ret : null; }

        /// <summary>
        /// Troubleshoot serialized blob import by returning detected elements and indexes as the serialized bytes are imported
        /// </summary>
        /// <param name="io"></param>
        /// <returns></returns>
        public static string DebugImport(byte[] io) {
            if (io?.Any() != true) return NULL;
            StringBuilder ret = new StringBuilder();
            int ix = 0;
            try {
                BlobIO b = new BlobIO();
                if (!b.Import(ref ix, io, prefix: true, map: true, data: true, cols: true, tbl: true, pk: true, ixs: true, debug: ret) || b?.IsValid != true) throw new Exception("FAIL");
                ret.AppendLine("OK");
            } catch (Exception e) {
                ret.AppendLine($"Failed at index: {ix}");
                ret.AppendLine(e.Message);
                if (ix < io.Length) ret.AppendLine(NumberEx.ToServerHex(io.Skip(ix).ToArray()));
            }
            return ret.ToString();
        }
        /// <summary>
        /// Blob list iterator for handling lists of blobs passed to clr stored procedures
        /// The blob map and columns are expected to be identical for all rows and are therefore left out of all but the initial row
        /// When streaming the format expected is as follows (Note that a single BlobIO and list have the same beginning, and any list items are optional after the first blob definition):
        /// -BlobIO.IO of first row
        /// -2 bytes for count of additional rows (total length - 1)
        /// -(count - 1) * 4 bytes for index positions -  int array of data start indexes except the first one which is always 0 (the location in the blob where each new row starts - the current position, so the starting row should always be 0 and is therefore excluded)
        /// -BlobIO.DataIO only starting at zero and then each index position in the positions array
        /// </summary>
        public static IEnumerable<KeyValuePair<int, byte[]>> ToBlobList(byte[] io) {
            int len = io?.Length ?? 0;
            if (len <= 0) yield break;
            int ix = 0;
            bool swap = BinaryEx.NeedSwap(c_usebigendian);
            BlobIO b = new BlobIO();
            if (!b.Import(ref ix, io, prefix: true, map: true, data: true, cols: true, tbl: true, pk: true, ixs: true) || b?.IsValid != true) throw new Exception("Failed to read initial blob");
            int pk = 0; // default pk decrements starting at -1 when no pk is in the blob
            bool incpk = b._IncPK;
            int id = incpk ? b.PK ?? --pk : --pk; // assume that all blobs will either have or not have PKs
            byte[] prefix = b.PrefixIO;
            if (prefix?.Any() != true) throw new Exception("Initial blob was invalid");
            byte[] data = b.DataIO;
            byte[] val = data?.Any() == true ? new byte[prefix.Length + data.Length] : null;
            if (val != null) {
                Array.Copy(prefix, val, prefix.Length);
                Array.Copy(data, 0, val, prefix.Length, data.Length);
            }
            yield return new KeyValuePair<int, byte[]>(id, val);
            if (ix + sizeof(short) >= len) yield break; // no additional rows specified
            int count = IOEx.GetInt16(io, ref ix, swap); // count of additional rows - 1
            int pos = ix + (count * sizeof(int));
            if (pos >= len) throw new Exception($"Count exceeds mapped rows: {count} for index at {ix} of {len}");
            int x = pos;
            for (int i = 0; i <= count; i++) { // the first one is always zero, so not included in the io
                int y = i < count ? IOEx.GetInt32(io, ref ix, swap) + pos : len;
                if (y > len) throw new Exception($"Index position exceeds mapped rows: {y} from row map {i} for index at {ix} of {len}");
                if (x + (incpk ? sizeof(int) : 0) > y) continue; // this is a blank row without an id, so skip it                
                id = incpk ? IOEx.GetInt32(io, ref x, swap) : --pk; // assume that all blobs will either have or not have PKs
                val = x < y ? new byte[prefix.Length + y - x] : null;
                if (val != null) {
                    Array.Copy(prefix, val, prefix.Length);
                    Array.Copy(io, x, val, prefix.Length, y - x);
                }
                yield return new KeyValuePair<int, byte[]>(id, val);
                x = y;
            }
        }
        public static IEnumerable<KeyValuePair<int, BlobIO>> ToBlobs(byte[] io, bool min = false, bool shallow = false) {
            BlobIO first = null;
            foreach (var v in ToBlobList(io)) {
                BlobIO b = new BlobIO(v.Value);
                if (first == null && min) first = b = b.Minified;
                else if (first == null) first = b;
                else b = Data(first, min, shallow, b?.GetData());
                yield return new KeyValuePair<int, BlobIO>(v.Key, b);
            }
        }

        /// <summary>
        /// Blob list exporter for handling lists of blobs passed to clr stored procedures
        /// The blob map and columns are expected to be identical for all rows and are therefore left out of all but the initial row
        /// When streaming the format expected is as follows (Note that a single BlobIO and list have the same beginning, and any list items are optional after the first blob definition):
        /// -BlobIO.IO of first row
        /// -2 bytes for count of additional rows (total length - 1)
        /// -(count - 1) * 4 bytes for index positions -  int array of data start indexes except the first one which is always 0 (the location in the blob where each new row starts - the current position, so the starting row should always be 0 and is therefore excluded)
        /// -BlobIO.DataIO only starting at zero and then each index position in the positions array
        /// </summary>
        public static byte[] ExportBlobList(params BlobIO[] blobs) { return ExportBlobList(blobs?.Any(z => z?.PK != null) == true, blobs); }
        public static byte[] ExportBlobList(bool incpk, params BlobIO[] blobs) {
            int len = blobs?.Length ?? 0;
            if (len <= 0) return null;
            int seed = 0;
            if (len == 1) {
                BlobIO b = blobs[0];
                byte[] data = b?.IsValid == true ? b.Export(incpk, ref seed) : null;
                if (data?.Any() != true) throw new Exception("Failed to write initial blob");
                return data;
            }
            bool swap = BinaryEx.NeedSwap(c_usebigendian);
            byte r = incpk ? IOREADPK : IOREAD;
            int pos = -1, sz = 0;
            int[] ixs = null;
            using (MemoryStream ms = new MemoryStream()) {
                for (int i = 0; i < len; i++) {
                    BlobIO b = blobs[i];
                    if (b == null) throw new Exception("Blob cannot be null");
                    if (i == 0) {
                        if (b._VER?.HasRead == true) r = b._VER.Read;
                        switch (r) { // DB may serve apps with different versions, so need to be able to write compatible output for the version known to the app
                            default: throw new Exception("Unsupported blob io version");
                            case IOREAD:
                            case IOREADPK:
                                byte[] data = b?.IsValid == true ? b.Export(incpk, ref seed) : null;
                                if (data?.Any() != true) throw new Exception("Failed to write initial blob");
                                IOEx.PutBuffer(ms, false, data, swap); // put the whole first blob
                                ixs = new int[len - c_multioff]; // skip the first and second ones since they are obvious
                                IOEx.Put(ms, (short)ixs.Length, swap); // put the count of additional rows minus the first and second one
                                pos = (int)ms.Position; // record the location of the indexes
                                sz = ixs.Length * sizeof(int);
                                if (sz > 0) IOEx.PutBuffer(ms, false, new byte[sz], swap); // skip the index section till the end
                                continue;
                        }
                    }
                    switch (r) {
                        default: throw new Exception("Unsupported blob io version");
                        case IOREAD:
                        case IOREADPK:
                            int p = (int)ms.Position - pos - sz;
                            if (p < 0) throw new Exception("Unexpected io position during io export");
                            if (i >= c_multioff) ixs[i - c_multioff] = p;
                            if (incpk) IOEx.PutInt32(ms, b.PK ?? --seed, swap);
                            byte[] data = b.DataIO;
                            if (data?.Any() == true) IOEx.PutBuffer(ms, false, data, swap); // put the data only
                            break;
                    }
                }
                if (pos >= 0 && sz > 0) {
                    if (ms.Seek(pos, SeekOrigin.Begin) != pos) throw new Exception("Failed to seek to write indexes");
                    for (int i = 0, n = ixs.Length; i < n; i++)
                        IOEx.Put(ms, ixs[i], swap);
                    if (ms.Position != pos + sz) throw new Exception("Unexpected io position during io index export");
                    ms.Seek(0, SeekOrigin.End);
                }
                return ms.ToArray()?.DefaultIfEmpty(null);
            }
        }
    }
}
