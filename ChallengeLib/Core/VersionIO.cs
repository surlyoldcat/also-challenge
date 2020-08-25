using System;
using System.Linq;

namespace AE.CoreInterface
{
    /// <summary>
    /// Binary versioning class to assist with handling backwards and forwards compatibility of IOBinary serialization patterns
    /// 
    /// To transition from Read version pattern to Read + Write version pattern, the Write version is inserted after the last property that was part of the minimum version.
    /// Since that would be past the original length, application builds for the minimum version will just ignore the value.
    /// Builds from between the minimum version and the legacy version when this new pattern was introduced can use this method to try to get a Write version, but fall back to using the Read version if the Write byte is not in the buffer
    /// Future builds will back up their Read version to the minimum version and use the Write version to determine how many properties it can read before it stops.
    /// Future builds will then be able to check the Read and Write and determine if the object can be safely saved without clobbering properties that were saved using a newer application build.
    /// Legacy builds can still read the object however which makes this pattern both forward and backward compatible.
    /// 
    /// Usage: 
    /// 
    /// In this case, Minimum version would be 0 and Legacy version would be 2. When deserializing, IOREAD could be 0, 1, or 2 for all existing serializations.
    /// In the new pattern, IOREAD is set back to the Minimum version of 0 and IOWRITE is set to the Legacy read version which could be 0, 1, or 2
    /// 
    /// When past build Ver 0 reads and IOREAD = 0, it will only read Ver_0 properties
    /// When past build Ver 0 reads and IOREAD &gt; 0, it was not future compatible and failed
    /// 
    /// When past build Ver 1 reads and IOREAD = 0, it will only read Ver_0 properties
    /// When past build Ver 1 reads and IOREAD = 1, it will only read Ver_0 and Ver_1 properties
    /// When past build Ver 1 reads and IOREAD &gt; 1, it was not future compatible and failed
    /// 
    /// New pattern added to end of Ver 1:
    /// When current build Ver 1 reads and IOREAD &lt; 1, it will read Ver_0 and Ver_1 properties
    /// When current build Ver 1 reads and IOREAD = 1, it will read Ver_0 and Ver_1 properties, then will try to read the IOWRITE version
    ///	
    /// IOREAD will remain at Ver 1 and IOWRITE will start at Ver 1
    /// 
    /// When future build Ver 2 reads and IOREAD = 0, it will only read Ver_0 properties
    /// When future build Ver 2 reads and IOREAD = 1 and IOWRITE = 1, it will read Ver_0 and Ver_1 properties, then will try to read the IOWRITE version
    /// When future build Ver 2 reads and IOREAD = 1 and IOWRITE = 2, it will read Ver_0, Ver_1, IOWRITE, and Ver_2 properties
    /// When future build Ver 2 reads and IOREAD = 1 and IOWRITE &gt; 2, it will only read Ver_0, Ver_1, IOWRITE, and Ver_2 properties but will mark the object as read-only so it does not get saved overwriting changes made by a newer build
    /// 
    /// When future build Ver 2 reads and IOREAD = 2, it is not future compatible and fails
    /// 
    /// Example:
    /// 
    /// IOREAD							&lt;&lt;---- Original Read Version
    /// 
    /// Ver_0_PastA,
    /// Ver_0_PastB,
    /// Ver_0_PastC,
    /// 
    /// if (ver >= 1) Ver_1_PastD,
    /// if (ver >= 1) Ver_1_PastE		&lt;&lt;---- Last Known Legacy Read Version before new pattern added in current build 
    /// 
    /// IOWRITE	(Current)				&lt;&lt;---- New Write Versions inserted after last known version, but Read version number not incremented
    ///
    /// if (ver >= 2) Ver_2_FutureF		&lt;&lt;---- Future Version before new pattern added in Ver_2 
    /// 
    /// </summary>
    public class VersionIO : IEquatable<VersionIO>, IComparable, IComparable<VersionIO>
    {
        public const string c_assemblycompany = "Also Energy, Inc.";
        public const string c_assemblycopyright = "Copyright © 2019 Also Energy, Inc.";

        private const byte DEF = 0x00;
        private const byte RONLY = 0xFF;

        private const byte CTOR = 0x01;
        private const byte READ = 0x02;
        private const byte WRITE = 0x04;
        private const byte BAD = 0x08;
        private const byte ERR = 0x10;
        private const byte OK = 0x20;
        private const byte FUT = 0x40;
        private const byte ROK = CTOR | READ;
        private const byte WOK = CTOR | WRITE;
        private const byte FOK = CTOR | WRITE | FUT;
        private const byte FAIL = BAD | ERR;

        /// <summary>
        /// State of the version initialization 
        /// </summary>
        private byte State { get; set; }
        /// <summary>
        /// Application build must be as current as the minimum version to deserialize the object
        /// </summary>
        public byte Minumum { get; private set; }
        /// <summary>
        /// Application build's last known version at the time of deployment
        /// </summary>
        public byte Current { get; private set; }
        /// <summary>
        /// Serialized object's minimum read version as defined by the application build when the object was last serialized
        /// </summary>
        public byte Read { get; private set; }
        /// <summary>
        /// Serialized object's current version as defined by the application build when the object was last serialized
        /// </summary>
        public byte Write { get; private set; }

        /// <summary>
        /// Object serialization version which is either the future version if the future property data is stored, or the current provided version
        /// </summary>
        /// <param name="cur">Application build's last known version at the time of deployment</param>
        public static byte WRITEVER(VersionIO ver, byte cur) { return (ver?.FUTUREOK == true) ? ver.Write : cur; }
        /// <summary>
        /// Binary read successfully deserialized the object
        /// </summary>
        public bool IOOK { get { return IsNotSet || (Read <= Write && HasRead && HasWrite && (State & (OK | FAIL)) == OK); } }
        /// <summary>
        /// Binary reading can occur as long as the minimum version is recognized by current application build
        /// </summary>
        public bool READOK { get { return IsNotSet || (Read >= Minumum && HasRead); } }
        /// <summary>
        /// Binary writing can occur as long as the current verion known to the current application build is the same or newer than the version the object was last saved with
        /// </summary>
        public bool WRITEOK { get { return IsNotSet || FUTUREOK || (Current >= Write && HasRead && HasWrite); } }
        /// <summary>
        /// Binary writing can occur for future version as long as the future bytes were retained and can be appended to the end of the output during serialization
        /// </summary>
        public bool FUTUREOK { get { return IsFuture && HasFuture; } }
        /// <summary>
        /// Version is the default instance vs one created using the constructor
        /// </summary>
        public bool IsNotSet { get { return State == DEF; } }
        /// <summary>
        /// Version is from the future
        /// </summary>
        public bool IsFuture { get { return Write > Current && HasRead && HasWrite; } }
        /// <summary>
        /// Successfully obtained a Read version
        /// </summary>
        public bool HasRead { get { return (State & (ROK | FAIL)) == ROK; } }
        /// <summary>
        /// Successfully obtained a Write version
        /// </summary>
        public bool HasWrite { get { return (State & (WOK | FAIL)) == WOK; } }
        /// <summary>
        /// Version is in the future, but writing is allowed because the future serialized bytes have been retained
        /// </summary>
        public bool HasFuture { get { return (State & (FOK | FAIL)) == FOK; } }

        private VersionIO() { }
        /// <summary>
        /// Construct a new version with the minimum and current versions
        /// </summary>
        /// <param name="min">Application build must be as current as the minimum version to deserialize the object</param>
        /// <param name="cur">Application build's last known version at the time of deployment</param>
        /// <param name="valid">Choose whether the version starts as a valid or invalid version</param>
        public VersionIO(byte min, byte cur, bool valid = true) { Init(min, cur, valid); }
        public VersionIO(VersionIO src) {
            State = src.State;
            Minumum = src.Minumum;
            Current = src.Current;
            Read = src.Read;
            Write = src.Write;
        }

        /// <summary>
        /// Initialize the version with the minimum and current versions
        /// </summary>
        /// <param name="min">Application build must be as current as the minimum version to deserialize the object</param>
        /// <param name="cur">Application build's last known version at the time of deployment</param>
        /// <param name="valid">Choose whether the version starts as a valid or invalid version</param>
        public void Init(byte min, byte cur, bool valid = true) { Minumum = min; Current = cur; Read = (valid) ? min : DEF; Write = (valid) ? cur : RONLY; State = CTOR; if (!valid) return; State |= READ; State |= WRITE; }
        /// <summary>
        /// Initialize the Read and Write versions
        /// </summary>
        /// <param name="read">Serialized object's minimum read version as defined by the application build when the object was last serialized</param>
        /// <param name="write">Serialized object's current version as defined by the application build when the object was last serialized</param>
        public void Set(byte read, byte write) { Read = read; State |= READ; Write = write; State |= WRITE; }
        /// <summary>
        /// Check the version saved in the buffer without incrementing the index
        /// </summary>
        /// <param name="buf">Serialized buffer for the object</param>
        /// <param name="ix">Index where the version byte is expected</param>
        public byte? Peek(byte[] buf, int ix) { return (ix >= 0 && ix < buf?.Length) ? buf[ix] : (byte?)null; }
        /// <summary>
        /// Set the Read version from the specified index and increment if available or return Invalid if unable to obtain a valid version byte at the provided index
        /// </summary>
        /// <param name="buf">Serialized buffer for the object</param>
        /// <param name="ix">Index where the version byte is expected</param>
        public byte GetRead(byte[] buf, ref int ix) { byte? ret = Peek(buf, ix); State |= (ret != null) ? READ : BAD; if (ret == null) State &= READ; if (ret == null) throw new ArgumentOutOfRangeException("Failed to get Read version from buffer"); ix++; return Read = ret.Value; }
        /// <summary>
        /// Set the Write version from the specified index and increment if available or return Invalid if unable to obtain a valid version byte at the provided index
        /// </summary>
        /// <param name="buf">Serialized buffer for the object</param>
        /// <param name="ix">Index where the version byte is expected</param>
        public byte GetWrite(byte[] buf, ref int ix) { byte? ret = Peek(buf, ix); State |= (ret != null) ? WRITE : BAD; if (ret == null) State &= WRITE; if (ret == null) throw new ArgumentOutOfRangeException("Failed to get Write version from buffer"); ix++; return Write = ret.Value; }
        /// <summary>
        /// Try to read the Write version byte in cases where it should exist, or fall back to the Read version for compatibility.
        /// </summary>
        /// <param name="buf">Serialized buffer for the object</param>
        /// <param name="ix">Index where the version byte is expected</param>
        /// <param name="legacy">Last version number after which the Write version was added without changing the read version</param>
        /// <param name="read">Current read version which ceases to increment after the pattern is introduced</param>
        /// <param name="write">Output write version which must not be larger than the known current version, otherwise this object is from the future and must be read only</param>
        public byte TryGetWrite(byte[] buf, ref int ix, byte legacy, byte read) {
            if (read > legacy) return GetWrite(buf, ref ix); // read version is after legacy end version so the write version byte should be there
            if (read == legacy && Peek(buf, ix) != null) return GetWrite(buf, ref ix); // read version matches legacy read version when pattern was added but not sure if this was before or after pattern; the buffer is long enough to contain a write version which would have been longer than the original buffer for this version, so this is the new pattern
            SetWrite(read); // apply the write version			
            return read; // this is a legacy serialization from before the pattern was introduced and therefore the write version byte is not part of the buffer so write version is the same as read
        }
        /// <summary>
        /// Set the object to a Read version
        /// </summary>
        public byte SetRead(byte read) { State |= READ; return Read = read; }
        /// <summary>
        /// Set the object to a Write version
        /// </summary>
        public byte SetWrite(byte write) { State |= WRITE; return Write = write; }
        /// <summary>
        /// Set the object to Read-Only for serializations that do not contain a Write version
        /// </summary>
        public byte SetReadOnly() { State |= WRITE; return Write = RONLY; }
        /// <summary>
        /// Set the IOOK state to failed
        /// IO should call Init when processing starts and SetFail if it fails and SetSucceed if it succeeds
        /// </summary>
        public bool SetFail() { State |= ERR; return IOOK; }
        /// <summary>
        /// Set the IOOK state to failed
        /// IO should call Init when processing starts and SetFail if it fails and SetSucceed if it succeeds
        /// </summary>
        public bool SetSuccess() { State |= OK; return IOOK; }
        /// <summary>
        /// Set the IOOK state to failed
        /// IO should call Init when processing starts and SetFail if it fails and SetSucceed if it succeeds
        /// </summary>
        public bool SetFuture(byte[] future) { if (!IsFuture || future?.Any() != true) return false; State |= FUT; return FUTUREOK; }

        /// <summary>
        /// Tests whether two VersionIO structures have the same Minimum, Current, Read and Write versions
        /// State is ignored
        /// </summary>
        public static bool operator ==(VersionIO a, VersionIO b) { return (Object.ReferenceEquals(a, null)) ? Object.ReferenceEquals(b, null) : a.Equals(b); }

        /// <summary>
        /// Tests whether two VersionIO structures have the same Minimum, Current, Read and Write versions
        /// State is ignored
        /// </summary>
        public static bool operator !=(VersionIO a, VersionIO b) { return !(a == b); }

        /// <summary>
        /// Tests whether one version is greater than the other using Write, Read, Current, and Minimum in that order
        /// State is ignored
        /// </summary>
        public static bool operator >(VersionIO a, VersionIO b) { return a?.CompareTo(b) > 0; }

        /// <summary>
        /// Tests whether one version is greater than the other using Write, Read, Current, and Minimum in that order
        /// State is ignored
        /// </summary>
        public static bool operator <(VersionIO a, VersionIO b) { return a?.CompareTo(b) < 0; }

        /// <summary>
        /// Tests whether one version is greater or equal than the other using Write, Read, Current, and Minimum in that order
        /// State is ignored
        /// </summary>
        public static bool operator >=(VersionIO a, VersionIO b) { return a?.CompareTo(b) >= 0; }

        /// <summary>
        /// Tests whether one version is greater or equal than the other using Write, Read, Current, and Minimum in that order
        /// State is ignored
        /// </summary>
        public static bool operator <=(VersionIO a, VersionIO b) { return a?.CompareTo(b) <= 0; }

        /// <summary>
        /// Tests whether two VersionIO structures have the same Minimum, Current, Read and Write versions
        /// State is ignored
        /// </summary>
        public override bool Equals(object obj) { return obj is VersionIO && Equals((VersionIO)obj); }

        /// <summary>
        /// Tests whether two VersionIO structures have the same Minimum, Current, Read and Write versions
        /// State is ignored
        /// </summary>
        public bool Equals(VersionIO other) { return GetHashCode() == other?.GetHashCode(); }

        /// <summary>
        /// Gets a hash code for the current VersionIO structure
        /// </summary>
        public override int GetHashCode() { return (Write << 24) | (Read << 16) | (Current << 8) | Minumum; }

        /// <summary>
        /// Compare this version with another and return the sort order determined by the Write, Read, Current, and Minimum in that order
        /// </summary>
        public int CompareTo(object obj) { return (obj is VersionIO) ? CompareTo((VersionIO)obj) : 1; }

        /// <summary>
        /// Compare this version with another and return the sort order determined by the Write, Read, Current, and Minimum in that order
        /// </summary>
        public int CompareTo(VersionIO other) { return Compare(this, other); }

        /// <summary>
        /// Compare this version with another and return the sort order determined by the Write, Read, Current, and Minimum in that order
        /// </summary>
        public static int Compare(VersionIO a, VersionIO b) { return (a?.GetHashCode() ?? 0).CompareTo(b?.GetHashCode() ?? 0); }

        /// <summary>
        /// Friendly display text
        /// </summary>
        public override string ToString() { return $"Read: {((READOK) ? "*" : String.Empty)}{Read} >= {Minumum} Write: {((WRITEOK) ? "*" : String.Empty)}{Write} <= {Current}{((FUTUREOK) ? " Future" : String.Empty)}{((!IOOK) ? " INVALID" : String.Empty)}"; }

    }
}
