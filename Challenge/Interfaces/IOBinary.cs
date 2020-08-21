namespace AE.CoreInterface
{
    /// <summary>
    /// Read-only binary serialization interface for IO operations
    /// </summary>
    public interface IOBinaryR
    {
        byte[] IO { get; }
        bool IOOK { get; }
    }

    /// <summary>
    /// Binary serialization interface for IO operations
    /// </summary>
    public interface IOBinary : IOBinaryR
    {
        new byte[] IO { get; set; }
    }
}
