namespace AE.CoreInterface
{
    public interface IOStringR
    {
        string Str { get; }
        bool StrOK { get; }
    }

    public interface IOString : IOStringR
    {
        new string Str { get; set; }
    }
}
