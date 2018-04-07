namespace Sooda
{
    using System;

    [Flags]
    public enum SoodaTransactionOptions
    {
        NoImplicit = 0x0000,
        Implicit = 0x0001
    }
}