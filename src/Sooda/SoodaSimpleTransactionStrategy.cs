namespace Sooda
{
    public class SoodaSimpleTransactionStrategy : IDefaultSoodaTransactionStrategy
    {
        private SoodaTransaction _default;

        #region IDefaultSoodaTransactionStrategy Members

        public SoodaTransaction SetDefaultTransaction(SoodaTransaction transaction)
        {
            SoodaTransaction _prev = GetDefaultTransaction();
            _default = transaction;
            return _prev;
        }

        public SoodaTransaction GetDefaultTransaction()
        {
            return _default;
        }

        #endregion
    }
}