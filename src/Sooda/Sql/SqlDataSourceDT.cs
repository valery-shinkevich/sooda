﻿//
// Copyright (c) 2003-2006 Jaroslaw Kowalski <jaak@jkowalski.net>
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// * Redistributions of source code must retain the above copyright notice,
//   this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//


#if !MONO

namespace Sooda.Sql
{
    /// <summary>
    /// Data source that allows external (DTC) transactions
    /// How? By not using ADO.Net transaction when there is active external transaction.
    /// No changes other from that.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public class SqlDataSourceDT : SqlDataSource
    {
        public SqlDataSourceDT(string name)
            : base(name)
        {
        }

        public SqlDataSourceDT(Schema.DataSourceInfo dataSourceInfo)
            : this(dataSourceInfo.Name)
        {
        }

        private static bool InExternalTransaction()
        {
            return System.Transactions.Transaction.Current != null;
            //TransactionStatus ts = System.Transactions.Transaction.Current.TransactionInformation.Status;
            //if (ts == System.Transactions.TransactionStatus.Active || ts == System.Transactions.TransactionStatus.InDoubt)                
        }

        protected override void BeginTransaction()
        {
            if (Transaction != null)
                Logger.Warn("Previous transaction has not been closed");
            if (InExternalTransaction())
            {
                Logger.Debug("External transaction exists, will not start ADO transaction");
            }
            else
            {
                Logger.Debug("Starting new ADO.Net transaction");
                base.BeginTransaction();
            }
        }
    }
}

#endif