//
// Copyright (c) 2003-2006 Jaroslaw Kowalski <jaak@jkowalski.net>
// Copyright (c) 2006-2014 Piotr Fusik <piotr@fusik.info>
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
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

using System;
using System.Data;

namespace Sooda.ObjectMapper.KeyGenerators
{
    public abstract class TableBasedGeneratorBase
    {
        private readonly string _keyName;
        protected readonly int PoolSize;
        private static readonly Random Random = new Random();
        private readonly Schema.DataSourceInfo _dataSourceInfo;
        private readonly string _tableName;
        private readonly string _keyNameColumn;
        private readonly string _keyValueColumn;

        protected TableBasedGeneratorBase(string keyName, Schema.DataSourceInfo dataSourceInfo)
        {
            _keyName = keyName;
            _dataSourceInfo = dataSourceInfo;

            _tableName = SoodaConfig.GetString(dataSourceInfo.Name + ".keygentable.name", "KeyGen");
            _keyNameColumn = SoodaConfig.GetString(dataSourceInfo.Name + ".keygentable.keycolumn", "key_name");
            _keyValueColumn = SoodaConfig.GetString(dataSourceInfo.Name + ".keygentable.valuecolumn", "key_value");
            PoolSize = Convert.ToInt32(SoodaConfig.GetString(dataSourceInfo.Name + ".keygentable.pool_size", "10"));
        }

        protected long AcquireNextRange()
        {
#if MONO
 return AcquireNextRangeInternal();
#else
            using (
                // ReSharper disable once UnusedVariable
                var ts =
                    new System.Transactions.TransactionScope(System.Transactions.TransactionScopeOption.Suppress))
            {
                return AcquireNextRangeInternal();
            }
#endif
        }

        private long AcquireNextRangeInternal()
        {
            // TODO - fix me, this is hack

            using (Sql.SqlDataSource sds = (Sql.SqlDataSource) _dataSourceInfo.CreateDataSource())
            {
                sds.Open();

                IDbConnection conn = sds.Connection;

                bool justInserted = false;
                int maxRandomTimeout = 2;
                for (int i = 0; i < 10; ++i)
                {
                    string query = "select " + _keyValueColumn + " from " + _tableName + " where " + _keyNameColumn +
                                   " = '" + _keyName + "'";
                    IDbCommand cmd = conn.CreateCommand();

                    if (!sds.DisableTransactions)
                        cmd.Transaction = sds.Transaction;

                    cmd.CommandText = query;
                    long keyValue = -1;

                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                            keyValue = Convert.ToInt64(reader.GetValue(0));
                    }

                    if (keyValue == -1)
                    {
                        if (justInserted)
                            throw new Exception("FATAL DATABASE ERROR - cannot get new key value");
                        cmd.CommandText = "insert into " + _tableName + "(" + _keyNameColumn + ", " + _keyValueColumn +
                                          ") values('" + _keyName + "', 1)";
                        cmd.ExecuteNonQuery();
                        justInserted = true;
                        continue;
                    }

                    //Console.WriteLine("Got key: {0}", keyValue);
                    //Console.WriteLine("Press any key to update database (simulating possible race condition here).");
                    //Console.ReadLine();

                    long nextKeyValue = keyValue + PoolSize;

                    cmd.CommandText = "update " + _tableName + " set " + _keyValueColumn + " = " + nextKeyValue +
                                      " where " + _keyNameColumn + " = '" + _keyName + "' and " + _keyValueColumn +
                                      " = " + keyValue;
                    int rows = cmd.ExecuteNonQuery();
                    // Console.WriteLine("{0} row(s) affected", rows);

                    if (rows != 1)
                    {
                        // Console.WriteLine("Conflict on write, sleeping for random number of milliseconds ({0} max)", maxRandomTimeout);
                        System.Threading.Thread.Sleep(1 + Random.Next(maxRandomTimeout));
                        maxRandomTimeout = maxRandomTimeout*2;
                        // conflict on write
                        continue;
                    }
                    sds.Commit();

                    //Console.WriteLine("New key range for {0} [{1}:{2}]", keyName, currentValue, maxValue);
                    return keyValue;
                }
                throw new Exception("FATAL DATABASE ERROR - cannot get new key value");
            }
        }
    }
}