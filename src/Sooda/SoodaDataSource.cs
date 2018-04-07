//
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

namespace Sooda
{
    using System;
    using System.Collections;
    using System.Data;
    using Schema;

    public abstract class SoodaDataSource : IDisposable
    {
        private readonly string _name;

        protected SoodaDataSource(string name)
        {
            _name = name;
        }

        protected SoodaDataSource(DataSourceInfo dataSourceInfo)
        {
            _name = dataSourceInfo.Name;
        }

        public string Name
        {
            get { return _name; }
        }

        protected string GetParameter(string name, bool throwOnFailure)
        {
            string val = SoodaConfig.GetString(Name + "." + name);
            if (val != null)
                return val;

            if (Name == "default")
            {
                val = SoodaConfig.GetString(name);
                if (val != null)
                    return val;
            }

            if (throwOnFailure)
                throw new SoodaException("Parameter '" + name + "' not defined for data source '" + Name + "'");
            return null;
        }

        public void Dispose()
        {
            if (IsOpen)
            {
                Close();
            }
        }

        public SoodaStatistics Statistics { get; set; }

        public IDbConnection Connection;

        private CommandBehavior _cmdBehavior = CommandBehavior.Default;

        public CommandBehavior CmdBehavior
        {
            get { return _cmdBehavior; }
            set { _cmdBehavior = value; }
        }

        public abstract bool IsOpen { get; }
        public abstract void Rollback();
        public abstract void Commit();
        public abstract void Open();
        public abstract void Close();
        public abstract IsolationLevel IsolationLevel { get; set; }

        public abstract void BeginSaveChanges();
        public abstract void SaveObjectChanges(SoodaObject obj, bool isPrecommit);
        public abstract void FinishSaveChanges();

        public abstract IDataReader LoadObject(SoodaObject obj, object keyValue, out TableInfo[] tables);

        public abstract IDataReader LoadObjectTable(SoodaObject obj, object keyValue, int tableNumber,
            out TableInfo[] tables);

        public abstract IDataReader LoadAllObjectTables(SoodaObject obj, object keyValue, out TableInfo[] tables);

        public abstract void MakeTuple(string tableName, string leftColumn, string rightColumn, object leftVal,
            object rightVal, int mode);

        public abstract IDataReader LoadMatchingPrimaryKeys(SchemaInfo schemaInfo, ClassInfo classInfo,
            SoodaWhereClause whereClause, SoodaOrderBy orderBy, int startIdx, int pageCount);

        public abstract IDataReader LoadObjectList(SchemaInfo schemaInfo, ClassInfo classInfo,
            SoodaWhereClause whereClause, SoodaOrderBy orderBy, int startIdx, int pageCount,
            SoodaSnapshotOptions options, out TableInfo[] tables);

        public abstract IDataReader LoadRefObjectList(SchemaInfo schemaInfo, RelationInfo relationInfo, int masterColumn,
            object masterValue, out TableInfo[] tables);

        public abstract IDataReader ExecuteQuery(QL.SoqlQueryExpression query, SchemaInfo schema,
            params object[] parameters);

        public abstract IDataReader ExecuteRawQuery(string queryText, params object[] parameters);
        public abstract int ExecuteNonQuery(string queryText, params object[] parameters);

        public IDataReader ExecuteQuery(QL.SoqlQueryExpression queryText, SchemaInfo schema, ArrayList parameters)
        {
            return ExecuteQuery(queryText, schema, parameters.ToArray());
        }

        public IDataReader ExecuteRawQuery(string queryText, ArrayList parameters)
        {
            return ExecuteRawQuery(queryText, parameters.ToArray());
        }

        public int ExecuteNonQuery(string queryText, ArrayList parameters)
        {
            return ExecuteNonQuery(queryText, parameters.ToArray());
        }
    }
}