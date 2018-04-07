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

namespace Sooda.Sql
{
    using Logging;
    using QL;
    using Schema;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Utils;

    public class SqlDataSource : SoodaDataSource
    {
        protected static readonly Logger Logger = LogManager.GetLogger("Sooda.SqlDataSource");
        protected static readonly Logger Sqllogger = LogManager.GetLogger("Sooda.SQL");

        private IDbCommand _updateCommand;
        private IsolationLevel _isolationLevel = IsolationLevel.ReadCommitted;

        private bool _ownConnection;
        public IDbTransaction Transaction;
        public ISqlBuilder SqlBuilder;
        public bool DisableTransactions;
        public bool DisableUpdateBatch;
        public bool StripWhitespaceInLogs;
        public bool IndentQueries;
        public bool UpperLike = false;
        public double QueryTimeTraceWarn = 10.0;
        public double QueryTimeTraceInfo = 2.0;
        public int CommandTimeout = 30;
        public Type ConnectionType;
        public string ConnectionString;
        public string CreateTable = "";
        public string CreateIndex = "";

        public SqlDataSource(string name) : base(name)
        {
            var s = GetParameter("queryTimeTraceInfo", false);

            if (!string.IsNullOrEmpty(s))
                QueryTimeTraceInfo = Convert.ToDouble(s);

            s = GetParameter("queryTimeTraceWarn", false);

            if (!string.IsNullOrEmpty(s))
                QueryTimeTraceWarn = Convert.ToDouble(s);

            s = GetParameter("commandTimeout", false);

            if (!string.IsNullOrEmpty(s))
                CommandTimeout = Convert.ToInt32(s);

            if (GetParameter("disableTransactions", false) == "true")
                DisableTransactions = true;

            if (GetParameter("stripWhitespaceInLogs", false) == "true")
                StripWhitespaceInLogs = true;

            if (GetParameter("indentQueries", false) == "true")
                IndentQueries = true;

            if (GetParameter("upperLike", false) == "true")
                UpperLike = true;

            string at = GetParameter("createTable", false);
            if (at != null)
                CreateTable = at;

            at = GetParameter("createIndex", false);
            if (at != null)
                CreateIndex = at;

            var dialect = GetParameter("sqlDialect", false) ?? "microsoft";

            DisableUpdateBatch = true;

            switch (dialect)
            {
                default:
                    SqlBuilder = new SqlServerBuilder();
                    DisableUpdateBatch = false;
                    break;

                case "postgres":
                case "postgresql":
                    SqlBuilder = new PostgreSqlBuilder();
                    break;

                case "mysql":
                case "mysql4":
                    SqlBuilder = new MySqlBuilder();
                    break;

                case "oracle":
                    SqlBuilder = new OracleBuilder();
                    break;
            }

            if (GetParameter("useSafeLiterals", false) == "false")
                SqlBuilder.UseSafeLiterals = false;

            if (GetParameter("indentQueries", false) == "true")
                IndentQueries = true;

            if (GetParameter("disableUpdateBatch", false) == "true")
                DisableUpdateBatch = true;

            var connectionTypeName = GetParameter("connectionType", false) ?? "sqlclient";

            switch (connectionTypeName)
            {
                case "sqlclient":
                    ConnectionType = typeof (System.Data.SqlClient.SqlConnection);
                    break;

                default:
                    ConnectionType = Type.GetType(connectionTypeName);
                    break;
            }

            ConnectionString = GetParameter("connectionString", false);
        }

        public SqlDataSource(DataSourceInfo dataSourceInfo) : this(dataSourceInfo.Name)
        {
        }

        public override IsolationLevel IsolationLevel
        {
            get { return _isolationLevel; }
            set { _isolationLevel = value; }
        }

        protected virtual void BeginTransaction()
        {
            Transaction = Connection.BeginTransaction(IsolationLevel);
        }

        public override void Open()
        {
            if (ConnectionString == null)
                throw new SoodaDatabaseException("connectionString parameter not defined for datasource: " + Name);
            if (ConnectionType == null)
                throw new SoodaDatabaseException("connectionType parameter not defined for datasource: " + Name);
            string stries = SoodaConfig.GetString("sooda.connectionopenretries", "2");
            int tries;
            try
            {
                tries = Convert.ToInt32(stries);
            }
            catch
            {
                tries = 2;
            }
            int maxtries = tries;
            while (tries > 0)
            {
                try
                {
                    Connection =
                        (IDbConnection) Activator.CreateInstance(ConnectionType, new object[] {ConnectionString});
                    _ownConnection = true;
                    Connection.Open();

                    if (!DisableTransactions)
                    {
                        BeginTransaction();
                        if (SqlBuilder is OracleBuilder &&
                            SoodaConfig.GetString("sooda.oracleClientAutoCommitBugWorkaround", "false") == "true")
                        {
                            // http://social.msdn.microsoft.com/forums/en-US/adodotnetdataproviders/thread/d4834ce2-482f-40ec-ad90-c3f9c9c4d4b1/
                            // http://connect.microsoft.com/VisualStudio/feedback/ViewFeedback.aspx?FeedbackID=351746
                            Connection.GetType()
                                .GetProperty("TransactionState",
                                    System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic).
                                SetValue(Connection, 1, null);
                        }
                    }
                    tries = 0;
                }
                catch (Exception e)
                {
                    tries--;
                    Logger.Warn("Exception on Open#{0}: {1}", maxtries - tries, e);
                    if ((tries == 0) || SqlBuilder.IsFatalException(Connection, e))
                        throw;
                }
            }
        }

        public override bool IsOpen
        {
            get { return (Connection != null) && (Connection.State == ConnectionState.Open); }
        }

        public override void Rollback()
        {
            if (!_ownConnection || DisableTransactions) return;
            if (Transaction != null)
            {
                Transaction.Rollback();
                Transaction.Dispose();
                Transaction = null;
            }
            BeginTransaction();
        }

        public override void Commit()
        {
            if (!_ownConnection || DisableTransactions) return;
            if (Transaction != null)
            {
                Transaction.Commit();
                Transaction.Dispose();
                Transaction = null;
            }
            BeginTransaction();
        }

        public override void Close()
        {
            if (!_ownConnection) return;

            if (!DisableTransactions && Transaction != null)
            {
                Transaction.Rollback();
                Transaction.Dispose();
                Transaction = null;
            }

            if (Connection == null) return;

            Connection.Close();
            Connection.Dispose();
            Connection = null;
        }

        public override void BeginSaveChanges()
        {
            //if (!IsOpen)//{//    Open(); //+wash//}
            _updateCommand = Connection.CreateCommand();
            try
            {
                _updateCommand.CommandTimeout = CommandTimeout;
            }
            catch (NotSupportedException e)
            {
                Logger.Debug("CommandTimeout not supported. {0}", e.Message);
            }
            if (Transaction != null)
                _updateCommand.Transaction = Transaction;
            _updateCommand.CommandText = "";
        }

        public override void FinishSaveChanges()
        {
            FlushUpdateCommand(true);
            _updateCommand = null;
        }

        private void FlushUpdateCommand(bool final)
        {
            if (final || DisableUpdateBatch || _updateCommand.Parameters.Count >= 100)
            {
                if (_updateCommand.CommandText != "")
                {
                    TimedExecuteNonQuery(_updateCommand);
                    _updateCommand.Parameters.Clear();
                    _updateCommand.CommandText = "";
                }
            }
        }

        private static void FieldEquals(FieldInfo fi, object value, StringBuilder builder, ArrayList queryParams)
        {
            builder.Append(fi.DBColumnName);
            builder.Append("={");
            builder.Append(queryParams.Add(value));
            builder.Append(':');
            builder.Append(fi.DataType);
            builder.Append('}');
        }

        private void DoWithWhere(SoodaObject obj, StringBuilder builder, ArrayList queryParams, bool isRaw)
        {
            builder.Append(" where ");
            object primaryKeyValue = obj.GetPrimaryKeyValue();
            FieldInfo[] primaryKeyFields = obj.GetClassInfo().GetPrimaryKeyFields();
            for (int i = 0; i < primaryKeyFields.Length; i++)
            {
                if (i > 0)
                    builder.Append(" and ");
                FieldEquals(primaryKeyFields[i], SoodaTuple.GetValue(primaryKeyValue, i), builder, queryParams);
            }
            SqlBuilder.BuildCommandWithParameters(_updateCommand, true, builder.ToString(), queryParams.ToArray(), isRaw);
            FlushUpdateCommand(false);
        }

        private void DoDeletesForTable(SoodaObject obj, TableInfo table)
        {
            StringBuilder builder = new StringBuilder();
            ArrayList queryParams = new ArrayList();
            builder.Append("delete from ");
            builder.Append(table.DBTableName);
            DoWithWhere(obj, builder, queryParams, true);
        }


        private void DoDeletes(SoodaObject obj)
        {
            List<TableInfo> tables = obj.GetClassInfo().UnifiedTables;
            for (int i = tables.Count - 1; i >= 0; --i)
            {
                DoDeletesForTable(obj, tables[i]);
            }
        }

        public override void SaveObjectChanges(SoodaObject obj, bool isPrecommit)
        {
            if (obj.IsMarkedForDelete())
            {
                DoDeletes(obj);
                return;
            }
            if (obj.IsInsertMode() && !obj.InsertedIntoDatabase)
            {
                DoInserts(obj, isPrecommit);
                obj.InsertedIntoDatabase = true;
            }
            else
            {
                DoUpdates(obj);
            }
        }

        public override IDataReader LoadObjectTable(SoodaObject obj, object keyVal, int tableNumber,
            out TableInfo[] loadedTables)
        {
            var classInfo = obj.GetClassInfo();

            //if (!IsOpen) Open(); //wash

            var cmd = Connection.CreateCommand();
            try
            {
                cmd.CommandTimeout = CommandTimeout;
            }
            catch (NotSupportedException e)
            {
                Logger.Debug("CommandTimeout not supported. {0}", e.Message);
            }

            if (Transaction != null)
                cmd.Transaction = Transaction;

            SqlBuilder.BuildCommandWithParameters(cmd, false,
                GetLoadingSelectStatement(classInfo,
                    classInfo.UnifiedTables[tableNumber],
                    out loadedTables),
                SoodaTuple.GetValuesArray(keyVal), false);
            var reader = TimedExecuteReader(cmd);

            if (reader.Read()) return reader;

            reader.Dispose();

            return null;
        }

        public override IDataReader LoadObject(SoodaObject obj, object keyVal, out TableInfo[] loadedTables)
        {
            return LoadObjectTable(obj, keyVal, 0, out loadedTables);
        }

        public override IDataReader LoadAllObjectTables(SoodaObject obj, object keyVal, out TableInfo[] loadedTables)
        {
            var classInfo = obj.GetClassInfo();

            var cmd = Connection.CreateCommand();
            try
            {
                cmd.CommandTimeout = CommandTimeout;
            }
            catch (NotSupportedException e)
            {
                Logger.Debug("CommandTimeout not supported. {0}", e.Message);
            }

            if (Transaction != null)
                cmd.Transaction = Transaction;

            SqlBuilder.BuildCommandWithParameters(cmd, false,
                GetAllDataLoadingSelectStatement(classInfo, out loadedTables),
                SoodaTuple.GetValuesArray(keyVal), false);
            var reader = TimedExecuteReader(cmd);

            if (reader.Read()) return reader;

            reader.Dispose();

            return null;
        }

        public override void MakeTuple(string tableName, string leftColumnName, string rightColumnName, object leftVal,
            object rightVal, int mode)
        {
            var parameters = new[] {leftVal, rightVal};

            var query = "delete from " + tableName + " where " + leftColumnName + "={0} and " + rightColumnName + "={1}";
            SqlBuilder.BuildCommandWithParameters(_updateCommand, true, query, parameters, false);
            FlushUpdateCommand(false);

            if (mode == 1)
            {
                query = "insert into " + tableName + "(" + leftColumnName + "," + rightColumnName + ") values({0},{1})";
                SqlBuilder.BuildCommandWithParameters(_updateCommand, true, query, parameters, false);

                FlushUpdateCommand(false);
            }
        }

        private string SoqlToSql(SoqlQueryExpression queryExpression, SchemaInfo schemaInfo, bool generateColumnAliases)
        {
            StringWriter sw = new StringWriter();
            SoqlToSqlConverter converter = new SoqlToSqlConverter(sw, schemaInfo, SqlBuilder);
            converter.IndentOutput = IndentQueries;
            converter.GenerateColumnAliases = generateColumnAliases;
            converter.UpperLike = UpperLike;
            //logger.Trace("Converting {0}", queryExpression);
            converter.ConvertQuery(queryExpression);
            string query = sw.ToString();
            //logger.Trace("Converted as {0}", query);
            return query;
        }

        public override IDataReader LoadMatchingPrimaryKeys(SchemaInfo schemaInfo, ClassInfo classInfo,
            SoodaWhereClause whereClause, SoodaOrderBy orderBy, int startIdx, int pageCount)
        {
            try
            {
                var queryExpression = new SoqlQueryExpression();
                foreach (var fi in classInfo.GetPrimaryKeyFields())
                {
                    queryExpression.SelectExpressions.Add(new SoqlPathExpression(fi.Name));
                    queryExpression.SelectAliases.Add("");
                }
                if (schemaInfo.GetSubclasses(classInfo).Count > 0)
                {
                    queryExpression.SelectExpressions.Add(new SoqlPathExpression(classInfo.SubclassSelectorField.Name));
                    queryExpression.SelectAliases.Add("");
                }
                queryExpression.StartIdx = startIdx;
                queryExpression.PageCount = pageCount;
                queryExpression.From.Add(classInfo.Name);
                queryExpression.FromAliases.Add("");
                if (whereClause != null && whereClause.WhereExpression != null)
                {
                    queryExpression.WhereClause = whereClause.WhereExpression;
                }

                if (orderBy != null)
                {
                    queryExpression.SetOrderBy(orderBy);
                }

                string query = SoqlToSql(queryExpression, schemaInfo, false);

                var cmd = Connection.CreateCommand();
                try
                {
                    cmd.CommandTimeout = CommandTimeout;
                }
                catch (NotSupportedException e)
                {
                    Logger.Debug("CommandTimeout not supported. {0}", e.Message);
                }

                if (Transaction != null)
                    cmd.Transaction = Transaction;

                if (whereClause != null)
                    SqlBuilder.BuildCommandWithParameters(cmd, false, query, whereClause.Parameters, false);

                // CmdLimiting(cmd, limitBegin, limitEnd);
                return TimedExecuteReader(cmd);
            }
            catch (Exception ex)
            {
                Logger.Error("Exception in LoadMatchingPrimaryKeys: {0}", ex);
                throw;
            }
        }

        public override IDataReader LoadObjectList(SchemaInfo schemaInfo, ClassInfo classInfo,
            SoodaWhereClause whereClause, SoodaOrderBy orderBy, int startIdx, int pageCount,
            SoodaSnapshotOptions options, out TableInfo[] tables)
        {
            try
            {
                var queue = new Queue<QueueItem>();

                var tablesArrayList = new List<TableInfo>(classInfo.UnifiedTables.Count);
                var queryExpression = new SoqlQueryExpression
                {
                    StartIdx = startIdx,
                    PageCount = pageCount
                };
                queryExpression.From.Add(classInfo.Name);
                queryExpression.FromAliases.Add("");
                var count = 0;
                foreach (var ti in classInfo.UnifiedTables)
                {
                    tablesArrayList.Add(ti);
                    foreach (var fi in ti.Fields)
                    {
                        var pathExpr = new SoqlPathExpression(fi.Name);

                        queryExpression.SelectExpressions.Add(pathExpr);
                        if (fi.IsPrimaryKey)
                        {
                            queryExpression.SelectAliases.Add(count == 0 ? "" : fi.Name + "_" + count);
                            count++;
                        }
                        else
                            queryExpression.SelectAliases.Add("");

                        if (fi.ReferencedClass == null || fi.PrefetchLevel <= 0 ||
                            ((options & SoodaSnapshotOptions.PrefetchRelated) == 0)) continue;

                        var item = new QueueItem
                        {
                            ClassInfo = fi.ReferencedClass,
                            Level = fi.PrefetchLevel,
                            Prefix = pathExpr
                        };
                        queue.Enqueue(item);
                    }
                }

                while (queue.Count > 0)
                {
                    var it = queue.Dequeue();

                    foreach (var ti in it.ClassInfo.UnifiedTables)
                    {
                        tablesArrayList.Add(ti);

                        foreach (var fi in ti.Fields)
                        {
                            // TODO - this relies on the fact that path expressions
                            // are never reconstructed or broken. We simply share previous prefix
                            // perhaps it's cleaner to Clone() the expression here

                            var extendedExpression = new SoqlPathExpression(it.Prefix, fi.Name);

                            queryExpression.SelectExpressions.Add(extendedExpression);
                            if (fi.IsPrimaryKey)
                            {
                                queryExpression.SelectAliases.Add(count == 0 ? "" : fi.Name + "_" + count);
                                count++;
                            }
                            else
                                queryExpression.SelectAliases.Add("");

                            if (it.Level < 1 || fi.PrefetchLevel <= 0 || fi.ReferencedClass == null) continue;

                            var newItem = new QueueItem
                            {
                                ClassInfo = fi.ReferencedClass,
                                Prefix = extendedExpression,
                                Level = (it.Level - 1)
                            };
                            queue.Enqueue(newItem);
                        }
                    }
                }

                if (whereClause != null && whereClause.WhereExpression != null)
                {
                    queryExpression.WhereClause = whereClause.WhereExpression;
                }

                if (orderBy != null)
                {
                    queryExpression.SetOrderBy(orderBy);
                }

                string query = SoqlToSql(queryExpression, schemaInfo, false);

                //if (!IsOpen) Open(); //wash

                var cmd = Connection.CreateCommand();
                try
                {
                    cmd.CommandTimeout = CommandTimeout;
                }
                catch (NotSupportedException e)
                {
                    Logger.Debug("CommandTimeout not supported. {0}", e.Message);
                }


                if (Transaction != null)
                    cmd.Transaction = Transaction;

                if (whereClause != null)
                    SqlBuilder.BuildCommandWithParameters(cmd, false, query, whereClause.Parameters, false);

                tables = tablesArrayList.ToArray();

                //CmdLimiting(cmd, limitBegin, limitEnd);

                return TimedExecuteReader(cmd);
            }
            catch (Exception ex)
            {
                Logger.Error("Exception in LoadObjectList: {0}", ex);
                throw;
            }
        }

        //private void CmdLimiting(IDbCommand cmd, int limitBegin, int limitEnd)
        //{
        //    if (limitBegin < 0) return;


        //    var cmdText = "select top " + int.MaxValue + " " + cmd.CommandText.Substring(6);

        //    cmd.CommandText = string.Format("select * from (select *, row_number() over(order by (select 1)) as a_a_a from ({0}) t) q where a_a_a between @limit_Begin and @limit_End", cmdText);

        //    var param = cmd.CreateParameter();
        //    param.DbType = DbType.Int32;
        //    param.ParameterName = "limit_Begin";
        //    param.Value = limitBegin;
        //    cmd.Parameters.Add(param);

        //    param = cmd.CreateParameter();
        //    param.DbType = DbType.Int32;
        //    param.ParameterName = "limit_End";
        //    param.Value = limitEnd >= 0 ? limitEnd : int.MaxValue;
        //    cmd.Parameters.Add(param);
        //}

        public override IDataReader ExecuteQuery(SoqlQueryExpression query, SchemaInfo schema,
            params object[] parameters)
        {
            try
            {
                string queryText = SoqlToSql(query, schema, false);
                return ExecuteRawQuery(queryText, parameters);
            }
            catch (Exception ex)
            {
                Logger.Error("Exception in ExecuteQuery: {0}", ex);
                throw;
            }
        }

        public override IDataReader ExecuteRawQuery(string queryText, params object[] parameters)
        {
            try
            {
                var cmd = Connection.CreateCommand();
                try
                {
                    cmd.CommandTimeout = CommandTimeout;
                }
                catch (NotSupportedException e)
                {
                    Logger.Debug("CommandTimeout not supported. {0}", e.Message);
                }

                if (Transaction != null)
                    cmd.Transaction = Transaction;

                SqlBuilder.BuildCommandWithParameters(cmd, false, queryText, parameters, true);
                return TimedExecuteReader(cmd);
            }
            catch (Exception ex)
            {
                Logger.Error("Exception in ExecuteRawQuery: {0}", ex);
                throw;
            }
        }

        public override int ExecuteNonQuery(string queryText, params object[] parameters)
        {
            try
            {
                using (var cmd = Connection.CreateCommand())
                {
                    try
                    {
                        cmd.CommandTimeout = CommandTimeout;
                    }
                    catch (NotSupportedException e)
                    {
                        Logger.Debug("CommandTimeout not supported. {0}", e.Message);
                    }
                    if (Transaction != null)
                        cmd.Transaction = Transaction;

                    SqlBuilder.BuildCommandWithParameters(cmd, false, queryText, parameters, true);
                    return TimedExecuteNonQuery(cmd);
                }
            }
            catch (Exception ex)
            {
                Logger.Error("Exception in ExecuteNonQuery: {0}", ex);
                throw;
            }
        }

        public override IDataReader LoadRefObjectList(SchemaInfo schema, RelationInfo relationInfo, int masterColumn,
            object masterValue, out TableInfo[] tables)
        {
            try
            {
                tables = masterColumn == 0
                    ? relationInfo.GetRef1ClassInfo().UnifiedTables[0].ArraySingleton
                    : relationInfo.GetRef2ClassInfo().UnifiedTables[0].ArraySingleton;

                var query = GetLoadRefObjectSelectStatement(relationInfo, masterColumn);

                var cmd = Connection.CreateCommand();
                try
                {
                    cmd.CommandTimeout = CommandTimeout;
                }
                catch (NotSupportedException e)
                {
                    Logger.Debug("CommandTimeout not supported. {0}", e.Message);
                }

                if (Transaction != null)
                    cmd.Transaction = Transaction;

                SqlBuilder.BuildCommandWithParameters(cmd, false, query, new[] {masterValue}, false);
                return TimedExecuteReader(cmd);
            }
            catch (Exception ex)
            {
                Logger.Error("Exception in LoadRefObjectList: {0}", ex);
                throw;
            }
        }

        private void DoInserts(SoodaObject obj, bool isPrecommit)
        {
            foreach (TableInfo table in obj.GetClassInfo().DatabaseTables)
            {
                DoInsertsForTable(obj, table, isPrecommit);
            }
        }

        private void DoInsertsForTable(SoodaObject obj, TableInfo table, bool isPrecommit)
        {
            if (table.IsDynamic && obj.GetFieldValue(table.Fields[table.Fields.Count - 1].ClassUnifiedOrdinal) == null)
            {
                // optimization: don't insert null dynamic fields
                return;
            }
            var builder = new StringBuilder(500);
            builder.Append("insert into ");
            builder.Append(table.DBTableName);
            builder.Append('(');

            var par = new ArrayList();
            bool comma = false;
            foreach (FieldInfo fi in table.Fields)
            {
                //if (fi.ReadOnly) continue;
                if (comma) builder.Append(',');
                comma = true;
                builder.Append(fi.DBColumnName);
            }

            builder.Append(") values (");
            comma = false;
            foreach (FieldInfo fi in table.Fields)
            {
                //if (fi.ReadOnly) continue;
                if (comma) builder.Append(',');
                comma = true;
                var val = obj.GetFieldValue(fi.ClassUnifiedOrdinal);
                if (!fi.IsNullable && SqlBuilder.IsNullValue(val, fi))
                {
                    if (!isPrecommit)
                        throw new SoodaDatabaseException(obj.GetObjectKeyString() + "." + fi.Name +
                                                         " cannot be null on commit.");
                    val = fi.PrecommitTypedValue;
                    if (val == null)
                        throw new SoodaDatabaseException(obj.GetObjectKeyString() + "." + fi.Name +
                                                         " is null on precommit and no 'precommitValue' has been defined for it.");
                    if (Logger.IsDebugEnabled)
                    {
                        Logger.Debug("Using precommit value of {0} for {1}.{2}", val, table.NameToken,
                            fi.Name);
                    }
                }
                builder.Append('{');
                builder.Append(par.Add(val));
                builder.Append(':');
                builder.Append(fi.DataType);
                builder.Append('}');
            }
            builder.Append(')');
            SqlBuilder.BuildCommandWithParameters(_updateCommand, true, builder.ToString(), par.ToArray(), false);
            FlushUpdateCommand(false);
        }

        private void DoUpdates(SoodaObject obj)
        {
            foreach (var table in obj.GetClassInfo().DatabaseTables)
            {
                DoUpdatesForTable(obj, table);
            }
        }

        private void DoUpdatesForTable(SoodaObject obj, TableInfo table)
        {
            if (table.IsDynamic)
            {
                // For dynamic fields do DELETE+INSERT instead of UPDATE.
                // This is because if a dynamic field is added to an existing object, there is no dynamic field row to update.
                // Another reason is that we never store null dynamic fields in the database - an INSERT will be ommitted in this case.
                DoDeletesForTable(obj, table);
                DoInsertsForTable(obj, table, true);
                return;
            }

            var builder = new StringBuilder(500);
            builder.Append("update ");
            builder.Append(table.DBTableName);
            builder.Append(" set ");

            var par = new ArrayList();
            var anyChange = false;
            foreach (FieldInfo fi in table.Fields)
            {
                var fieldNumber = fi.ClassUnifiedOrdinal;

                if (obj.IsFieldDirty(fieldNumber))
                {
                    if (anyChange) builder.Append(", ");

                    FieldEquals(fi, obj.GetFieldValue(fieldNumber), builder, par);
                    anyChange = true;
                }
            }

            if (!anyChange) return;

            DoWithWhere(obj, builder, par, false);
        }

        private string StripWhitespace(string s)
        {
            return !StripWhitespaceInLogs
                ? s
                : s.Replace("\n", " ").Replace("  ", " ").Replace("  ", " ").Replace("  ", " ").Replace("  ", " ");
        }

        private string LogCommand(IDbCommand cmd)
        {
            var txt = new StringBuilder();
            if (IndentQueries)
                txt.Append("\n");
            txt.Append(StripWhitespace(cmd.CommandText));
            if (cmd.Parameters.Count > 0)
            {
                txt.Append(" [");
                foreach (IDataParameter par in cmd.Parameters)
                {
                    txt.AppendFormat(" {0}:{1}={2}", par.ParameterName, par.DbType, par.Value);
                }
                txt.AppendFormat(" ] ({0})", Connection.GetHashCode());
            }
            return txt.ToString();
        }

        public void ExecuteRaw(string sql)
        {
            using (var cmd = Connection.CreateCommand())
            {
                try
                {
                    cmd.CommandTimeout = CommandTimeout;
                }
                catch (NotSupportedException e)
                {
                    Logger.Debug("CommandTimeout not supported. {0}", e.Message);
                }
                if (Transaction != null)
                    cmd.Transaction = Transaction;

                cmd.CommandText = sql;
                TimedExecuteNonQuery(cmd);
            }
        }

        private class TableLoadingCache
        {
            public readonly string SelectStatement;
            public readonly TableInfo[] LoadedTables;

            public TableLoadingCache(string selectStatement, TableInfo[] loadedTables)
            {
                SelectStatement = selectStatement;
                LoadedTables = loadedTables;
            }
        }

        private readonly Dictionary<TableInfo, TableLoadingCache> _tableLoadingCache =
            new Dictionary<TableInfo, TableLoadingCache>();

        private readonly Dictionary<RelationInfo, string>[] _cacheLoadRefObjectSelectStatement =
        {
            new Dictionary<RelationInfo, string>(), new Dictionary<RelationInfo, string>()
        };

        private readonly Dictionary<ClassInfo, TableLoadingCache> _allDataTableLoadingCache =
            new Dictionary<ClassInfo, TableLoadingCache>();

        /*
        private readonly Hashtable cacheAllDataLoadingSelectStatement;
        private readonly Hashtable cacheAllDataLoadedTables;
        */

        private class QueueItem
        {
            public ClassInfo ClassInfo;
            public SoqlPathExpression Prefix;
            public int Level;
        }

        private string GetLoadingSelectStatement(IFieldContainer classInfo, TableInfo tableInfo,
            out TableInfo[] loadedTables)
        {
            TableLoadingCache cache;
            if (_tableLoadingCache.TryGetValue(tableInfo, out cache))
            {
                loadedTables = cache.LoadedTables;
                return cache.SelectStatement;
            }

            var queue = new Queue<QueueItem>();
            var additional = new List<TableInfo>
            {
                tableInfo
            };

            var queryExpression = new SoqlQueryExpression();
            queryExpression.From.Add(classInfo.Name);
            queryExpression.FromAliases.Add("");

            foreach (FieldInfo fi in tableInfo.Fields)
            {
                var pathExpr = new SoqlPathExpression(fi.Name);
                queryExpression.SelectExpressions.Add(pathExpr);
                queryExpression.SelectAliases.Add("");

                if (fi.ReferencedClass != null && fi.PrefetchLevel > 0)

                {
                    var item = new QueueItem
                    {
                        ClassInfo = fi.ReferencedClass,
                        Level = fi.PrefetchLevel,
                        Prefix = pathExpr
                    };
                    queue.Enqueue(item);
                }
            }

            // TODO - add prefetching
            while (queue.Count > 0)
            {
                var it = queue.Dequeue();

                foreach (var ti in it.ClassInfo.UnifiedTables)
                {
                    additional.Add(ti);

                    foreach (var fi in ti.Fields)
                    {
                        // TODO - this relies on the fact that path expressions
                        // are never reconstructed or broken. We simply share previous prefix
                        // perhaps it's cleaner to Clone() the expression here

                        var extendedExpression = new SoqlPathExpression(it.Prefix, fi.Name);

                        queryExpression.SelectExpressions.Add(extendedExpression);
                        queryExpression.SelectAliases.Add("");

                        if (it.Level >= 1 && fi.PrefetchLevel > 0 && fi.ReferencedClass != null)
                        {
                            var newItem = new QueueItem
                            {
                                ClassInfo = fi.ReferencedClass,
                                Prefix = extendedExpression,
                                Level = (it.Level - 1)
                            };
                            queue.Enqueue(newItem);
                        }
                    }
                }
            }

            queryExpression.WhereClause = null;
            var parameterPos = 0;

            foreach (var fi in tableInfo.Fields)
            {
                if (fi.IsPrimaryKey)
                {
                    //var expr =
                    //    new SoqlBooleanRelationalExpression(
                    //        new SoqlPathExpression("obj", fi.Name),
                    //        new SoqlParameterLiteralExpression(parameterPos),
                    //        SoqlRelationalOperator.Equal);

                    var expr = Soql.FieldEqualsParam(fi.Name, parameterPos);

                    if (parameterPos == 0)
                    {
                        queryExpression.WhereClause = expr;
                    }
                    else
                    {
                        queryExpression.WhereClause = new SoqlBooleanAndExpression(queryExpression.WhereClause, expr);
                    }
                    parameterPos++;
                }
            }

            string query = SoqlToSql(queryExpression, tableInfo.OwnerClass.Schema, false);
            // logger.Debug("Loading statement for table {0}: {1}", tableInfo.NameToken, query);

            loadedTables = additional.ToArray();
            _tableLoadingCache[tableInfo] = new TableLoadingCache(query, loadedTables);
            return query;
        }

        private string GetLoadRefObjectSelectStatement(RelationInfo relationInfo, int masterColumn)
        {
            string query;
            if (_cacheLoadRefObjectSelectStatement[masterColumn].TryGetValue(relationInfo, out query))
                return query;
            string soqlQuery = String.Format("select mt.{0}.* from {2} mt where mt.{1} = {{0}}",
                relationInfo.Table.Fields[masterColumn].Name,
                relationInfo.Table.Fields[1 - masterColumn].Name,
                relationInfo.Name);
            query = SoqlToSql(SoqlParser.ParseQuery(soqlQuery), relationInfo.Schema, false);
            _cacheLoadRefObjectSelectStatement[masterColumn][relationInfo] = query;
            return query;
        }

        private static void UnifyTable(IDictionary<string, TableInfo> tables, TableInfo ti, bool isInherited)
        {
            TableInfo baseTable;
            if (!tables.TryGetValue(ti.DBTableName, out baseTable))
            {
                baseTable = new TableInfo
                {
                    DBTableName = ti.DBTableName
                };

                tables[ti.DBTableName] = baseTable;
                isInherited = false;
            }

            foreach (
                var fi in
                    from FieldInfo fi in ti.Fields
                    let found = baseTable.Fields.Any(fi0 => fi0.Name == fi.Name)
                    where !found
                    select fi)
            {
                baseTable.Fields.Add(fi);
                if (isInherited)
                    fi.IsNullable = true;
            }
        }

        private IDataReader TimedExecuteReader(IDbCommand cmd)
        {
            StopWatch sw = StopWatch.Create();

            try
            {
                sw.Start();
                IDataReader retval = cmd.ExecuteReader(CmdBehavior); //IDataReader retval = cmd.ExecuteReader();
                sw.Stop();
                return retval;
            }
            catch (Exception ex)
            {
                sw.Stop();
                if (Sqllogger.IsErrorEnabled)
                    Sqllogger.Error("Error while executing: {0}\nException: {1}", LogCommand(cmd), ex);
                throw;
            }
            finally
            {
                var timeInSeconds = sw.Seconds;

                if (Statistics != null)
                    Statistics.RegisterQueryTime(timeInSeconds);

                SoodaStatistics.Global.RegisterQueryTime(timeInSeconds);
                if (timeInSeconds > QueryTimeTraceWarn && Sqllogger.IsWarnEnabled)
                {
                    Sqllogger.Warn("Query time: {0} ms. {1}", Math.Round(timeInSeconds*1000.0, 3), LogCommand(cmd));
                }
                else if (timeInSeconds > QueryTimeTraceInfo && Sqllogger.IsInfoEnabled)
                {
                    Sqllogger.Info("Query time: {0} ms: {1}", Math.Round(timeInSeconds*1000.0, 3), LogCommand(cmd));
                }
                else if (Sqllogger.IsTraceEnabled)
                {
                    Sqllogger.Trace("Query time: {0} ms. {1}", Math.Round(timeInSeconds*1000.0, 3), LogCommand(cmd));
                }
            }
        }

        private int TimedExecuteNonQuery(IDbCommand cmd)
        {
            StopWatch sw = StopWatch.Create();

            try
            {
                sw.Start();
                var retval = cmd.ExecuteNonQuery();
                sw.Stop();
                return retval;
            }
            catch (Exception ex)
            {
                sw.Stop();
                if (Sqllogger.IsErrorEnabled)
                {
                    Sqllogger.Error("Error while executing: {0}\nException: {1}", LogCommand(cmd), ex);
                }
                throw;
            }
            finally
            {
                double timeInSeconds = sw.Seconds;

                if (Statistics != null)
                    Statistics.RegisterQueryTime(timeInSeconds);

                SoodaStatistics.Global.RegisterQueryTime(timeInSeconds);
                if (timeInSeconds > QueryTimeTraceWarn && Sqllogger.IsWarnEnabled)
                {
                    Sqllogger.Warn("Non-query time: {0} ms. {1}", Math.Round(timeInSeconds*1000.0, 3), LogCommand(cmd));
                }
                else if (timeInSeconds > QueryTimeTraceInfo && Sqllogger.IsInfoEnabled)
                {
                    Sqllogger.Info("Non-query time: {0} ms. {1}", Math.Round(timeInSeconds*1000.0, 3), LogCommand(cmd));
                }
                else if (Sqllogger.IsTraceEnabled)
                {
                    Sqllogger.Trace("Non-query time: {0} ms.{1}", Math.Round(timeInSeconds*1000.0, 3), LogCommand(cmd));
                }
            }
        }

        public virtual void GenerateDdlForSchema(SchemaInfo schema, TextWriter tw)
        {
            var tables = new Dictionary<string, TableInfo>();
            var processed = new Dictionary<string, string>();

            while (processed.Count < schema.Classes.Count)
            {
                foreach (ClassInfo ci in schema.Classes)
                {
                    if (processed.ContainsKey(ci.Name)) continue;

                    var isInherited = ci.InheritsFromClass != null;
                    if (!isInherited || processed.ContainsKey(ci.InheritsFromClass.Name))
                    {
                        foreach (var ti in ci.UnifiedTables)
                        {
                            UnifyTable(tables, ti, isInherited);
                        }
                        processed.Add(ci.Name, ci.Name);
                    }
                }
            }

            foreach (RelationInfo ri in schema.Relations)
            {
                UnifyTable(tables, ri.Table, false);
            }

            var names = tables.Values.Select(ti => ti.DBTableName).ToList();

            names.Sort();

            foreach (string s in names)
            {
                tw.WriteLine("--- table {0}", s);
                SqlBuilder.GenerateCreateTable(tw, tables[s], CreateTable, null);
            }

            foreach (string s in names)
            {
                SqlBuilder.GeneratePrimaryKey(tw, tables[s], CreateIndex, null);
            }

            foreach (string s in names)
            {
                SqlBuilder.GenerateForeignKeys(tw, tables[s], null);
            }

            foreach (string s in names)
            {
                SqlBuilder.GenerateIndices(tw, tables[s], CreateIndex, null);
            }

            if (schema.GetDataSourceInfo(Name).EnableDynamicFields)
                SqlBuilder.GenerateSoodaDynamicField(tw, null);
        }

        public virtual void CreateTablesInDataSourceForSchema(SchemaInfo schema)
        {
            if (!IsOpen) throw new DataException("DataSource is not opened!!!");

            var tables = new Dictionary<string, TableInfo>();
            var processed = new Dictionary<string, string>();

            while (processed.Count < schema.Classes.Count)
            {
                foreach (var ci in schema.Classes)
                {
                    if (processed.ContainsKey(ci.Name)) continue;

                    var isInherited = ci.InheritsFromClass != null;
                    if (!isInherited || processed.ContainsKey(ci.InheritsFromClass.Name))
                    {
                        foreach (TableInfo ti in ci.UnifiedTables)
                        {
                            UnifyTable(tables, ti, isInherited);
                        }
                        processed.Add(ci.Name, ci.Name);
                    }
                }
            }

            foreach (var ri in schema.Relations)
            {
                UnifyTable(tables, ri.Table, false);
            }

            var names = new ArrayList();

            foreach (TableInfo ti in tables.Values)
            {
                names.Add(ti.DBTableName);
            }

            names.Sort();


            foreach (string s in names)
            {
                ExecuteRaw(SqlBuilder.GenerateCreateTable(tables[s]));
            }

            foreach (string s in names)
            {
                var cmd = SqlBuilder.GeneratePrimaryKey(tables[s]);
                if (!String.IsNullOrEmpty(cmd)) ExecuteRaw(cmd);
            }

            foreach (string s in names)
            {
                var cmd = SqlBuilder.GenerateForeignKeys(tables[s]);
                if (!String.IsNullOrEmpty(cmd)) ExecuteRaw(cmd);
            }
        }

        #region +wash Load All Data

        private string GetAllDataLoadingSelectStatement(ClassInfo classInfo, out TableInfo[] loadedTables)
        {
            TableLoadingCache cache;
            if (_allDataTableLoadingCache.TryGetValue(classInfo, out cache))
            {
                loadedTables = cache.LoadedTables;
                return cache.SelectStatement;
            }

            var queue = new Queue<QueueItem>();
            var additional = new List<TableInfo>(classInfo.UnifiedTables);

            var queryExpression = new SoqlQueryExpression();
            queryExpression.From.Add(classInfo.Name);
            queryExpression.FromAliases.Add("");

            foreach (FieldInfo fi in classInfo.UnifiedFields)
            {
                var pathExpr = new SoqlPathExpression(fi.Name);
                queryExpression.SelectExpressions.Add(pathExpr);
                queryExpression.SelectAliases.Add("");

                if (fi.ReferencedClass != null && fi.PrefetchLevel > 0)
                {
                    var item = new QueueItem
                    {
                        ClassInfo = fi.ReferencedClass,
                        Level = fi.PrefetchLevel,
                        Prefix = pathExpr
                    };
                    queue.Enqueue(item);
                }
            }

            // TODO - add prefetching
            while (queue.Count > 0)
            {
                var it = queue.Dequeue();

                foreach (var ti in it.ClassInfo.UnifiedTables)
                {
                    additional.Add(ti);

                    foreach (var fi in ti.Fields)
                    {
                        // TODO - this relies on the fact that path expressions
                        // are never reconstructed or broken. We simply share previous prefix
                        // perhaps it's cleaner to Clone() the expression here

                        var extendedExpression = new SoqlPathExpression(it.Prefix, fi.Name);

                        queryExpression.SelectExpressions.Add(extendedExpression);
                        queryExpression.SelectAliases.Add("");

                        if (it.Level >= 1 && fi.PrefetchLevel > 0 && fi.ReferencedClass != null)
                        {
                            var newItem = new QueueItem
                            {
                                ClassInfo = fi.ReferencedClass,
                                Prefix = extendedExpression,
                                Level = (it.Level - 1)
                            };
                            queue.Enqueue(newItem);
                        }
                    }
                }
            }

            queryExpression.WhereClause = null;
            var parameterPos = 0;

            foreach (var fi in classInfo.UnifiedFields)
            {
                if (fi.IsPrimaryKey)
                {
                    //var expr =
                    //    new SoqlBooleanRelationalExpression(
                    //        new SoqlPathExpression("obj", fi.Name),
                    //        new SoqlParameterLiteralExpression(parameterPos),
                    //        SoqlRelationalOperator.Equal);

                    var expr = Soql.FieldEqualsParam(fi.Name, parameterPos);

                    if (parameterPos == 0)
                    {
                        queryExpression.WhereClause = expr;
                    }
                    else
                    {
                        queryExpression.WhereClause = new SoqlBooleanAndExpression(queryExpression.WhereClause, expr);
                    }
                    parameterPos++;
                }
            }

            string query = SoqlToSql(queryExpression, classInfo.Schema, false);
            // logger.Debug("Loading statement for table {0}: {1}", tableInfo.NameToken, query);

            loadedTables = additional.ToArray();
            _allDataTableLoadingCache[classInfo] = new TableLoadingCache(query, loadedTables);
            return query;
        }

        #endregion
    }
}