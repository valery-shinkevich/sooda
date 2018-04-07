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
    using System;
    using System.Data;
    using System.Data.OracleClient;
    using QL;
    using Schema;

    public class OracleBuilder : SqlBuilderPositionalArg
    {
        public override string GetDDLCommandTerminator()
        {
            return Environment.NewLine + "GO" + Environment.NewLine + Environment.NewLine;
        }

        public override string GetSQLDataType(FieldInfo fi)
        {
            switch (fi.DataType)
            {
                case FieldDataType.Integer:
                case FieldDataType.BooleanAsInteger:
                case FieldDataType.TimeSpan:
                case FieldDataType.Long:
                    return "integer";

                case FieldDataType.AnsiString:
                    return fi.Size >= 4000 ? "clob" : "varchar2(" + fi.Size + ")";

                case FieldDataType.String:
                    return fi.Size >= 2000 ? "nclob" : "nvarchar2(" + fi.Size + ")";

                case FieldDataType.Decimal:
                    return fi.Size < 0
                        ? "number"
                        : (fi.Precision < 0
                            ? "number(" + fi.Size + ")"
                            : "number(" + fi.Size + "," + fi.Precision + ")");

                case FieldDataType.Double:
                case FieldDataType.Float:
                    return fi.Size < 0
                        ? "float"
                        : (fi.Precision < 0
                            ? "float(" + fi.Size + ")"
                            : "float(" + fi.Size + "," + fi.Precision + ")");

                case FieldDataType.Date:
                case FieldDataType.DateTime:
                    return "date";

                case FieldDataType.Image:
                    return "blob";

                case FieldDataType.Boolean:
                    return "number(1)";

                case FieldDataType.Blob:
                    return "blob";

                default:
                    throw new NotImplementedException(String.Format("Datatype {0} not supported for this database",
                        fi.DataType));
            }
        }

        public override string GetSQLNullable(FieldInfo fi)
        {
            switch (fi.DataType)
            {
                case FieldDataType.AnsiString:
                case FieldDataType.String:
                    if (fi.Size < 4000)
                        // IsNull works fine for Oracle clob, but for nvarchar2 isnull('') = true - contrary to ansi SQL-92
                        return "null";
                    break;
            }

            return base.GetSQLNullable(fi);
        }

        protected override string GetNameForParameter(int pos)
        {
            return ":p" + pos;
        }

        public override SqlTopSupportMode TopSupport
        {
            get { return SqlTopSupportMode.OracleRowNum; }
        }


        public override SqlOuterJoinSyntax OuterJoinSyntax
        {
            get { return SqlOuterJoinSyntax.Oracle; }
        }

        public override string GetSQLOrderBy(FieldInfo fi, bool start)
        {
            switch (fi.DataType)
            {
                case FieldDataType.AnsiString:
                    return fi.Size > 2000 ? (start ? "cast(substr(" : ", 0, 2000) as varchar2(2000))") : "";

                case FieldDataType.String:
                    return fi.Size > 2000 ? (start ? "cast(substr(" : ", 0, 2000) as nvarchar2(2000))") : "";

                default:
                    return "";
            }
        }

        public override string GetAlterTableStatement(TableInfo tableInfo)
        {
            var ident = GetTruncatedIdentifier("PK_" + tableInfo.DBTableName);
            return String.Format("alter table {0} add constraint {1} primary key", tableInfo.DBTableName, ident);
        }

        protected override string AddParameterFromValue(IDbCommand command, object v,
            SoqlLiteralValueModifiers modifiers)
        {
            string paramName = base.AddParameterFromValue(command, v, modifiers);
            var param = (OracleParameter) command.Parameters[paramName];
            if (param.DbType == DbType.String && v.ToString().Length > 2000)
                param.OracleType = OracleType.NClob;
            return paramName;
        }

        public override bool IsFatalException(IDbConnection connection, Exception e)
        {
#if !MONO
            // OracleConnection.ClearAllPools();
#endif
            return false;
        }

        // for Oracle empty string is also null string
        public override bool IsNullValue(object val, FieldInfo fi)
        {
            if (val == null)
                return true;
            if (fi.DataType == FieldDataType.AnsiString || fi.DataType == FieldDataType.String)
                return ((string) val).Length == 0;
            return false;
        }
    }
}