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
    using System.Data.SqlClient;
    using System.Globalization;
    using Schema;

    public class SqlServerBuilder : SqlBuilderNamedArg
    {
        public override string StringConcatenationOperator
        {
            get { return "+"; }
        }

        public override string GetDDLCommandTerminator()
        {
            return Environment.NewLine + "GO" + Environment.NewLine + Environment.NewLine;
        }

        public override string GetSQLDataType(FieldInfo fi)
        {
            return GetSQLDataType(fi.DataType, fi.Size, fi.Precision, fi.IsIdentity);
        }

        public string GetSQLDataType(FieldDataType datatype, int size, int precision, bool identity)
        {
            switch (datatype)
            {
                case FieldDataType.Integer:
                    return identity ? "int identity" : "int";

                case FieldDataType.Money:
                    return "money";

                case FieldDataType.AnsiString:
                    return "varchar(" + (size > 4000 || size < 0 ? "max" : size.ToString(CultureInfo.InvariantCulture)) +
                           ")";

                case FieldDataType.String:
                    return "nvarchar(" + (size > 4000 || size < 0 ? "max" : size.ToString(CultureInfo.InvariantCulture)) +
                           ")";

                case FieldDataType.Decimal:
                    return size < 0
                        ? "decimal"
                        : (precision < 0 ? "decimal(" + size + ")" : "decimal(" + size + "," + precision + ")");

                case FieldDataType.Guid:
                    return "uniqueidentifier";

                case FieldDataType.Double:
                    return size < 0
                        ? "double"
                        : (precision < 0 ? "double(" + size + ")" : "double(" + size + "," + precision + ")");

                case FieldDataType.Float:
                    return size < 0
                        ? "float"
                        : (precision < 0 ? "float(" + size + ")" : "float(" + size + "," + precision + ")");

                case FieldDataType.DateTime:
                    return "datetime";

                case FieldDataType.Date:
                    return "date";

                case FieldDataType.Image:
                    return "varbinary(max)";

                case FieldDataType.Long:
                    return "bigint";

                case FieldDataType.BooleanAsInteger:
                    return "int";

                case FieldDataType.TimeSpan:
                    return "int";

                case FieldDataType.Boolean:
                    return "bit";

                case FieldDataType.RowVersion:
                    return "timestamp";

                case FieldDataType.Blob:
                    return "varbinary(max)";

                default:
                    throw new NotImplementedException(String.Format("Datatype {0} not supported for this database",
                        datatype));
            }
        }

        protected override string GetNameForParameter(int pos)
        {
            return "@p" + pos;
        }

        public override string QuoteIdentifier(string s)
        {
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (!(c >= 'A' && c <= 'Z')
                    && !(c >= 'a' && c <= 'z')
                    && !(c >= '0' && c <= '9')
                    && c != '_')
                    return "[" + s + "]";
            }
            return s;
        }

        public override SqlTopSupportMode TopSupport
        {
            get
            {
                return SqlTopSupportMode.MsSqlRowNum;
                // return SqlTopSupportMode.MSSQL2012;
            }
        }

        public override int MaxIdentifierLength
        {
            get { return 128; }
        }

        public override string EndInsert(string tableName)
        {
            return "set identity_insert " + tableName + " off ";
        }

        public override string BeginInsert(string tableName)
        {
            return "set identity_insert " + tableName + " on ";
        }

        public override string GetSQLOrderBy(FieldInfo fi, bool start)
        {
            switch (fi.DataType)
            {
                case FieldDataType.AnsiString:
                    if (fi.Size > 4000)
                        return start ? "convert(varchar(3999), " : ")";
                    return "";

                case FieldDataType.String:
                    if (fi.Size > 4000)
                        return start ? "convert(nvarchar(3999), " : ")";
                    return "";

                default:
                    return "";
            }
        }

        public override string GetAlterTableStatement(TableInfo tableInfo)
        {
            string ident = GetTruncatedIdentifier(String.Format("PK_{0}", tableInfo.DBTableName));
            return String.Format("alter table {0} add constraint {1} primary key", tableInfo.DBTableName, ident);
        }

        public override bool IsFatalException(IDbConnection connection, Exception e)
        {
            SqlConnection.ClearAllPools();
            return false;
        }
    }
}