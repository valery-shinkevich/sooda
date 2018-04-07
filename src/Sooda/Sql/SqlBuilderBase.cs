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
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Data;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using ObjectMapper;
    using ObjectMapper.FieldHandlers;
    using QL;
    using Schema;

    public abstract class SqlBuilderBase : ISqlBuilder
    {
        private bool _useSafeLiterals = true;

        private static string HashString(string input)
        {
            int sum = input.Select((t, i) => i*t).Sum();
            sum = sum%65536;
            return sum.ToString("x4");
        }

        public string GetTruncatedIdentifier(string identifier)
        {
            if (identifier.Length <= MaxIdentifierLength)
                return identifier;
            string hash = HashString(identifier);
            return identifier.Substring(0, MaxIdentifierLength - 5) + "_" + hash;
        }

        public bool UseSafeLiterals
        {
            get { return _useSafeLiterals; }
            set { _useSafeLiterals = value; }
        }

        public virtual string GetDDLCommandTerminator()
        {
            return ";" + Environment.NewLine;
        }

        public virtual SqlOuterJoinSyntax OuterJoinSyntax
        {
            get { return SqlOuterJoinSyntax.Ansi; }
        }

        public virtual string StringConcatenationOperator
        {
            get { return "||"; }
        }

        public virtual int MaxIdentifierLength
        {
            get { return 30; }
        }

        public void GenerateCreateTableField(TextWriter xtw, FieldInfo fieldInfo)
        {
            xtw.Write(GenerateCreateTableField(fieldInfo));
        }

        public void GenerateCreateTable(TextWriter xtw, TableInfo tableInfo, string additionalSettings,
            string terminator)
        {
            xtw.WriteLine(GenerateCreateTable(tableInfo));
            TerminateDDL(xtw, additionalSettings, terminator);
        }

        public virtual string GetAlterTableStatement(TableInfo tableInfo)
        {
            return String.Format("alter table {0} add primary key", tableInfo.DBTableName);
        }

        public void GeneratePrimaryKey(TextWriter xtw, TableInfo tableInfo, string additionalSettings, string terminator)
        {
            var key = GeneratePrimaryKey(tableInfo);
            if (!string.IsNullOrEmpty(key)) return;
            xtw.WriteLine(GeneratePrimaryKey(tableInfo));
            TerminateDDL(xtw, additionalSettings, terminator);
        }

        public void GenerateForeignKeys(TextWriter xtw, TableInfo tableInfo, string terminator)
        {
            foreach (FieldInfo fi in tableInfo.Fields)
            {
                if (fi.References != null)
                {
                    xtw.Write("alter table {0} add constraint {1} foreign key ({2}) references {3}({4})",
                        tableInfo.DBTableName, GetConstraintName(tableInfo.DBTableName, fi.DBColumnName),
                        fi.DBColumnName,
                        fi.ReferencedClass.UnifiedTables[0].DBTableName,
                        fi.ReferencedClass.GetFirstPrimaryKeyField().DBColumnName
                        );
                    xtw.Write(terminator ?? GetDDLCommandTerminator());
                }
            }
        }

        public void GenerateIndex(TextWriter xtw, FieldInfo fi, string additionalSettings, string terminator)
        {
            string table = fi.Table.DBTableName;
            xtw.Write("create index {0} on {1} ({2})", GetIndexName(table, fi.DBColumnName), table, fi.DBColumnName);
            TerminateDDL(xtw, additionalSettings, terminator);
        }

        public void GenerateIndices(TextWriter xtw, TableInfo tableInfo, string additionalSettings, string terminator)
        {
            foreach (FieldInfo fi in tableInfo.Fields)
            {
                if (fi.References == null) continue;
                GenerateIndex(xtw, fi, additionalSettings, terminator);
            }
        }

        public void GenerateSoodaDynamicField(TextWriter xtw, string terminator)
        {
            xtw.WriteLine("create table SoodaDynamicField (");
            xtw.WriteLine("\tclass varchar(32) not null,");
            xtw.WriteLine("\tfield varchar(32) not null,");
            xtw.WriteLine("\ttype varchar(32) not null,");
            xtw.WriteLine("\tnullable int not null,");
            xtw.WriteLine("\tfieldsize int null,");
            xtw.WriteLine("\tprecision int null,");
            xtw.WriteLine("\tconstraint PK_SoodaDynamicField primary key (class, field)");
            xtw.Write(')');
            xtw.Write(terminator ?? GetDDLCommandTerminator());
        }

        public virtual string GetConstraintName(string tableName, string foreignKey)
        {
            return GetTruncatedIdentifier(String.Format("FK_{0}_{1}", tableName, foreignKey));
        }

        public virtual string GetIndexName(string tableName, string column)
        {
            return GetTruncatedIdentifier(String.Format("IDX_{0}_{1}", tableName, column));
        }

        public abstract string GetSQLDataType(FieldInfo fi);
        public abstract string GetSQLOrderBy(FieldInfo fi, bool start);

        // ReSharper disable once InconsistentNaming
        public virtual string GetSQLNullable(FieldInfo fi)
        {
            return fi.IsNullable && !fi.IsDynamic ? "null" : "not null";
        }

        protected virtual bool SetDbTypeFromValue(IDbDataParameter parameter, object value,
            SoqlLiteralValueModifiers modifiers)
        {
            DbType dbType;
            if (!ParamTypes.TryGetValue(value.GetType(), out dbType)) return false;
            parameter.DbType = dbType;
            return true;
        }

        private static readonly Dictionary<Type, DbType> ParamTypes = new Dictionary<Type, DbType>();

        static SqlBuilderBase()
        {
            ParamTypes[typeof (SByte)] = DbType.SByte;
            ParamTypes[typeof (Int16)] = DbType.Int16;
            ParamTypes[typeof (Int32)] = DbType.Int32;
            ParamTypes[typeof (Int64)] = DbType.Int64;
            ParamTypes[typeof (Single)] = DbType.Single;
            ParamTypes[typeof (Double)] = DbType.Double;
            ParamTypes[typeof (String)] = DbType.String;
            ParamTypes[typeof (Boolean)] = DbType.Boolean;
            ParamTypes[typeof (Decimal)] = DbType.Decimal;
            ParamTypes[typeof (Guid)] = DbType.Guid;
            ParamTypes[typeof (TimeSpan)] = DbType.Int32;
            ParamTypes[typeof (byte[])] = DbType.Binary;
            ParamTypes[typeof (System.Drawing.Image)] = DbType.Binary;
            ParamTypes[typeof (System.Drawing.Bitmap)] = DbType.Binary;
        }

        public virtual string QuoteIdentifier(string s)
        {
            if (s.Where((c, i) => !(c >= 'A' && c <= 'Z')
                                  && !(c >= 'a' && c <= 'z')
                                  && !(c >= '0' && c <= '9')
                                  && !(c == '_' && i > 0)).Any())
            {
                return "\"" + s + "\"";
            }
            return s;
        }

        public abstract SqlTopSupportMode TopSupport { get; }

        protected bool IsStringSafeForLiteral(string v)
        {
            if (v.Length > 500)
                return false;

            foreach (var ch in v)
            {
                switch (ch)
                {
                    // we are very conservative about what 'safe' means
                    case ' ':
                    case '.':
                    case ',':
                    case '-':
                    case '%':
                    case '_':
                    case '@':
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case 'A':
                    case 'B':
                    case 'C':
                    case 'D':
                    case 'E':
                    case 'F':
                    case 'G':
                    case 'H':
                    case 'I':
                    case 'J':
                    case 'K':
                    case 'L':
                    case 'M':
                    case 'N':
                    case 'O':
                    case 'P':
                    case 'Q':
                    case 'R':
                    case 'S':
                    case 'T':
                    case 'U':
                    case 'V':
                    case 'W':
                    case 'X':
                    case 'Y':
                    case 'Z':
                    case 'a':
                    case 'b':
                    case 'c':
                    case 'd':
                    case 'e':
                    case 'f':
                    case 'g':
                    case 'h':
                    case 'i':
                    case 'j':
                    case 'k':
                    case 'l':
                    case 'm':
                    case 'n':
                    case 'o':
                    case 'p':
                    case 'q':
                    case 'r':
                    case 's':
                    case 't':
                    case 'u':
                    case 'v':
                    case 'w':
                    case 'x':
                    case 'y':
                    case 'z':
                        break;
                    default:
                        return false;
                }
            }
            return true;
        }

        public virtual string EndInsert(string tableName)
        {
            return "";
        }

        public virtual string BeginInsert(string tableName)
        {
            return "";
        }

        protected virtual string AddParameterFromValue(IDbCommand command, object v, SoqlLiteralValueModifiers modifiers)
        {
            IDbDataParameter p = command.CreateParameter();
            p.Direction = ParameterDirection.Input;

            p.ParameterName = GetNameForParameter(command.Parameters.Count);
            if (modifiers != null)
            {
                FieldHandlerFactory.GetFieldHandler(modifiers.DataTypeOverride).SetupDBParameter(p, v);
            }
            else
            {
                SetDbTypeFromValue(p, v, null);
                p.Value = v;
            }
            command.Parameters.Add(p);
            return p.ParameterName;
        }

        public void BuildCommandWithParameters(IDbCommand command, bool append, string query, object[] par, bool isRaw)
        {
            if (append)
            {
                if (command.CommandText == null)
                    command.CommandText = "";
                if (command.CommandText != "")
                    command.CommandText += ";\n";
            }
            else
            {
                command.CommandText = "";
                command.Parameters.Clear();
            }

            var sb = new StringBuilder(query.Length*2);
            var paramNames = new StringCollection();

            for (int i = 0; i < query.Length; ++i)
            {
                char c = query[i];

                if (c == '\'') // locate the string
                {
                    int j = ++i;

                    for (;; ++j)
                    {
                        if (j >= query.Length)
                            throw new ArgumentException("Query has unbalanced quotes");

                        if (query[j] == '\'')
                        {
                            if (j + 1 >= query.Length || query[j + 1] != '\'')
                                break;
                            // double apostrophe
                            j++;
                        }
                    }

                    string stringValue = query.Substring(i, j - i);
                    char modifier = j + 1 < query.Length ? query[j + 1] : ' ';

                    string paramName;

                    switch (modifier)
                    {
                        case 'V':
                            sb.Append('\'');
                            sb.Append(stringValue);
                            sb.Append('\'');
                            j++;
                            break;
                        case 'D':
                            paramName = AddParameterFromValue(command,
                                DateTime.ParseExact(stringValue, "yyyyMMddHH:mm:ss",
                                    CultureInfo.InvariantCulture), null);
                            sb.Append(paramName);
                            j++;
                            break;
                        case 'A':
                            stringValue = stringValue.Replace("''", "'");
                            paramName = AddParameterFromValue(command, stringValue, SoqlLiteralValueModifiers.AnsiString);
                            sb.Append(paramName);
                            j++;
                            break;
                        default:
                            if (!isRaw && (!UseSafeLiterals || !IsStringSafeForLiteral(stringValue)))
                            {
                                stringValue = stringValue.Replace("''", "'");
                                paramName = AddParameterFromValue(command, stringValue, null);

                                sb.Append(paramName);
                            }
                            else
                            {
                                sb.Append('\'');
                                sb.Append(stringValue);
                                sb.Append('\'');
                            }
                            break;
                    }
                    i = j;
                }
                else if (c == '{')
                {
                    c = query[i + 1];

                    if (c == 'L')
                    {
                        // {L:fieldDataTypeName:value

                        int startPos = i + 3;
                        int endPos = query.IndexOf(':', startPos);
                        if (endPos < 0)
                            throw new ArgumentException("Missing ':' in literal specification");

                        SoqlLiteralValueModifiers modifier =
                            SoqlParser.ParseLiteralValueModifiers(query.Substring(startPos, endPos - startPos));
                        FieldDataType fdt = modifier.DataTypeOverride;

                        int valueStartPos = endPos + 1;
                        bool anyEscape = false;

                        for (i = valueStartPos; i < query.Length && query[i] != '}'; ++i)
                        {
                            if (query[i] == '\\')
                            {
                                i++;
                                anyEscape = true;
                            }
                        }

                        string literalValue = query.Substring(valueStartPos, i - valueStartPos);
                        if (anyEscape)
                        {
                            literalValue = literalValue.Replace("\\}", "}");
                            literalValue = literalValue.Replace("\\\\", "\\");
                        }

                        SoodaFieldHandler fieldHandler = FieldHandlerFactory.GetFieldHandler(fdt);
                        object v = fieldHandler.RawDeserialize(literalValue);

                        if (v == null)
                        {
                            sb.Append("null");
                        }
                        else if (UseSafeLiterals && v is int)
                        {
                            sb.Append((int) v);
                        }
                        else if (UseSafeLiterals && v is string && IsStringSafeForLiteral((string) v))
                        {
                            sb.Append('\'');
                            sb.Append((string) v);
                            sb.Append('\'');
                        }
                        else
                        {
                            IDbDataParameter p = command.CreateParameter();
                            p.Direction = ParameterDirection.Input;
                            p.ParameterName = GetNameForParameter(command.Parameters.Count);
                            fieldHandler.SetupDBParameter(p, v);
                            command.Parameters.Add(p);
                            sb.Append(p.ParameterName);
                        }
                    }
                    else if (c >= '0' && c <= '9')
                    {
                        i++;
                        int paramNumber = 0;
                        do
                        {
                            paramNumber = paramNumber*10 + c - '0';
                            c = query[++i];
                        } while (c >= '0' && c <= '9');

                        SoqlLiteralValueModifiers modifiers = null;

                        if (c == ':')
                        {
                            int startPos = i + 1;
                            i = query.IndexOf('}', startPos);
                            if (i < 0)
                                throw new ArgumentException("Missing '}' in parameter specification");
                            modifiers = SoqlParser.ParseLiteralValueModifiers(query.Substring(startPos, i - startPos));
                        }
                        else if (c != '}')
                            throw new ArgumentException("Missing '}' in parameter specification");

                        object v = par[paramNumber];

                        if (v is SoodaObject)
                        {
                            v = ((SoodaObject) v).GetPrimaryKeyValue();
                        }

                        if (v == null)
                        {
                            sb.Append("null");
                        }
                        else if (UseSafeLiterals && v is int)
                        {
                            sb.Append((int) v);
                        }
                        else if (UseSafeLiterals && v is string && IsStringSafeForLiteral((string) v))
                        {
                            sb.Append('\'');
                            sb.Append((string) v);
                            sb.Append('\'');
                        }
                        else
                        {
                            sb.Append(AddNumberedParameter(command, v, modifiers, paramNames, paramNumber));
                        }
                    }
                    else
                    {
                        throw new ArgumentException("Unexpected character in parameter specification");
                    }
                }
                else if (c == '(' || c == ' ' || c == ',' || c == '=' || c == '>' || c == '<' || c == '+' || c == '-' ||
                         c == '*' || c == '/')
                {
                    sb.Append(c);
                    if (i < query.Length - 1)
                    {
                        c = query[i + 1];
                        if (c >= '0' && c <= '9' && !UseSafeLiterals)
                        {
                            var v = 0;
                            double f = 0;
                            double dp = 0;
                            var isDouble = false;
                            do
                            {
                                if (c != '.')
                                {
                                    if (!isDouble)
                                        v = v*10 + c - '0';
                                    else
                                    {
                                        f = f + dp*(c - '0');
                                        dp = dp*0.1;
                                    }
                                }
                                else
                                {
                                    isDouble = true;
                                    f = v;
                                    dp = 0.1;
                                }
                                i++;
                                if (i < query.Length - 1)
                                    c = query[i + 1];
                            } while (((c >= '0' && c <= '9') || c == '.') && (i < query.Length - 1));
                            if (!isDouble)
                            {
                                string paramName = AddParameterFromValue(command, v, null);
                                sb.Append(paramName);
                            }
                            else
                            {
                                string paramName = AddParameterFromValue(command, f, null);
                                sb.Append(paramName);
                            }
                        }
                    }
                }
                else
                {
                    sb.Append(c);
                }
            }
            command.CommandText += sb.ToString();
        }

        public virtual bool IsFatalException(IDbConnection connection, Exception e)
        {
            return true;
        }

        public virtual bool IsNullValue(object val, FieldInfo fi)
        {
            return val == null;
        }

        protected abstract string AddNumberedParameter(IDbCommand command, object v, SoqlLiteralValueModifiers modifiers,
            StringCollection paramNames, int paramNumber);

        protected abstract string GetNameForParameter(int pos);

        //wash{
        public String GenerateForeignKeys(TableInfo tableInfo)
        {
            var sb = new StringBuilder();
            foreach (FieldInfo fi in tableInfo.Fields)
            {
                if (fi.References != null)
                {
                    sb.AppendFormat("alter table {0} add constraint {1} foreign key ({2}) references {3}({4})",
                        tableInfo.DBTableName, GetConstraintName(tableInfo.DBTableName, fi.DBColumnName),
                        fi.DBColumnName,
                        fi.ReferencedClass.UnifiedTables[0].DBTableName,
                        fi.ReferencedClass.GetFirstPrimaryKeyField().DBColumnName
                        );
                    sb.AppendLine();
                    //        sb.Append(GetDDLCommandTerminator());
                }
            }
            return sb.ToString();
        }

        public String GeneratePrimaryKey(TableInfo tableInfo)
        {
            var sb = new StringBuilder();

            var first = true;

            foreach (var fi in tableInfo.Fields)
            {
                if (!fi.IsPrimaryKey) continue;
                if (first)
                {
                    sb.Append(GetAlterTableStatement(tableInfo));
                    sb.Append(" (");
                }
                else
                {
                    sb.Append(", ");
                }
                sb.Append(fi.DBColumnName);
                first = false;
            }
            if (!first)
            {
                sb.AppendLine(") ");
                //sb.Append(GetDDLCommandTerminator());
            }
            return sb.ToString();
        }

        public String GenerateDropPrimaryKey(TableInfo tableInfo)
        {
            var first = true;
            var sb = new StringBuilder();
            foreach (var fi in tableInfo.Fields)
            {
                if (!fi.IsPrimaryKey) continue;
                if (first)
                {
                    sb.AppendFormat("alter table {0} DROP primary key (", tableInfo.DBTableName);
                }
                else
                {
                    sb.Append(", ");
                }
                sb.Append(fi.DBColumnName);
                first = false;
            }
            if (!first)
            {
                sb.AppendLine(")");
                //sb.Append(GetDDLCommandTerminator());
            }
            return sb.ToString();
        }

        public String GenerateCreateTable(TableInfo tableInfo)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("create table {0} (", tableInfo.DBTableName);
            sb.AppendLine();
            var processedFields = new Dictionary<string, bool>();
            for (int i = 0; i < tableInfo.Fields.Count; ++i)
            {
                if (!processedFields.ContainsKey(tableInfo.Fields[i].DBColumnName))
                {
                    sb.Append(GenerateCreateTableField(tableInfo.Fields[i]));
                    if (i == tableInfo.Fields.Count - 1)
                        sb.AppendLine();
                    else
                        sb.AppendLine(",");
                    processedFields.Add(tableInfo.Fields[i].DBColumnName, true);
                }
            }
            sb.Append(')');
            //sb.Append(GetDDLCommandTerminator());
            return sb.ToString();
        }

        public string GenerateCreateTableField(FieldInfo fieldInfo)
        {
            var field =
                string.Format("\t{0} {1} {3} {2}", fieldInfo.DBColumnName, GetSQLDataType(fieldInfo),
                    GetSQLNullable(fieldInfo), GetSQLFilestream(fieldInfo));

            field = AddDefaultValue(fieldInfo, field);
            return field;
        }

        private static string AddDefaultValue(FieldInfo fieldInfo, string field)
        {
            if (!fieldInfo.IsNullable && !fieldInfo.IsPrimaryKey && string.IsNullOrEmpty(fieldInfo.References))
            {
                if (fieldInfo.DataType == FieldDataType.Integer || fieldInfo.DataType == FieldDataType.Money ||
                    fieldInfo.DataType == FieldDataType.Float || fieldInfo.DataType == FieldDataType.Double ||
                    fieldInfo.DataType == FieldDataType.Decimal || fieldInfo.DataType == FieldDataType.Long ||
                    fieldInfo.DataType == FieldDataType.Boolean || fieldInfo.DataType == FieldDataType.BooleanAsInteger)
                {
                    field += " DEFAULT (0)";
                }
                else if (fieldInfo.DataType == FieldDataType.AnsiString || fieldInfo.DataType == FieldDataType.Guid)
                {
                    field += string.Format(" DEFAULT '{0}'", fieldInfo.PrecommitTypedValue);
                }
                else if (fieldInfo.DataType == FieldDataType.String)
                {
                    field += string.Format(" DEFAULT N'{0}'", fieldInfo.PrecommitTypedValue);
                }
            }
            return field;
        }

        private string GetSQLFilestream(FieldInfo fieldInfo)
        {
            return fieldInfo.IsFileStream ? "FILESTREAM" : (fieldInfo.IsRowGuidCol ? "UNIQUE ROWGUIDCOL" : string.Empty);
        }

        //}wash

        // ReSharper disable once InconsistentNaming
        private void TerminateDDL(TextWriter xtw, string additionalSettings, string terminator)
        {
            if (!string.IsNullOrEmpty(additionalSettings))
            {
                xtw.Write(' ');
                xtw.Write(additionalSettings);
            }
            xtw.Write(terminator ?? GetDDLCommandTerminator());
        }
    }
}