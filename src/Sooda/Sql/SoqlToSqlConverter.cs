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

using Sooda.QL;
using Sooda.Schema;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;

namespace Sooda.Sql
{
    /// <summary>
    /// Summary description for SoqlToSqlConverter.
    /// </summary>
    public class SoqlToSqlConverter : SoqlPrettyPrinter
    {
        private readonly ISqlBuilder _builder;
        private bool _generatingOrderBy;

        public bool DisableBooleanExpansion = false;
        public bool GenerateColumnAliases = true;
        public bool UpperLike = false;
        public bool GenerateUniqueAliases = false;
        public int UniqueColumnId = 0;
        public StringCollection ActualFromAliases = new StringCollection();
        public List<StringCollection> FromJoins = new List<StringCollection>();
        public StringCollection WhereJoins = new StringCollection();

        public SoqlToSqlConverter(TextWriter output, SchemaInfo schema, ISqlBuilder builder)
            : base(output)
        {
            Schema = schema;
            _builder = builder;
        }

        private void StartClause()
        {
            if (IndentOutput)
            {
                Output.WriteLine();
                WriteIndentString();
            }
            else
            {
                Output.Write(' ');
            }
        }

        private IFieldContainer GenerateTableJoins(SoqlPathExpression expr, out string p, out string firstTableAlias)
        {
            // logger.Debug("GenerateTableJoins({0})", expr);
            IFieldContainer currentContainer;
            SoqlPathExpression e;

            // make the list bi-directional

            for (e = expr; e.Left != null; e = e.Left)
            {
                e.Left.Next = e;
            }

            SoqlPathExpression firstToken = e;
            SoqlPathExpression startingToken;

            // check if the first name on the list is an alias in current context
            if (TableAliases.ContainsKey(firstToken.PropertyName))
            {
                string ta = TableAliases[firstToken.PropertyName];

                currentContainer = Schema.FindContainerByName(ta);
                p = firstToken.PropertyName;
                startingToken = firstToken.Next;
            }
            else
            {
                if (Parent != null && Parent.TableAliases.ContainsKey(firstToken.PropertyName))
                {
                    return Parent.GenerateTableJoins(expr, out p, out firstTableAlias);
                }

                // artificial first token

                // TODO - find default container for current field

                currentContainer = FindStartingContainerByFieldName(firstToken.PropertyName, out p);
                startingToken = firstToken;
            }

            string lastTableAlias = GetTableAliasForExpressionPrefix(p);
            firstTableAlias = lastTableAlias;

            bool nullable = false;

            for (SoqlPathExpression currentToken = startingToken;
                currentToken != null;
                currentToken = currentToken.Next)
            {
                lastTableAlias = GetTableAliasForExpressionPrefix(p);
                if (currentContainer == null) continue;
                FieldInfo fi = currentContainer.FindFieldByName(currentToken.PropertyName);
                if (fi == null)
                {
                    throw new Exception(String.Format("{0} not found in {1}", currentToken.PropertyName,
                        currentContainer.Name));
                }

                if (p.Length > 0)
                    p += '.';
                p += currentToken.PropertyName;

                if (fi.ReferencedClass == null)
                {
                    currentContainer = null;
                    continue;
                }

                if (fi.IsNullable)
                    nullable = true;

                if (fi.Table.OrdinalInClass > 0)
                {
                    string extPrefix = AddPrimaryKeyJoin(firstTableAlias, (ClassInfo) currentContainer, lastTableAlias,
                        fi);
                    AddRefJoin(firstTableAlias, p, extPrefix, fi, nullable);
                }
                else
                {
                    AddRefJoin(firstTableAlias, p, lastTableAlias, fi, nullable);
                }
                currentContainer = fi.ReferencedClass;
            }

            return currentContainer;
        }

        private void ReplaceEmbeddedSoql(string s0)
        {
            int p;
            string s = s0;
            const string parameterStart = "SOQL{{";
            const string parameterEnd = "}}";

            p = s.IndexOf(parameterStart);
            while (p != -1)
            {
                int r = s.IndexOf(parameterEnd, p);
                if (r == -1)
                    break;

                string after = s.Substring(r + parameterEnd.Length);
                string before = s.Substring(0, p);
                Output.Write(before);
                string soqlString = s.Substring(p + parameterStart.Length, r - p - parameterStart.Length);
                SoqlExpression expr = SoqlParser.ParseExpression(soqlString);
                expr.Accept(this);

                s = after;
                p = s.IndexOf(parameterStart);
            }
            Output.Write(s);
        }

        public override void Visit(SoqlRawExpression v)
        {
            ReplaceEmbeddedSoql(v.Text);
        }

        public void OutputLiteral(object literalValue, SoqlLiteralValueModifiers modifier)
        {
            if (literalValue is String)
            {
                Output.Write('\'');
                Output.Write(((string) literalValue).Replace("'", "''"));
                Output.Write('\'');
                if (modifier != null && modifier.DataTypeOverride == FieldDataType.AnsiString)
                {
                    Output.Write('A');
                }
            }
            else if (literalValue is DateTime)
            {
                Output.Write('\'');
                Output.Write(((DateTime) literalValue).ToString("yyyyMMddHH:mm:ss"));
                Output.Write("'D");
            }
            else if (literalValue == null)
            {
                Output.Write("null");
            }
            else
            {
                // this is to output the decimal point as dot and not comma under Polish locale
                IFormattable formattable = literalValue as IFormattable;
                if (formattable != null)
                    Output.Write(formattable.ToString(null, System.Globalization.CultureInfo.InvariantCulture));
                else
                    Output.Write(literalValue);
            }
        }

        public override void Visit(SoqlLiteralExpression v)
        {
            OutputLiteral(v.LiteralValue, v.Modifiers);
        }

        public override void Visit(SoqlBooleanLiteralExpression v)
        {
            if (DisableBooleanExpansion)
            {
                Output.Write(v.Value ? '1' : '0');
            }
            else
            {
                Output.Write(v.Value ? "(1=1)" : "(0=1)");
            }
        }

        public override void Visit(Sooda.QL.TypedWrappers.SoqlBooleanWrapperExpression v)
        {
            if (DisableBooleanExpansion || v.InnerExpression is SoqlBooleanExpression)
            {
                base.Visit(v);
            }
            else
            {
                Output.Write('(');
                base.Visit(v);
                Output.Write(" <> 0)");
            }
        }


        private void OutputColumn(string tableAlias, FieldInfo fi)
        {
            if (tableAlias.Length > 0)
            {
                Output.Write(tableAlias);
                Output.Write('.');
            }
            Output.Write(fi.DBColumnName);
        }

        public override void Visit(SoqlAsteriskExpression v)
        {
            string p;
            string firstTableAlias;
            IFieldContainer currentContainer;

            if (Parent != null && v.Left == null)
            {
                Output.Write('*');
                return;
            }

            if (v.Left != null)
            {
                currentContainer = GenerateTableJoins(v.Left, out p, out firstTableAlias);
            }
            else
            {
                p = ExpressionPrefixToTableAlias[_query.From[0]];
                currentContainer = Schema.FindContainerByName(_query.From[0]);
                firstTableAlias = GetTableAliasForExpressionPrefix(p);
            }

            bool first = true;
            foreach (FieldInfo fi in currentContainer.GetAllFields())
            {
                if (!first)
                {
                    if (IndentOutput)
                    {
                        Output.WriteLine(',');
                        WriteIndentString();
                        Output.Write("         ");
                    }
                    else
                    {
                        Output.Write(',');
                    }
                }
                string extPrefix = GetTableAliasForExpressionPrefix(p);
                if (fi.Table.OrdinalInClass > 0)
                    extPrefix = AddPrimaryKeyJoin(firstTableAlias, (ClassInfo) currentContainer, extPrefix, fi);
                OutputColumn(extPrefix, fi);
                if (GenerateColumnAliases)
                {
                    Output.Write(" as ");
                    Output.Write(_builder.QuoteIdentifier(fi.Name));
                }
                first = false;
            }
        }

        private void OutputSoodaClassCase(ClassInfo ci)
        {
            if (ci.IsAbstractClass())
                return;

            Output.Write(" when ");
            switch (ci.SubclassSelectorField.DataType)
            {
                case FieldDataType.String:
                    Output.Write("N'");
                    Output.Write(ci.SubclassSelectorValue);
                    Output.Write('\'');
                    break;
                case FieldDataType.AnsiString:
                    Output.Write('\'');
                    Output.Write(ci.SubclassSelectorValue);
                    Output.Write('\'');
                    break;
                default:
                    Output.Write(ci.SubclassSelectorValue);
                    break;
            }
            Output.Write(" then '");
            Output.Write(ci.Name);
            Output.Write('\'');
        }

        public override void Visit(SoqlSoodaClassExpression v)
        {
            ClassInfo currentClass;
            string p;

            if (v.Path != null)
            {
                string firstTableAlias;
                IFieldContainer container = GenerateTableJoins(v.Path, out p, out firstTableAlias);
                currentClass = container as ClassInfo;
            }
            else
            {
                if (_query.From.Count != 1)
                    throw new Exception("Ambiguous SoodaClass!");
                p = ExpressionPrefixToTableAlias[_query.From[0]];
                currentClass = (ClassInfo) Schema.FindContainerByName(_query.From[0]);
            }

            if (currentClass == null) return;
            var subclasses = currentClass.GetSubclassesForSchema(Schema);

            if (subclasses.Count == 0)
            {
                Output.Write('\'');
                Output.Write(currentClass.Name);
                Output.Write('\'');
            }
            else
            {
                Output.Write("(case ");
                Output.Write(p);
                Output.Write('.');
                Output.Write(currentClass.SubclassSelectorField.DBColumnName);
                OutputSoodaClassCase(currentClass);

                foreach (var subci in subclasses) OutputSoodaClassCase(subci);
                Output.Write(" else null end)");
            }
        }

        private SoqlToSqlConverter CreateSubconverter()
        {
            var subconverter = new SoqlToSqlConverter(Output, Schema, _builder)
            {
                Parent = this,
                IndentLevel = IndentLevel,
                IndentStep = IndentStep,
                UpperLike = UpperLike
            };
            return subconverter;
        }

        private SoqlQueryExpression CreateCollectionQuery(ClassInfo currentClass, string p,
            CollectionOnetoManyInfo col1N, SoqlExpression selectExpression, SoqlExpression needle)
        {
            SoqlBooleanExpression where = new SoqlBooleanRelationalExpression(
                new SoqlPathExpression(col1N.ForeignField2.Name),
                new SoqlPathExpression(new SoqlPathExpression(p.Split('.')),
                    currentClass.GetFirstPrimaryKeyField().Name),
                SoqlRelationalOperator.Equal);

            if (!string.IsNullOrEmpty(col1N.Where))
                where &= SoqlParser.ParseWhereClause(col1N.Where);

            var fromAlias = string.Empty;
            if (needle != null)
            {
                var nq = needle as SoqlQueryExpression;
                if (nq != null
                    && nq.StartIdx == 0 && nq.PageCount == -1 && nq.SelectExpressions.Count == 0
                    && nq.From.Count == 1 && nq.From[0] == col1N.ClassName
                    && nq.Having == null && nq.GroupByExpressions.Count == 0)
                {
                    fromAlias = nq.FromAliases[0];
                    if (nq.WhereClause != null)
                        where &= nq.WhereClause;
                }
                else
                {
                    if (col1N.Class.GetPrimaryKeyFields().Length >= 2)
                        throw new NotSupportedException("Contains() with composite primary key");
                    var collection = new SoqlExpressionCollection {needle};
                    where &= new SoqlBooleanInExpression(
                        new SoqlPathExpression(col1N.Class.GetFirstPrimaryKeyField().Name),
                        collection);
                }
            }

            var query = new SoqlQueryExpression();
            query.SelectExpressions.Add(selectExpression);
            query.SelectAliases.Add("");
            query.From.Add(col1N.ClassName);
            query.FromAliases.Add(fromAlias);
            query.WhereClause = where;
            return query;
        }

        public override void Visit(SoqlCountExpression v)
        {
            ClassInfo currentClass;
            string p;
            string firstTableAlias;

            if (v.Path != null)
            {
                IFieldContainer container = GenerateTableJoins(v.Path, out p, out firstTableAlias);
                currentClass = container as ClassInfo;
            }
            else
            {
                currentClass = FindClassByCollectionName(v.CollectionName, out p);
            }

            CollectionOnetoManyInfo col1n = currentClass.FindCollectionOneToMany(v.CollectionName);
            if (col1n != null)
            {
                SoqlQueryExpression query = CreateCollectionQuery(currentClass, p, col1n,
                    new SoqlFunctionCallExpression("count", new SoqlAsteriskExpression()), null);

                query.Accept(this);
                return;
            }

            CollectionManyToManyInfo colnn = currentClass.FindCollectionManyToMany(v.CollectionName);
            if (colnn != null)
            {
                RelationInfo ri = colnn.GetRelationInfo();

                Output.Write("(select count(*) from ");
                OutputTableFrom(ri.Table, "");
                Output.Write(" where ");
                Output.Write(ri.Table.Fields[1 - colnn.MasterField].DBColumnName);
                Output.Write('=');
                OutputColumn(GetTableAliasForExpressionPrefix(p), currentClass.GetFirstPrimaryKeyField());
                Output.Write(')');
                return;
            }

            throw new Exception("Unknown collection " + v.CollectionName + " in " + currentClass.Name);
        }

        private void OutputTableFrom(TableInfo tableInfo, string tableAlias)
            //private void SetTableUsageHint(TextWriter Output, TableInfo tableInfo)
        {
            Output.Write(_builder.QuoteIdentifier(tableInfo.DBTableName));
            if (tableAlias.Length > 0)
            {
                Output.Write(' ');
                Output.Write(tableAlias);
            }
            if (_builder.OuterJoinSyntax != SqlOuterJoinSyntax.Oracle &&
                tableInfo.TableUsageType == TableUsageType.Dictionary)
            {
                Output.Write(" WITH (NOLOCK) ");
            }
        }

        private string GetTableUsageHint(TableInfo tableInfo)
        {
            if (_builder.OuterJoinSyntax != SqlOuterJoinSyntax.Oracle &&
                tableInfo.TableUsageType == TableUsageType.Dictionary)
            {
                return " WITH (NOLOCK) ";
            }
            return "";
        }

        public override void Visit(SoqlContainsExpression v)
        {
            ClassInfo currentClass;
            string p;
            if (v.Path != null)
            {
                string firstTableAlias;
                IFieldContainer container = GenerateTableJoins(v.Path, out p, out firstTableAlias);
                currentClass = container as ClassInfo;
            }
            else
            {
                currentClass = FindClassByCollectionName(v.CollectionName, out p);
            }

            if (currentClass == null)
                throw new NullReferenceException("Unknown class collection " + v.CollectionName);

            CollectionOnetoManyInfo col1N = currentClass.FindCollectionOneToMany(v.CollectionName);
            if (col1N != null)
            {
                SoqlQueryExpression query = CreateCollectionQuery(currentClass, p, col1N, new SoqlAsteriskExpression(),
                    v.Expr);
                var subExists = new SoqlExistsExpression(query);
                subExists.Accept(this);
                return;
            }

            CollectionManyToManyInfo colnn = currentClass.FindCollectionManyToMany(v.CollectionName);
            if (colnn != null)
            {
                RelationInfo ri = colnn.GetRelationInfo();

                Output.Write("exists (select * from ");
                OutputTableFrom(ri.Table, "");
                Output.Write(" where ");
                Output.Write(ri.Table.Fields[1 - colnn.MasterField].DBColumnName);
                Output.Write('=');
                OutputColumn(GetTableAliasForExpressionPrefix(p), currentClass.GetFirstPrimaryKeyField());
                Output.Write(" and ");
                Output.Write(ri.Table.Fields[colnn.MasterField].DBColumnName);
                Output.Write(" in ");
                if (!(v.Expr is SoqlQueryExpression))
                    Output.Write('(');
                v.Expr.Accept(this);
                if (!(v.Expr is SoqlQueryExpression))
                    Output.Write(')');
                Output.Write(')');
                return;
            }

            throw new Exception("Unknown collection " + v.CollectionName + " in " + currentClass.Name);
        }

        private FieldInfo VisitAndGetFieldInfo(SoqlPathExpression v, bool doOutput)
        {
            if (v.Left != null && v.Left.Left == null)
            {
                string firstToken = v.Left.PropertyName;
                string secondToken = v.PropertyName;

                ClassInfo ci = Schema.FindClassByName(firstToken);
                if (ci != null && ci.Constants != null)
                {
                    foreach (ConstantInfo constInfo in ci.Constants)
                    {
                        if (constInfo.Name == secondToken)
                        {
                            switch (ci.GetFirstPrimaryKeyField().DataType)
                            {
                                case FieldDataType.Integer:
                                    OutputLiteral(Convert.ToInt32(constInfo.Key), null);
                                    break;
                                case FieldDataType.String:
                                    OutputLiteral(constInfo.Key, null);
                                    break;
                                case FieldDataType.AnsiString:
                                    OutputLiteral(constInfo.Key, SoqlLiteralValueModifiers.AnsiString);
                                    break;
                                default:
                                    throw new NotSupportedException("Constant of type: " +
                                                                    ci.GetFirstPrimaryKeyField().DataType +
                                                                    " not supported in SOQL");
                            }
                            return null;
                        }
                    }
                }
            }

            IFieldContainer currentContainer;
            string firstTableAlias;
            string p;

            if (v.Left != null)
            {
                currentContainer = GenerateTableJoins(v.Left, out p, out firstTableAlias);
            }
            else
            {
                currentContainer = FindStartingContainerByFieldName(v.PropertyName, out p);
                //firstTableAlias = p;
                firstTableAlias = GetTableAliasForExpressionPrefix(p);
            }

            FieldInfo fi = currentContainer.FindFieldByName(v.PropertyName);
            if (fi == null)
            {
                throw new Exception(String.Format("{0} not found in {1}", v.PropertyName, currentContainer.Name));
            }

            if (doOutput)
            {
                if (_generatingOrderBy)
                    Output.Write(_builder.GetSQLOrderBy(fi, true));
                string extPrefix = GetTableAliasForExpressionPrefix(p);
                if (fi.Table.OrdinalInClass > 0)
                    extPrefix = AddPrimaryKeyJoin(firstTableAlias, (ClassInfo) currentContainer, extPrefix, fi);
                OutputColumn(extPrefix, fi);
                if (_generatingOrderBy)
                    Output.Write(_builder.GetSQLOrderBy(fi, false));
            }
            return fi;
        }

        public override void Visit(SoqlPathExpression v)
        {
            VisitAndGetFieldInfo(v, true);
        }

        private void OutputScalar(SoqlExpression expr)
        {
            if (expr is SoqlBooleanExpression && !(expr is SoqlRawExpression))
            {
                Output.Write("case when ");
                expr.Accept(this);
                Output.Write(" then 1 else 0 end");
            }
            else
            {
                expr.Accept(this);
            }
        }

        private void OutputColumns(SoqlQueryExpression v, bool onlyAliases)
        {
            if (v.SelectExpressions.Count == 0)
            {
                // simplified query - emit the primary key here

                var ci = Schema.FindClassByName(v.From[0]);
                var first = true;

                foreach (FieldInfo pkfi in ci.GetPrimaryKeyFields())
                {
                    if (!first)
                        Output.Write(", ");
                    if (!onlyAliases)
                        OutputColumn(ActualFromAliases[0], pkfi);
                    if (GenerateColumnAliases || GenerateUniqueAliases)
                    {
                        if (!onlyAliases)
                            Output.Write(" as ");
                        Output.Write(_builder.QuoteIdentifier(pkfi.Name));
                    }
                    first = false;
                }
            }
            else
            {
                for (int i = 0; i < v.SelectExpressions.Count; ++i)
                {
                    if (i > 0)
                    {
                        if (IndentOutput)
                        {
                            Output.WriteLine(',');
                            WriteIndentString();
                            Output.Write("         ");
                        }
                        else
                        {
                            Output.Write(',');
                        }
                    }
                    SoqlExpression expr = v.SelectExpressions[i];
                    if (!onlyAliases)
                        OutputScalar(expr);
                    if (v.SelectAliases[i].Length > 0)
                    {
                        if (!onlyAliases)
                            Output.Write(" as ");
                        Output.Write(_builder.QuoteIdentifier(v.SelectAliases[i]));
                    }
                    else if (GenerateColumnAliases)
                    {
                        var aliasProvider = expr as ISoqlSelectAliasProvider;
                        if (aliasProvider != null)
                        {
                            if (!onlyAliases)
                                Output.Write(" as ");
                            aliasProvider.WriteDefaultSelectAlias(Output);
                        }
                    }
                    else if (GenerateUniqueAliases)
                    {
                        if (!onlyAliases)
                            Output.Write(" as");
                        Output.Write(" col_{0}", UniqueColumnId++);
                    }
                }
            }
        }

        private void WriteSelect()
        {
            WriteIndentString();
            Output.Write(IndentOutput ? "select   " : "select ");
        }

        private void OutputOrderBy(SoqlQueryExpression v)
        {
            Output.Write("order by ");
            if (v.OrderByExpressions.Count > 0)

            {
                _generatingOrderBy = true;
                for (int i = 0; i < v.OrderByExpressions.Count; ++i)

                {
                    if (i > 0)
                        Output.Write(", ");
                    OutputScalar(v.OrderByExpressions[i]);
                    Output.Write(' ');
                    Output.Write(v.OrderByOrder[i]);
                }
                _generatingOrderBy = false;
            }
            else
            {
                FieldInfo[] pkfis = Schema.FindClassByName(v.From[0]).GetPrimaryKeyFields();
                for (int i = 0; i < pkfis.Length; i++)

                {
                    if (i > 0)
                        Output.Write(", ");
                    OutputColumn(ActualFromAliases[0], pkfis[i]);
                }
            }
        }

        private void DoVisit(SoqlQueryExpression v)
        {
            IndentLevel++;
            try
            {
                WriteSelect();
                if (v.StartIdx != 0 || v.PageCount != -1)
                {
                    if (_builder.TopSupport == SqlTopSupportMode.OracleRowNum ||
                        _builder.TopSupport == SqlTopSupportMode.MsSqlRowNum)
                    {
                        GenerateUniqueAliases = true;
                        OutputColumns(v, true);
                        StartClause();
                        Output.Write("from (");
                        IndentLevel++;
                        WriteSelect();
                        Output.Write(' ');

                        if (_builder.TopSupport == SqlTopSupportMode.OracleRowNum)
                        {
                            Output.Write("rownum as rownum_, pgo.* from (");
                            IndentLevel++;
                            WriteSelect();
                            Output.Write(' ');
                        }
                        UniqueColumnId = 0;
                    }
                }

                if (v.Distinct)
                    Output.Write("distinct ");
                OutputColumns(v, false);


                if (v.StartIdx != 0 || v.PageCount != -1)
                {
                    if (_builder.TopSupport == SqlTopSupportMode.MsSqlRowNum)
                    {
                        Output.Write(", ROW_NUMBER() over (");
                        OutputOrderBy(v);
                        Output.Write(") as rownum_");
                    }
                }

                var sw = new StringWriter();
                TextWriter oldOutput = Output;
                Output = sw;
                if (v.GroupByExpressions.Count > 0)
                {
                    StartClause();
                    Output.Write("group by ");
                    for (int i = 0; i < v.GroupByExpressions.Count; ++i)
                    {
                        if (i > 0)
                            Output.Write(", ");
                        OutputScalar(v.GroupByExpressions[i]);
                    }
                }
                if (v.Having != null)
                {
                    StartClause();
                    Output.Write("having ");
                    v.Having.Accept(this);
                }
                if (v.OrderByExpressions.Count > 0
                    && ((v.StartIdx == 0 && v.PageCount == -1) || _builder.TopSupport != SqlTopSupportMode.MsSqlRowNum))
                {
                    StartClause();
                    OutputOrderBy(v);
                }

                if (_builder.TopSupport == SqlTopSupportMode.MsSql2012 && (v.StartIdx != 0 || v.PageCount != -1))
                {
                    if (v.OrderByExpressions.Count == 0)

                    {
                        StartClause();
                        OutputOrderBy(v);
                    }
                    StartClause();
                    Output.Write("offset ");
                    Output.Write(v.StartIdx);
                    Output.Write(" rows");
                    if (v.PageCount != -1)

                    {
                        Output.Write(" fetch next ");
                        Output.Write(v.PageCount);
                        Output.Write(" rows only");
                    }
                }

                if (v.PageCount != -1 && _builder.TopSupport == SqlTopSupportMode.MySqlLimit)
                {
                    StartClause();
                    Output.Write("limit ");
                    Output.Write(v.StartIdx);
                    Output.Write(",");
                    Output.Write(v.PageCount);
                }

                var whereSw = new StringWriter();
                Output = whereSw;
                SoqlBooleanExpression limitedWhere = v.WhereClause;
                for (int i = 0; i < v.From.Count; ++i)
                {
                    ClassInfo ci = Schema.FindClassByName(v.From[i]);
                    if (ci == null)
                        continue;
                    SoqlBooleanExpression restriction =
                        Soql.ClassRestriction(new SoqlPathExpression(ActualFromAliases[i]), Schema, ci);
                    if (restriction != null)
                    {
                        limitedWhere = limitedWhere == null
                            ? restriction
                            : new SoqlBooleanAndExpression(limitedWhere, restriction);
                    }
                }
                if (limitedWhere != null || WhereJoins.Count > 0)
                {
                    if (IndentOutput)
                    {
                        Output.WriteLine();
                        WriteIndentString();
                        Output.Write("where ");
                    }
                    else
                    {
                        Output.Write(" where ");
                    }
                    bool first = true;
                    if (limitedWhere != null)
                    {
                        limitedWhere.Accept(this);
                        first = false;
                    }
                    foreach (string s in WhereJoins)
                    {
                        if (!first)
                            Output.Write(" and ");
                        Output.Write(s);
                        first = false;
                    }
                }
                Output = oldOutput;
                if (IndentOutput)
                {
                    // output FROM here
                    Output.WriteLine();
                    WriteIndentString();
                    Output.Write("from ");
                }
                else
                {
                    Output.Write(" from ");
                }
                for (int i = 0; i < v.From.Count; ++i)
                {
                    if (i > 0)
                    {
                        if (IndentOutput)
                        {
                            Output.WriteLine(',');
                            WriteIndentString();
                            Output.Write(" ");
                        }
                        else
                        {
                            Output.Write(',');
                        }
                    }
                    TableInfo tbl = Schema.FindContainerByName(v.From[i]).GetAllFields()[0].Table;
                    OutputTableFrom(tbl, ActualFromAliases[i]);
                    foreach (string s in FromJoins[i])
                    {
                        if (IndentOutput)
                        {
                            Output.WriteLine();
                            WriteIndentString();
                            Output.Write(" ");
                        }
                        else
                        {
                            Output.Write(' ');
                        }
                        Output.Write(s);
                    }
                }
                Output.Write(whereSw.ToString());
                Output.Write(sw.ToString());

                if (v.StartIdx != 0 || v.PageCount != -1)
                {
                    if (_builder.TopSupport == SqlTopSupportMode.OracleRowNum ||
                        _builder.TopSupport == SqlTopSupportMode.MsSqlRowNum)
                    {
                        if (_builder.TopSupport == SqlTopSupportMode.OracleRowNum)
                        {
                            if (IndentOutput)
                            {
                                Output.WriteLine();
                                WriteIndentString();
                            }
                            Output.Write(") pgo ");
                            IndentLevel--;
                        }
                        if (IndentOutput)
                        {
                            Output.WriteLine();
                            WriteIndentString();
                        }
                        Output.Write(") pg where rownum_ ");
                        if (v.PageCount != -1)
                        {
                            Output.Write("between ");
                            Output.Write(v.StartIdx + 1);
                            Output.Write(" and ");
                            Output.Write(v.StartIdx + v.PageCount);
                        }
                        else
                        {
                            Output.Write("> ");
                            Output.Write(v.StartIdx);
                        }
                        IndentLevel--;
                    }
                }
            }
            finally
            {
                IndentLevel--;
            }
        }

        public override void Visit(SoqlQueryExpression v)
        {
            SoqlToSqlConverter conv = this;
            if (_query != null)
            {
                Output.Write('(');
                if (IndentOutput)
                    Output.WriteLine();
                conv = CreateSubconverter();
            }
            conv.Init(v);
            conv.DoVisit(v);
            if (conv != this)
                Output.Write(')');
        }

        public SoqlToSqlConverter Parent;
        public SchemaInfo Schema;

        public StringDictionary ExpressionPrefixToTableAlias = new StringDictionary();
        public StringDictionary TableAliases = new StringDictionary();
        public int CurrentTablePrefix = 0;

        private SoqlQueryExpression _query;

        public void Init(SoqlQueryExpression query)
        {
            _query = query;

            var killPrefixes = new StringCollection();

            for (int i = 0; i < query.From.Count; ++i)
            {
                FromJoins.Add(new StringCollection());

                string table = query.From[i];
                string alias = query.FromAliases[i];

                if (alias.Length == 0)
                {
                    alias = GetNextTablePrefix();
                }
                ActualFromAliases.Add(alias);

                if (!ExpressionPrefixToTableAlias.ContainsKey(alias))
                {
                    ExpressionPrefixToTableAlias.Add(alias, alias);
                }
                else
                {
                    killPrefixes.Add(alias);
                }

                if (!ExpressionPrefixToTableAlias.ContainsKey(table))
                {
                    ExpressionPrefixToTableAlias.Add(table, alias);
                }
                else
                {
                    killPrefixes.Add(table);
                }

                if (!TableAliases.ContainsKey(alias))
                {
                    TableAliases.Add(alias, table);
                }
                if (!TableAliases.ContainsKey(table))
                {
                    TableAliases.Add(table, table);
                }
            }

            foreach (string s in killPrefixes)
            {
                TableAliases.Remove(s);
                ExpressionPrefixToTableAlias.Remove(s);
            }
        }

        public IFieldContainer FindStartingContainerByFieldName(string fieldName, out string alias)
        {
            if (_query.From.Count == 1)
            {
                alias = ExpressionPrefixToTableAlias[_query.From[0]];
                return Schema.FindContainerByName(_query.From[0]);
            }

            IFieldContainer foundContainer = null;
            alias = null;

            foreach (string containerName in _query.From)
            {
                IFieldContainer container = Schema.FindContainerByName(containerName);

                if (container.ContainsField(fieldName))
                {
                    if (foundContainer != null)
                    {
                        throw new Exception(
                            String.Format(
                                "Cannot determine table from field name '{0}'. Can be either {1}.{0} or {2}.{0}. Use prefixed names.",
                                fieldName,
                                foundContainer.Name, containerName));
                    }
                    alias = ExpressionPrefixToTableAlias[containerName];
                    foundContainer = container;
                }
            }
            if (foundContainer != null)
            {
                return foundContainer;
            }

            throw new Exception("Cannot determine table from field name '" + fieldName + "'. Use prefixed names.");
        }

        public ClassInfo FindClassByCollectionName(string collectionName, out string alias)
        {
            if (_query.From.Count == 1)
            {
                alias = ExpressionPrefixToTableAlias[_query.From[0]];
                return (ClassInfo) Schema.FindContainerByName(_query.From[0]);
            }

            IFieldContainer foundContainer = null;
            alias = null;

            foreach (string containerName in _query.From)
            {
                IFieldContainer container = Schema.FindContainerByName(containerName);

                if (container.ContainsCollection(collectionName) != 0)
                {
                    if (foundContainer != null)
                    {
                        throw new Exception(
                            String.Format(
                                "Cannot determine table from collection name '{0}'. Can be either {1}.{0} or {2}.{0}. Use prefixed names.",
                                collectionName,
                                foundContainer.Name, containerName));
                    }
                    alias = ExpressionPrefixToTableAlias[containerName];
                    foundContainer = container;
                }
            }
            if (foundContainer != null)
            {
                return foundContainer as ClassInfo;
            }

            throw new Exception("Cannot determine table from field name '" + collectionName + "'. Use prefixed names.");
        }

        private void AddJoin(string fromTableAlias, TableInfo rightTable, string leftPrefix, string rightPrefix,
            FieldInfo leftField, FieldInfo rightField, bool innerJoin)
        {
            if (innerJoin && SoodaConfig.GetString("sooda.innerjoins", "false") != "true")
                innerJoin = false;
            string s;
            if (_builder.OuterJoinSyntax == SqlOuterJoinSyntax.Oracle)
            {
                WhereJoins.Add(String.Format("({0}.{2} = {1}.{3}{4})",
                    leftPrefix, rightPrefix, leftField.DBColumnName, rightField.DBColumnName,
                    innerJoin ? "" : " (+)"));
                s = String.Format(", {0} {1}", _builder.QuoteIdentifier(rightTable.DBTableName), rightPrefix);
            }
            else
            {
                s = String.Format("{6} join {0} {2} {5} on ({1}.{3} = {2}.{4})",
                    rightTable.DBTableName, leftPrefix, rightPrefix, leftField.DBColumnName,
                    rightField.DBColumnName,
                    GetTableUsageHint(rightTable), innerJoin ? "inner" : "left outer");
            }
            int foundPos = ActualFromAliases.IndexOf(fromTableAlias);
            if (foundPos == -1)
                throw new NotSupportedException();
            StringCollection coll = FromJoins[foundPos];
            coll.Add(s);
        }

        public string AddPrimaryKeyJoin(string fromTableAlias, ClassInfo classInfo, string rootPrefix,
            FieldInfo fieldToReach)
        {
            // logger.Debug("AddPrimaryKeyJoin({0},{1},{2},{3})", fromTableAlias, classInfo.Name, rootPrefix, fieldToReach);
            if (fieldToReach.Table.DBTableName == classInfo.UnifiedTables[0].DBTableName)
                return rootPrefix;
            string newPrefix = rootPrefix + "_pkjoin_" + fieldToReach.Table.DBTableName;
            newPrefix = _builder.GetTruncatedIdentifier(newPrefix);

            if (TableAliases.ContainsKey(newPrefix))
                return newPrefix;

            TableAliases[newPrefix] = "EXT";

            FieldInfo fi = classInfo.UnifiedTables[0].Fields[0];
            AddJoin(fromTableAlias, fieldToReach.Table, rootPrefix, newPrefix,
                fi, classInfo.UnifiedTables[fieldToReach.Table.OrdinalInClass].Fields[0], !fi.IsNullable);

            return newPrefix;
        }

        public void AddRefJoin(string fromTableAlias, string newPrefix, string lastTableAlias, FieldInfo field,
            bool nullable)
        {
            if (ExpressionPrefixToTableAlias.ContainsKey(newPrefix))
                return;

            string tbl = GetNextTablePrefix();
            ExpressionPrefixToTableAlias.Add(newPrefix, tbl);

            AddJoin(fromTableAlias, field.ReferencedClass.UnifiedTables[0], lastTableAlias, tbl,
                field, field.ReferencedClass.GetFirstPrimaryKeyField(), !nullable);
        }

        public string GetTableAliasForExpressionPrefix(string prefix)
        {
            string s = ExpressionPrefixToTableAlias[prefix];
            if (s == null)
            {
                if (Parent != null)
                    return Parent.GetTableAliasForExpressionPrefix(prefix);
                throw new Exception("Table alias unknown for exception prefix: " + prefix);
            }
            return s;
        }

        private string GetNextTablePrefix()
        {
            return
                Parent != null
                    ? Parent.GetNextTablePrefix()
                    : "t" + CurrentTablePrefix++;
        }

        public void ConvertQuery(SoqlQueryExpression expr)
        {
            expr.Accept(this);
        }

        private void OutputModifiedLiteral(SoqlExpression expr, FieldInfo fieldInfo)
        {
            if (expr is SoqlLiteralExpression)
            {
                SoqlLiteralExpression e = (SoqlLiteralExpression) expr;

                Output.Write("{L:");
                Output.Write(fieldInfo.DataType.ToString());
                Output.Write(':');

                string serializedValue =
                    fieldInfo.GetNullableFieldHandler()
                        .RawSerialize(e.LiteralValue)
                        .Replace("\\", "\\\\")
                        .Replace("}", "\\}");

                Output.Write(serializedValue);
                Output.Write('}');
            }
            else if (expr is SoqlParameterLiteralExpression)
            {
                SoqlParameterLiteralExpression e = (SoqlParameterLiteralExpression) expr;

                Output.Write('{');
                Output.Write(e.ParameterPosition);
                Output.Write(':');
                Output.Write(fieldInfo.DataType.ToString());
                Output.Write('}');
            }
            else if (expr is SoqlBooleanLiteralExpression)
            {
                SoqlBooleanLiteralExpression e = (SoqlBooleanLiteralExpression) expr;

                Output.Write("{L:");
                Output.Write(fieldInfo.DataType.ToString());
                Output.Write(':');

                string serializedValue =
                    fieldInfo.GetNullableFieldHandler().RawSerialize(e.Value).Replace("\\", "\\\\").Replace("}", "\\}");

                Output.Write(serializedValue);
                Output.Write('}');
            }
            else
            {
                throw new ArgumentException("Not supported literal expression type: " + expr.GetType().FullName);
            }
        }

        public override void Visit(SoqlBooleanRelationalExpression v)
        {
            bool upper = UpperLike && v.op == SoqlRelationalOperator.Like;

            //
            // this is to support type coercions. Whenever we have
            //
            // path.expression OPERATOR literal 
            // or
            // path.expression OPERATOR parametrized literal
            // 
            // we may want to change the Modifiers of the literal to reflect the actual type of
            // the property being compared with.
            //
            // Unfortunately MSSQL crawls without this when comparing varchar() columns 
            // against nvarchar() parameter values.
            //

            bool oldBooleanExpansion;
            SoqlExpression unwrappedPar1 = SoqlExpression.Unwrap(v.par1);
            SoqlExpression unwrappedPar2 = SoqlExpression.Unwrap(v.par2);

            bool anyLiteral = unwrappedPar1 is ILiteralModifiers || unwrappedPar2 is ILiteralModifiers;
            if (anyLiteral)
            {
                var anyPath = unwrappedPar1 is SoqlPathExpression || unwrappedPar2 is SoqlPathExpression;

                if (anyPath)
                {
                    FieldInfo pathFieldInfo;

                    if (unwrappedPar1 is SoqlPathExpression)
                    {
                        pathFieldInfo = VisitAndGetFieldInfo((SoqlPathExpression) unwrappedPar1, false);
                    }
                    else
                    {
                        pathFieldInfo = VisitAndGetFieldInfo((SoqlPathExpression) unwrappedPar2, false);
                    }

                    if (pathFieldInfo != null)
                    {
                        oldBooleanExpansion = DisableBooleanExpansion;
                        DisableBooleanExpansion = true;
                        Output.Write('(');

                        switch (v.op)
                        {
                            case SoqlRelationalOperator.LTrimEqual:
                                Output.Write("ltrim(");
                                break;
                            case SoqlRelationalOperator.RTrimEqual:
                                Output.Write("rtrim(");
                                break;
                            case SoqlRelationalOperator.TrimEqual:
                                Output.Write("rtrim(ltrim(");
                                break;
                            case SoqlRelationalOperator.RemoveSharp:
                                Output.Write("replace(");
                                break;
                        }

                        if (upper)
                            Output.Write("upper(");

                        if (unwrappedPar1 is ILiteralModifiers)
                        {
                            OutputModifiedLiteral(unwrappedPar1, pathFieldInfo);
                        }
                        else
                        {
                            v.par1.Accept(this);
                        }
                        if (upper)
                            Output.Write(')');

                        switch (v.op)
                        {
                            case SoqlRelationalOperator.RTrimEqual:
                            case SoqlRelationalOperator.LTrimEqual:
                                Output.Write(')');
                                break;
                            case SoqlRelationalOperator.TrimEqual:
                                Output.Write("))");
                                break;
                            case SoqlRelationalOperator.RemoveSharp:
                                Output.Write(",'#','')");
                                break;
                        }

                        OutputRelationalOperator(v.op);

                        if (upper)
                            Output.Write("upper(");

                        if (unwrappedPar2 is ILiteralModifiers)
                        {
                            OutputModifiedLiteral(unwrappedPar2, pathFieldInfo);
                        }
                        else
                        {
                            v.par2.Accept(this);
                        }

                        if (upper)
                            Output.Write(')');

                        switch (v.op)
                        {
                            case SoqlRelationalOperator.RTrimEqual:
                            case SoqlRelationalOperator.LTrimEqual:
                                Output.Write(')');
                                break;
                            case SoqlRelationalOperator.TrimEqual:
                                Output.Write("))");
                                break;
                            case SoqlRelationalOperator.RemoveSharp:
                                Output.Write(",'#','')");
                                break;
                        }

                        Output.Write(')');
                        DisableBooleanExpansion = oldBooleanExpansion;
                        return;
                    }
                }
            }

            // by default booleans expand to "b <> 0"
            // in relational expressions they expand to "b"
            // this is a dirty hack - will be fixed when we support
            // proper boolean-to-any-type mapping

            oldBooleanExpansion = DisableBooleanExpansion;
            DisableBooleanExpansion = true;
            if (upper)
            {
                Output.Write("(upper(");
                v.par1.Accept(this);
                Output.Write(')');
                OutputRelationalOperator(v.op);
                Output.Write("upper(");
                v.par2.Accept(this);
                Output.Write("))");
            }
            else
                base.Visit(v);
            DisableBooleanExpansion = oldBooleanExpansion;
        }

        protected override void Write(SoqlBinaryOperator op)
        {
            if (op == SoqlBinaryOperator.Concat)
                Output.Write(_builder.StringConcatenationOperator);
            else
                base.Write(op);
        }
    }
}