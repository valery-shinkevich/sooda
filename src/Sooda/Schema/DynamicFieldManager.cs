//
// Copyright (c) 2014 Piotr Fusik <piotr@fusik.info>
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

#if DOTNET35

namespace Sooda.Schema
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using Logging;
    using Sql;

    public static class DynamicFieldManager
    {
        private static readonly Logger logger = LogManager.GetLogger("Sooda.Schema.DynamicFieldsManager");

        private static bool DynamicFieldsEnabled(SchemaInfo schema)
        {
            return schema.DataSources.Any(ds => ds.EnableDynamicFields);
        }

        private static TableInfo Prepare(FieldInfo fi)
        {
            ClassInfo ci = fi.ParentClass;
            if (ci.ContainsField(fi.Name))
                throw new SoodaSchemaException("Field " + fi.Name + " already exists in " + ci.Name);

            TableInfo table = new TableInfo();
            table.TableUsageType = TableUsageType.DynamicField;
            table.DBTableName = ci.Name + "_" + fi.Name;

            // copy primary key fields
            FieldInfo[] pks = ci.GetPrimaryKeyFields();
            for (int i = 0; i < pks.Length; i++)
            {
                FieldInfo pk = pks[i];
                table.Fields.Add(new FieldInfo
                {
                    Name = pk.Name,
                    DataType = pk.DataType,
                    Size = pk.Size,
                    Precision = pk.Precision,
                    References = pk.References,
                    IsPrimaryKey = true,
                    DBColumnName = i == 0 ? "id" : "id" + (i + 1)
                });
            }

            fi.DBColumnName = "value";
            table.Fields.Add(fi);

            return table;
        }

        private static void Resolve(SchemaInfo schema, HashSet<ClassInfo> affectedClasses)
        {
            // add subclasses
            foreach (ClassInfo ci in affectedClasses.ToArray())
            {
                foreach (ClassInfo sci in schema.GetSubclasses(ci))
                    affectedClasses.Add(sci);
            }

            schema.Resolve(affectedClasses);
        }

        private static void Resolve(ClassInfo ci)
        {
            Resolve(ci.Schema, new HashSet<ClassInfo> {ci});
        }

        private static void Load(SoodaTransaction transaction)
        {
            SchemaInfo schema = transaction.Schema;
            HashSet<ClassInfo> affectedClasses = new HashSet<ClassInfo>();
            foreach (DataSourceInfo dsi in schema.DataSources)
            {
                if (!dsi.EnableDynamicFields)
                    continue;
                SoodaDataSource ds = transaction.OpenDataSource(dsi);
                using (
                    IDataReader r =
                        ds.ExecuteRawQuery(
                            "select class, field, type, nullable, fieldsize, precision from SoodaDynamicField"))
                {
                    while (r.Read())
                    {
                        string className = r.GetString(0);
                        ClassInfo ci = schema.FindClassByName(className);
                        if (ci == null)
                        {
                            logger.Warn(
                                "Ignoring a dynamic field of non-existent class {0} -- see the SoodaDynamicField table",
                                className);
                            continue;
                        }

                        FieldInfo fi = new FieldInfo();
                        fi.ParentClass = ci;
                        fi.Name = r.GetString(1);
                        fi.TypeName = r.GetString(2);
                        fi.IsNullable = r.GetInt32(3) != 0;
                        if (!r.IsDBNull(4))
                            fi.Size = r.GetInt32(4);
                        if (!r.IsDBNull(5))
                            fi.Precision = r.GetInt32(5);

                        ci.LocalTables.Add(Prepare(fi));
                        affectedClasses.Add(ci);
                    }
                }
            }

            Resolve(schema, affectedClasses);
        }

        internal static void OpenTransaction(SoodaTransaction transaction)
        {
            SchemaInfo schema = transaction.Schema;

            if (schema.RwLock == null)
            {
                if (!DynamicFieldsEnabled(schema))
                    return;

                lock (schema)
                {
                    if (schema.RwLock == null)
                    {
                        Load(transaction);
                        schema.RwLock = new ReaderWriterLock();
                    }
                }
            }

            schema.RwLock.AcquireReaderLock(-1);
        }

        internal static void CloseTransaction(SoodaTransaction transaction)
        {
            ReaderWriterLock rwLock = transaction.Schema.RwLock;
            if (rwLock != null)
                rwLock.ReleaseReaderLock();
        }

        private static LockCookie LockWrite(SoodaTransaction transaction)
        {
            ReaderWriterLock rwLock = transaction.Schema.RwLock;
            if (rwLock == null)
                throw new InvalidOperationException("Dynamic fields not enabled in Sooda schema");
            LockCookie lockCookie = rwLock.UpgradeToWriterLock(-1);
            transaction.Cache.Clear();
            return lockCookie;
        }

        private static int? NegativeToNull(int i)
        {
            if (i < 0)
                return null;
            return i;
        }

        public static void Add(FieldInfo fi, SoodaTransaction transaction)
        {
            ClassInfo ci = fi.ParentClass;
            fi.ResolveReferences(ci.Schema);
            SqlDataSource ds = (SqlDataSource) transaction.OpenDataSource(ci.GetDataSource());

            LockCookie lockCookie = LockWrite(transaction);
            try
            {
                TableInfo table = Prepare(fi);
                fi.Table = table;

                StringWriter sw = new StringWriter();
                sw.Write(
                    "insert into SoodaDynamicField (class, field, type, nullable, fieldsize, precision) values ({0}, {1}, {2}, {3}, {4}, {5})");
                ds.ExecuteNonQuery(sw.ToString(), ci.Name, fi.Name, fi.TypeName, fi.IsNullable ? 1 : 0,
                    NegativeToNull(fi.Size), NegativeToNull(fi.Precision));

                sw = new StringWriter();
                ds.SqlBuilder.GenerateCreateTable(sw, table, null, "");
                ds.ExecuteNonQuery(sw.ToString());

                sw = new StringWriter();
                ds.SqlBuilder.GeneratePrimaryKey(sw, table, null, "");
                ds.ExecuteNonQuery(sw.ToString());

                sw = new StringWriter();
                ds.SqlBuilder.GenerateForeignKeys(sw, table, "");
                string sql = sw.ToString();
                if (sql.Length > 0)
                    ds.ExecuteNonQuery(sql);

                ci.LocalTables.Add(table);
                Resolve(ci);
            }
            finally
            {
                transaction.Schema.RwLock.DowngradeFromWriterLock(ref lockCookie);
            }
        }

        public static void CreateIndex(FieldInfo fi, SoodaTransaction transaction)
        {
            SqlDataSource ds = (SqlDataSource) transaction.OpenDataSource(fi.ParentClass.GetDataSource());

            StringWriter sw = new StringWriter();
            ds.SqlBuilder.GenerateIndex(sw, fi, null, "");
            ds.ExecuteNonQuery(sw.ToString());
        }

        public static void Update(FieldInfo fi, SoodaTransaction transaction)
        {
            if (!fi.IsDynamic)
                throw new InvalidOperationException(fi.Name + " is not a dynamic field");
            ClassInfo ci = fi.ParentClass;
            SoodaDataSource ds = transaction.OpenDataSource(ci.GetDataSource());

            ds.ExecuteNonQuery("update SoodaDynamicField set nullable={0} where class={1} and field={2}",
                fi.IsNullable ? 1 : 0, ci.Name, fi.Name);
        }

        public static void Remove(FieldInfo fi, SoodaTransaction transaction)
        {
            if (!fi.IsDynamic)
                throw new InvalidOperationException(fi.Name + " is not a dynamic field");
            ClassInfo ci = fi.ParentClass;
            SoodaDataSource ds = transaction.OpenDataSource(ci.GetDataSource());

            LockCookie lockCookie = LockWrite(transaction);
            try
            {
                ds.ExecuteNonQuery("delete from SoodaDynamicField where class={0} and field={1}", ci.Name, fi.Name);
                ds.ExecuteNonQuery("drop table " + fi.Table.DBTableName);

                ci.LocalTables.Remove(fi.Table);
                Resolve(ci);
            }
            finally
            {
                transaction.Schema.RwLock.DowngradeFromWriterLock(ref lockCookie);
            }
        }
    }
}

#endif