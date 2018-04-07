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

namespace Sooda.ObjectMapper
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using Logging;
    using Schema;

    public class SoodaObjectManyToManyCollection : SoodaObjectCollectionBase
    {
        private static readonly Logger Logger = LogManager.GetLogger("Sooda.ManyToManyCollection");
        protected readonly int MasterColumn;
        protected readonly object MasterValue;
        protected readonly Type RelationType;
        protected readonly RelationInfo RelationInfo;
        private SoodaRelationTable _relationTable;

        private readonly ISoodaObjectFactory _factory;

        public SoodaObjectManyToManyCollection(SoodaTransaction transaction, int masterColumn, object masterValue,
            Type relationType, RelationInfo relationInfo)
            : base(transaction, masterColumn == 0 ? relationInfo.GetRef1ClassInfo() : relationInfo.GetRef2ClassInfo())
        {
            RelationInfo = relationInfo;
            MasterValue = masterValue;
            MasterColumn = masterColumn;
            RelationType = relationType;

            _factory = transaction.GetFactory(classInfo);
        }

        public override int Add(object obj)
        {
            var so = (SoodaObject) obj;
            object pk = so.GetPrimaryKeyValue();
            SoodaRelationTable rel = GetSoodaRelationTable();
            if (MasterColumn == 0)
                rel.Add(pk, MasterValue);
            else
                rel.Add(MasterValue, pk);
            return InternalAdd(so);
        }

        public override void Remove(object obj)
        {
            var so = (SoodaObject) obj;
            object pk = so.GetPrimaryKeyValue();
            SoodaRelationTable rel = GetSoodaRelationTable();
            if (MasterColumn == 0)
                rel.Remove(pk, MasterValue);
            else
                rel.Remove(MasterValue, pk);
            InternalRemove(so);
        }

        public override bool Contains(object obj)
        {
            return InternalContains((SoodaObject) obj);
        }

        protected int InternalAdd(SoodaObject obj)
        {
            if (itemsArray == null)
                return -1;
            if (!items.ContainsKey(obj))
            {
                items.Add(obj, itemsArray.Count);
                itemsArray.Add(obj);
            }
            return -1;
        }

        protected void InternalRemove(SoodaObject obj)
        {
            if (itemsArray == null)
                return;

            int pos;
            if (!items.TryGetValue(obj, out pos))
                return;

            SoodaObject lastObj = itemsArray[itemsArray.Count - 1];
            if (lastObj != obj)
            {
                itemsArray[pos] = lastObj;
                items[lastObj] = pos;
            }
            itemsArray.RemoveAt(itemsArray.Count - 1);
            items.Remove(obj);
        }

        public bool InternalContains(SoodaObject obj)
        {
            if (itemsArray == null)
                LoadData();
            return items.ContainsKey(obj);
        }

        protected void LoadDataFromReader()
        {
            SoodaDataSource ds = transaction.OpenDataSource(RelationInfo.GetDataSource());
            TableInfo[] loadedTables;
            using (
                IDataReader reader = ds.LoadRefObjectList(transaction.Schema, RelationInfo, MasterColumn, MasterValue,
                    out loadedTables))
            {
                while (reader.Read())
                {
                    SoodaObject obj = SoodaObject.GetRefFromRecordHelper(transaction, _factory, reader, 0, loadedTables,
                        0);
                    InternalAdd(obj);
                }
            }
        }

        protected SoodaRelationTable GetSoodaRelationTable()
        {
            return _relationTable ?? (_relationTable = transaction.GetRelationTable(RelationType));
        }

        private void OnTupleChanged(object sender, SoodaRelationTupleChangedArgs args)
        {
            if (!MasterValue.Equals(MasterColumn == 0 ? args.Right : args.Left))
                return;

            var obj = _factory.GetRef(transaction, MasterColumn == 0 ? args.Left : args.Right);

            switch (args.Mode)
            {
                case 1:
                    InternalAdd(obj);
                    break;
                case -1:
                    InternalRemove(obj);
                    break;
            }
        }

        protected override void LoadData()
        {
            bool useCache = transaction.CachingPolicy.ShouldCacheRelation(RelationInfo, classInfo);
            string cacheKey = null;
            items = new Dictionary<SoodaObject, int>();
            itemsArray = new List<SoodaObject>();
            if (useCache)
            {
                if (!transaction.HasBeenPrecommitted(RelationInfo) && !transaction.HasBeenPrecommitted(classInfo))
                {
                    cacheKey = RelationInfo.Name + " where " + RelationInfo.Table.Fields[1 - MasterColumn].Name + " = " +
                               MasterValue;
                }
                else
                {
                    Logger.Debug(
                        "Cache miss. Cannot use cache for {0} where {1} = {2} because objects have been precommitted.",
                        RelationInfo.Name, RelationInfo.Table.Fields[1 - MasterColumn].Name, MasterValue);
                    SoodaStatistics.Global.RegisterCollectionCacheMiss();
                    transaction.Statistics.RegisterCollectionCacheMiss();
                }
            }
            IEnumerable keysCollection = transaction.LoadCollectionFromCache(cacheKey, Logger);
            if (keysCollection != null)
            {
                foreach (object o in keysCollection)
                {
                    SoodaObject obj = _factory.GetRef(transaction, o);
                    // this binds to cache
                    obj.EnsureFieldsInited();
                    InternalAdd(obj);
                }
            }
            else
            {
                LoadDataFromReader();
                if (cacheKey != null)
                {
                    TimeSpan expirationTimeout;
                    bool slidingExpiration;
                    if (transaction.CachingPolicy.GetExpirationTimeout(
                        RelationInfo, classInfo, itemsArray.Count, out expirationTimeout, out slidingExpiration))
                    {
                        transaction.StoreCollectionInCache(cacheKey, classInfo, itemsArray, new[] {RelationInfo.Name},
                            true, expirationTimeout, slidingExpiration);
                    }
                }
            }
            SoodaRelationTable rel = GetSoodaRelationTable();
            rel.OnTupleChanged += OnTupleChanged;
            if (rel.TupleCount != 0)
            {
                SoodaRelationTable.Tuple[] tuples = rel.Tuples;
                int count = rel.TupleCount;
                if (MasterColumn == 1)
                {
                    for (int i = 0; i < count; ++i)
                    {
                        if (tuples[i].ref1.Equals(MasterValue))
                        {
                            SoodaObject obj = _factory.GetRef(transaction, tuples[i].ref2);
                            if (tuples[i].tupleMode > 0)
                            {
                                InternalAdd(obj);
                            }
                            else
                            {
                                InternalRemove(obj);
                            }
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < count; ++i)
                    {
                        if (tuples[i].ref2.Equals(MasterValue))
                        {
                            SoodaObject obj = _factory.GetRef(transaction, tuples[i].ref1);
                            if (tuples[i].tupleMode > 0)
                            {
                                InternalAdd(obj);
                            }
                            else
                            {
                                InternalRemove(obj);
                            }
                        }
                    }
                }
            }
        }
    }
}