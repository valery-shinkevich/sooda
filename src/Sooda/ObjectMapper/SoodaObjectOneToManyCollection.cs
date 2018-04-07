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
    using System.Reflection;
    using Caching;
    using Logging;
    using QL;
    using Schema;

    internal enum CollectionChange
    {
        Added,
        Removed
    }

    public class SoodaObjectOneToManyCollection : SoodaObjectCollectionBase, ISoodaObjectListInternal
    {
        private static readonly Logger Logger = LogManager.GetLogger("Sooda.OneToManyCollection");

        private Dictionary<SoodaObject, CollectionChange> _tempItems;
        private readonly SoodaObject _parentObject;
        private readonly string _childRefField;
        private readonly Type _childType;
        private readonly SoodaWhereClause _additionalWhereClause;
        private readonly bool _cached;


        public SoodaObjectOneToManyCollection(SoodaTransaction tran, Type childType, SoodaObject parentObject,
            string childRefField, ClassInfo classInfo, SoodaWhereClause additionalWhereClause, bool cached)
            : base(tran, classInfo)
        {
            _childType = childType;
            _parentObject = parentObject;
            _childRefField = childRefField;
            _additionalWhereClause = additionalWhereClause;
            _cached = cached;
        }

        public override int Add(object obj)
        {
            if (obj == null)
                throw new ArgumentNullException("obj");

            Type t = _childType;
            PropertyInfo prop = t.GetProperty(_childRefField);
            prop.SetValue(obj, _parentObject, null);
            return 0;
        }

        public override void Remove(object obj)
        {
            if (obj == null)
                throw new ArgumentNullException("obj");

            Type t = _childType;
            PropertyInfo prop = t.GetProperty(_childRefField);
            prop.SetValue(obj, null, null);
        }

        public override bool Contains(object obj)
        {
            if (obj == null)
                return false;

            Type t = _childType;
            PropertyInfo prop = t.GetProperty(_childRefField);
            return _parentObject == (SoodaObject) prop.GetValue(obj, null);
        }

        public void InternalAdd(SoodaObject c)
        {
            if (!_childType.IsInstanceOfType(c))
                return;

            if (items == null)
            {
                if (_tempItems == null)
                    _tempItems = new Dictionary<SoodaObject, CollectionChange>();
                _tempItems[c] = CollectionChange.Added;
                return;
            }

            if (items.ContainsKey(c))
                return;

            items.Add(c, itemsArray.Count);
            itemsArray.Add(c);
        }

        public void InternalRemove(SoodaObject c)
        {
            if (!_childType.IsInstanceOfType(c))
                return;

            if (items == null)
            {
                if (_tempItems == null)
                    _tempItems = new Dictionary<SoodaObject, CollectionChange>();
                _tempItems[c] = CollectionChange.Removed;
                return;
            }

            int pos;
            if (!items.TryGetValue(c, out pos))
                throw new InvalidOperationException("Attempt to remove object not in collection");


            SoodaObject lastObj = itemsArray[itemsArray.Count - 1];
            if (lastObj != c)
            {
                itemsArray[pos] = lastObj;
                items[lastObj] = pos;
            }
            itemsArray.RemoveAt(itemsArray.Count - 1);
            items.Remove(c);
        }

        protected override void LoadData()
        {
            SoodaDataSource ds = transaction.OpenDataSource(classInfo.GetDataSource());
            bool dsIsOpened = ds.IsOpen; //+wash
            if (!dsIsOpened) ds.Open(); //+wash

            items = new Dictionary<SoodaObject, int>();
            itemsArray = new List<SoodaObject>();

            ISoodaObjectFactory factory = transaction.GetFactory(classInfo);
            var whereClause = new SoodaWhereClause(Soql.FieldEqualsParam(_childRefField, 0),
                _parentObject.GetPrimaryKeyValue());

            if (_additionalWhereClause != null)
                whereClause = whereClause.Append(_additionalWhereClause);

            string cacheKey = null;

            if (_cached)
            {
                // cache makes sense only on clean database
                if (!transaction.HasBeenPrecommitted(classInfo.GetRootClass()))
                {
                    cacheKey = SoodaCache.GetCollectionKey(classInfo, whereClause);
                }
            }
            IEnumerable keysCollection = transaction.LoadCollectionFromCache(cacheKey, Logger);
            if (keysCollection != null)
            {
                foreach (object o in keysCollection)
                {
                    SoodaObject obj = factory.GetRef(transaction, o);
                    // this binds to cache
                    obj.EnsureFieldsInited();

                    if (_tempItems != null)
                    {
                        CollectionChange change;
                        if (_tempItems.TryGetValue(obj, out change) && change == CollectionChange.Removed)
                            continue;
                    }

                    items.Add(obj, itemsArray.Count);
                    itemsArray.Add(obj);
                }
            }
            else
            {
                TableInfo[] loadedTables;
                using (
                    IDataReader reader = ds.LoadObjectList(transaction.Schema, classInfo, whereClause, null, 0, -1,
                        SoodaSnapshotOptions.Default, out loadedTables))
                {
                    List<SoodaObject> readObjects = null;

                    if (_cached)
                        readObjects = new List<SoodaObject>();

                    while (reader.Read())
                    {
                        SoodaObject obj = SoodaObject.GetRefFromRecordHelper(transaction, factory, reader, 0,
                            loadedTables, 0);
                        if (readObjects != null)
                            readObjects.Add(obj);
                        if (_tempItems != null)
                        {
                            CollectionChange change;
                            if (_tempItems.TryGetValue(obj, out change) && change == CollectionChange.Removed)
                                continue;
                        }
                        items.Add(obj, itemsArray.Count);
                        itemsArray.Add(obj);
                    }
                    if (_cached)
                    {
                        TimeSpan expirationTimeout;
                        bool slidingExpiration;

                        if (readObjects != null && transaction.CachingPolicy.GetExpirationTimeout(
                            classInfo, whereClause, null, 0, -1, readObjects.Count,
                            out expirationTimeout, out slidingExpiration))
                        {
                            transaction.StoreCollectionInCache(cacheKey, classInfo, readObjects, null, true,
                                expirationTimeout, slidingExpiration);
                        }
                    }
                }
            }

            if (_tempItems != null)
            {
                foreach (KeyValuePair<SoodaObject, CollectionChange> entry in _tempItems)
                {
                    if (entry.Value == CollectionChange.Added)
                    {
                        var obj = entry.Key;
                        if (!items.ContainsKey(obj))
                        {
                            items.Add(obj, itemsArray.Count);
                            itemsArray.Add(obj);
                        }
                    }
                }
            }
            if (!dsIsOpened) ds.Close(); //+wash
        }
    }
}