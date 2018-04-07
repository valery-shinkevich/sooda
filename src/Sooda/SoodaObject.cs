namespace Sooda
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Data;
    using System.Data.SqlTypes;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Xml;
    using Caching;
    using Logging;
    using ObjectMapper;
    using QL;
    using Schema;
    using FieldInfo = Schema.FieldInfo;

    public class SoodaObject :
#if DOTNET4
        System.Dynamic.DynamicObject,
#endif
        IEquatable<SoodaObject>
    {
        private static readonly Logger Logger = LogManager.GetLogger("Sooda.Object");

        // instance fields - initialized in InitRawObject()

        private byte[] _fieldIsDirty;
        internal SoodaObjectFieldValues FieldValues;
        private int _dataLoadedMask;
        private SoodaObjectFlags _flags;
        private object _primaryKeyValue;
        private SoodaTransaction _transaction;

#pragma warning disable 168
        // ReSharper disable once UnusedParameter.Local
        protected SoodaObject(SoodaConstructor c)
#pragma warning restore 168
        {
            GC.SuppressFinalize(this);
        }

        protected SoodaObject(SoodaTransaction tran)
        {
            GC.SuppressFinalize(this);
            tran.Statistics.RegisterObjectInsert();
            SoodaStatistics.Global.RegisterObjectInsert();

            InitRawObject(tran);
            InsertMode = true;
            SetAllDataLoaded();
            // ReSharper disable DoNotCallOverridableMethodsInConstructor
            if (GetClassInfo().SubclassSelectorValue != null)
            {
                DisableFieldUpdateTriggers();
                FieldInfo selectorField = GetClassInfo().SubclassSelectorField;
                SetPlainFieldValue(0, selectorField.Name, selectorField.ClassUnifiedOrdinal,
                    GetClassInfo().SubclassSelectorValue, null, null);
                EnableFieldUpdateTriggers();
            }
            // ReSharper restore DoNotCallOverridableMethodsInConstructor
        }

        private bool InsertMode
        {
            get { return (_flags & SoodaObjectFlags.InsertMode) != 0; }
            set
            {
                if (value)
                {
                    _flags |= SoodaObjectFlags.InsertMode;
                    SetObjectDirty();
                }
                else
                    _flags &= ~SoodaObjectFlags.InsertMode;
            }
        }

        internal bool VisitedOnCommit
        {
            get { return (_flags & SoodaObjectFlags.VisitedOnCommit) != 0; }
            set
            {
                if (value)
                    _flags |= SoodaObjectFlags.VisitedOnCommit;
                else
                    _flags &= ~SoodaObjectFlags.VisitedOnCommit;
            }
        }

        private bool WrittenIntoDatabase
        {
            get { return (_flags & SoodaObjectFlags.WrittenIntoDatabase) != 0; }
            set
            {
                if (value)
                    _flags |= SoodaObjectFlags.WrittenIntoDatabase;
                else
                    _flags &= ~SoodaObjectFlags.WrittenIntoDatabase;
            }
        }

        private bool PostCommitForced
        {
            get { return (_flags & SoodaObjectFlags.ForcePostCommit) != 0; }
/*
            set
            {
                if (value)
                    _flags |= SoodaObjectFlags.ForcePostCommit;
                else
                    _flags &= ~SoodaObjectFlags.ForcePostCommit;
            }
*/
        }

        internal bool InsertedIntoDatabase
        {
            get { return (_flags & SoodaObjectFlags.InsertedIntoDatabase) != 0; }
            set
            {
                if (value)
                    _flags |= SoodaObjectFlags.InsertedIntoDatabase;
                else
                    _flags &= ~SoodaObjectFlags.InsertedIntoDatabase;
            }
        }

        private bool FromCache
        {
            get { return (_flags & SoodaObjectFlags.FromCache) != 0; }
            set
            {
                if (value)
                    _flags |= SoodaObjectFlags.FromCache;
                else
                    _flags &= ~SoodaObjectFlags.FromCache;
            }
        }

        internal bool DeleteMarker
        {
            get { return (_flags & SoodaObjectFlags.MarkedForDeletion) != 0; }
            set
            {
                if (value)
                    _flags = _flags | SoodaObjectFlags.MarkedForDeletion;
                else
                    _flags = (_flags & ~SoodaObjectFlags.MarkedForDeletion);
            }
        }

        #region IEquatable<SoodaObject> Members

        public bool Equals(SoodaObject soodaObject)
        {
            if (soodaObject == null) return false;
            return GetObjectKeyString() == soodaObject.GetObjectKeyString();
        }

        #endregion

        public bool AreFieldUpdateTriggersEnabled()
        {
            return (_flags & SoodaObjectFlags.DisableFieldTriggers) == 0;
        }

        public bool EnableFieldUpdateTriggers()
        {
            return EnableFieldUpdateTriggers(true);
        }

        public bool DisableFieldUpdateTriggers()
        {
            return EnableFieldUpdateTriggers(false);
        }

        public bool EnableFieldUpdateTriggers(bool enable)
        {
            bool oldValue = AreFieldUpdateTriggersEnabled();

            if (!enable)
                _flags |= SoodaObjectFlags.DisableFieldTriggers;
            else
                _flags &= ~SoodaObjectFlags.DisableFieldTriggers;
            return oldValue;
        }

        public bool AreObjectTriggersEnabled()
        {
            return (_flags & SoodaObjectFlags.DisableObjectTriggers) == 0;
        }

        public bool EnableObjectTriggers()
        {
            return EnableObjectTriggers(true);
        }

        public bool DisableObjectTriggers()
        {
            return EnableObjectTriggers(false);
        }

        public bool EnableObjectTriggers(bool enable)
        {
            bool oldValue = AreObjectTriggersEnabled();

            if (!enable)
                _flags |= SoodaObjectFlags.DisableObjectTriggers;
            else
                _flags &= ~SoodaObjectFlags.DisableObjectTriggers;
            return oldValue;
        }


        public void ForcePostCommit()
        {
            _flags |= SoodaObjectFlags.ForcePostCommit;
        }

        private SoodaCacheEntry GetCacheEntry()
        {
            return new SoodaCacheEntry(_dataLoadedMask, FieldValues);
        }

        internal void SetInsertMode()
        {
            InsertMode = true;
            SetAllDataLoaded();
        }

        public bool IsInsertMode()
        {
            return InsertMode;
        }

        //~SoodaObject()
        //{
        //    // logger.Trace("Finalizer for {0}", GetObjectKeyString());
        //}

        private void PropagatePrimaryKeyToFields()
        {
            FieldInfo[] primaryKeys = GetClassInfo().GetPrimaryKeyFields();
            var tuple = _primaryKeyValue as SoodaTuple;

            if (tuple != null)
            {
                if (tuple.Length != primaryKeys.Length)
                    throw new InvalidOperationException("Primary key tuple length doesn't match the expected length");

                for (int i = 0; i < primaryKeys.Length; ++i)
                {
                    FieldValues.SetFieldValue(primaryKeys[i].ClassUnifiedOrdinal, tuple.GetValue(i));
                }
            }
            else
            {
                if (primaryKeys.Length != 1)
                    throw new InvalidOperationException("Primary key is not a scalar.");

                // scalar
                FieldValues.SetFieldValue(primaryKeys[0].ClassUnifiedOrdinal, _primaryKeyValue);
            }
        }

        protected virtual SoodaObjectFieldValues InitFieldValues(int fieldCount, string[] fieldNames)
        {
            return new SoodaObjectArrayFieldValues(fieldCount);
        }

        private void InitFieldData(bool justLoading)
        {
            if (!InsertMode && GetTransaction().CachingPolicy.ShouldCacheObject(this)) //if (!InsertMode)
            {
                SoodaCacheEntry cachedData = GetTransaction().Cache.Find(GetClassInfo().GetRootClass().Name,
                    _primaryKeyValue);
                if (cachedData != null)
                {
                    GetTransaction().Statistics.RegisterCacheHit();
                    SoodaStatistics.Global.RegisterCacheHit();

                    if (Logger.IsTraceEnabled)
                    {
                        Logger.Trace("Initializing object {0}({1}) from cache.", GetType().Name, _primaryKeyValue);
                    }
                    FieldValues = cachedData.Data;
                    _dataLoadedMask = cachedData.DataLoadedMask;
                    FromCache = true;
                    return;
                }

                // we don't register a cache miss when we're just loading
                if (!justLoading)
                {
                    GetTransaction().Statistics.RegisterCacheMiss();
                    SoodaStatistics.Global.RegisterCacheMiss();
                    if (Logger.IsTraceEnabled)
                    {
                        Logger.Trace("Cache miss. Object {0}({1}) not found in cache.", GetType().Name,
                            _primaryKeyValue);
                    }
                }
            }

            ClassInfo ci = GetClassInfo();

            int fieldCount = ci.UnifiedFields.Count;
            FieldValues = InitFieldValues(fieldCount, ci.OrderedFieldNames);
            GetTransaction().Statistics.RegisterFieldsInited();
            SoodaStatistics.Global.RegisterFieldsInited();

            // primary key was set before the fields - propagate the value
            // back to the field(s)
            if (_primaryKeyValue != null)
            {
                PropagatePrimaryKeyToFields();
            }

            if (InsertMode)
            {
                SetDefaultNotNullValues();
            }
        }

        private void SetDefaultNotNullValues()
        {
            ClassInfo ci = GetClassInfo();

            for (int i = 0; i < FieldValues.Length; ++i)
            {
                if (ci.UnifiedFields[i].IsPrimaryKey || ci.UnifiedFields[i].ReferencedClass != null)

                    continue;

                SoodaFieldHandler handler = GetFieldHandler(i);
                if (!handler.IsNullable)
                    FieldValues.SetFieldValue(i, handler.ZeroValue());
            }
        }

        private void SetUpdateMode(object primaryKeyValue)
        {
            InsertMode = false;
            SetPrimaryKeyValue(primaryKeyValue);
            SetAllDataNotLoaded();
        }

        public SoodaTransaction GetTransaction()
        {
            return _transaction;
        }

        protected virtual void BeforeObjectInsert()
        {
        }

        protected virtual void BeforeObjectUpdate()
        {
        }

        protected virtual void BeforeObjectDelete()
        {
        }

        protected virtual void AfterObjectInsert()
        {
        }

        protected virtual void AfterObjectUpdate()
        {
        }

        protected virtual void AfterObjectDelete()
        {
        }

        protected virtual void BeforeFieldUpdate(string name, object oldVal, object newVal)
        {
        }

        protected virtual void AfterFieldUpdate(string name, object oldVal, object newVal)
        {
        }

        public void MarkForDelete()
        {
            MarkForDelete(true, true);
        }

        public void MarkForDelete(bool delete, bool recurse)
        {
            try
            {
                int oldDeletePosition = GetTransaction().DeletedObjects.Count;
                MarkForDelete(delete, recurse, true);
                int newDeletePosition = GetTransaction().DeletedObjects.Count;
                if (newDeletePosition != oldDeletePosition)
                {
                    GetTransaction().SaveObjectChanges(true, null);
                    GetTransaction().SavingObjects = true;

                    foreach (SoodaDataSource source in GetTransaction().DataSources)
                    {
                        source.BeginSaveChanges();
                    }

                    var deleted = GetTransaction().DeletedObjects;

                    for (int i = oldDeletePosition; i < newDeletePosition; ++i)
                    {
                        // logger.Debug("Actually deleting {0}", GetTransaction().DeletedObjects[i].GetObjectKeyString());
                        // Console.WriteLine("Actually deleting {0}", GetTransaction().DeletedObjects[i].GetObjectKeyString());
                        //deleted[i].CommitObjectChanges();
                        //deleted[i].SetObjectDirty(); 
                        SoodaObject o = deleted[i];
                        o.CommitObjectChanges();
                        o.SetObjectDirty();
                        GetTransaction().MarkPrecommitted(o);
                    }
                    foreach (SoodaDataSource source in GetTransaction().DataSources)
                    {
                        source.FinishSaveChanges();
                    }
                    for (int i = oldDeletePosition; i < newDeletePosition; ++i)
                    {
                        deleted[i].AfterObjectDelete();
                    }
                }
            }
            finally
            {
                GetTransaction().SavingObjects = false;
            }
        }

        public void MarkForDelete(bool delete, bool recurse, bool savingChanges)
        {
            if (DeleteMarker != delete)
            {
                BeforeObjectDelete();
                DeleteMarker = delete;

                if (recurse)
                {
                    if (Logger.IsTraceEnabled)
                    {
                        Logger.Trace("Marking outer references of {0} for delete...", GetObjectKeyString());
                    }
                    for (ClassInfo ci = GetClassInfo(); ci != null; ci = ci.InheritsFromClass)
                    {
                        foreach (FieldInfo fi in ci.OuterReferences)
                        {
                            Logger.Trace("{0} Delete action: {1}", fi, fi.DeleteAction);
                            if (fi.DeleteAction == DeleteAction.Nothing)
                                continue;

                            ISoodaObjectFactory factory = GetTransaction().GetFactory(fi.ParentClass);

                            SoqlBooleanExpression whereExpression = Soql.FieldEquals(fi.Name, this);
                            var whereClause = new SoodaWhereClause(whereExpression);
                            // logger.Debug("loading list where: {0}", whereExpression);
                            IList referencingList = factory.GetList(GetTransaction(), whereClause, SoodaOrderBy.Unsorted,
                                SoodaSnapshotOptions.KeysOnly);
                            switch (fi.DeleteAction)
                            {
                                case DeleteAction.Cascade:
                                    foreach (SoodaObject o in referencingList)
                                    {
                                        o.MarkForDelete(delete, true, savingChanges);
                                    }
                                    break;
                                case DeleteAction.Nullify:
                                    foreach (SoodaObject o in referencingList)
                                    {
                                        if (o.FieldValues != null)
                                            o.FieldValues.SetFieldValue(fi.Name, null);
                                    }
                                    break;
                            }
                        }
                    }
                }
                GetTransaction().DeletedObjects.Add(this);
            }
        }

        public bool IsMarkedForDelete()
        {
            return DeleteMarker;
        }

        internal object GetFieldValue(int fieldNumber)
        {
            return FieldValues.GetBoxedFieldValue(fieldNumber);
        }

        public bool IsFieldDirty(int fieldNumber)
        {
            if (_fieldIsDirty == null)
                return false;

            int slotNumber = fieldNumber >> 3;
            int bitNumber = fieldNumber & 7;

            return (_fieldIsDirty[slotNumber] & (1 << bitNumber)) != 0;
        }

        public void SetFieldDirty(int fieldNumber, bool dirty)
        {
            if (_fieldIsDirty == null)
            {
                int fieldCount = GetClassInfo().UnifiedFields.Count;
                _fieldIsDirty = new byte[(fieldCount + 7) >> 3];
            }

            int slotNumber = fieldNumber >> 3;
            int bitNumber = fieldNumber & 7;

            if (dirty)
            {
                _fieldIsDirty[slotNumber] |= (byte) (1 << bitNumber);
            }
            else
            {
                _fieldIsDirty[slotNumber] &= (byte) ~(1 << bitNumber);
            }
        }

        protected virtual SoodaFieldHandler GetFieldHandler(int ordinal)
        {
            throw new NotImplementedException();
        }

        internal void CheckForNulls()
        {
            EnsureFieldsInited();
            if (IsInsertMode())
            {
                var ci = GetClassInfo();
                for (int i = 0; i < FieldValues.Length; ++i)
                {
                    if (!ci.UnifiedFields[i].IsNullable && FieldValues.IsNull(i))
                        FieldCannotBeNull(ci.UnifiedFields[i].Name);
                }
            }
        }

        protected internal virtual void CheckAssertions()
        {
        }

        private void FieldCannotBeNull(string fieldName)
        {
            throw new SoodaException("Field '" + fieldName + "' cannot be null on commit in " + GetObjectKeyString());
        }

        public bool IsObjectDirty()
        {
            return (_flags & SoodaObjectFlags.Dirty) != 0;
        }

        public void SetObjectDirty()
        {
            if (!IsObjectDirty())
            {
                EnsureFieldsInited();
                _flags |= SoodaObjectFlags.Dirty;
                GetTransaction().RegisterDirtyObject(this);
            }
            _flags &= ~SoodaObjectFlags.WrittenIntoDatabase;
        }

        internal void ResetObjectDirty()
        {
            _flags &= ~(SoodaObjectFlags.Dirty | SoodaObjectFlags.WrittenIntoDatabase);
        }

        public virtual ClassInfo GetClassInfo()
        {
            throw new NotImplementedException();
        }

        public string GetObjectKeyString()
        {
            return String.Format("{0}[{1}]", GetClassInfo().Name, GetPrimaryKeyValue());
        }

        public object GetPrimaryKeyValue()
        {
            return _primaryKeyValue;
        }

        protected void SetPrimaryKeySubValue(object keyValue, int valueOrdinal, int totalValues)
        {
            var tuple = (SoodaTuple) _primaryKeyValue;
            if (tuple == null)
                _primaryKeyValue = tuple = new SoodaTuple(totalValues);
            tuple.SetValue(valueOrdinal, keyValue);
            if (tuple.IsAllNotNull())
            {
                if (FieldValues != null)
                    PropagatePrimaryKeyToFields();
                if (IsRegisteredInTransaction())
                    throw new SoodaException("Cannot set primary key value more than once.");

                RegisterObjectInTransaction();
            }
        }

        protected internal void SetPrimaryKeyValue(object keyValue)
        {
            if (_primaryKeyValue == null)
            {
                _primaryKeyValue = keyValue;
                if (FieldValues != null)
                    PropagatePrimaryKeyToFields();
                RegisterObjectInTransaction();
                //if (InsertMode == false)
                //    SetObjectDirty();
            }
            else if (IsRegisteredInTransaction())
            {
                throw new SoodaException("Cannot set primary key value more than once.");
            }
        }

        protected internal virtual void AfterDeserialize()
        {
        }

        protected virtual void InitNewObject()
        {
        }

        private void LoadDataFromRecord(IDataRecord reader, int firstColumnIndex, TableInfo[] tables,
            int tableIndex)
        {
            int recordPos = firstColumnIndex;
            bool first = true;

            EnsureFieldsInited(true);

            int i;
            int oldDataLoadedMask = _dataLoadedMask;

            for (i = tableIndex; i < tables.Length; ++i)
            {
                TableInfo table = tables[i];
                // logger.Debug("Loading data from table {0}. Number of fields: {1} Record pos: {2} Table index {3}.", table.NameToken, table.Fields.Count, recordPos, tableIndex);

                if (table.OrdinalInClass == 0 && !first)
                {
                    // logger.Trace("Found table 0 of another object. Exiting.");
                    break;
                }

                foreach (FieldInfo field in table.Fields)
                {
                    // don't load primary keys 
                    if (!field.IsPrimaryKey)
                    {
                        try
                        {
                            int ordinal = field.ClassUnifiedOrdinal;

                            if (!IsFieldDirty(ordinal))
                            {
                                FieldValues.SetFieldValue(ordinal,
                                    reader.IsDBNull(recordPos)
                                        ? null
                                        : GetFieldHandler(field.ClassUnifiedOrdinal).RawRead(reader, recordPos));
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.Error("Error while reading field {0}.{1}: {2}", table.NameToken, field.Name, ex);
                            throw;
                        }
                    }
                    recordPos++;
                }

                SetDataLoaded(table.OrdinalInClass);
                first = false;
            }

            if (!IsObjectDirty() && (!FromCache || _dataLoadedMask != oldDataLoadedMask) &&
                GetTransaction().CachingPolicy.ShouldCacheObject(this))
            {
                TimeSpan expirationTimeout;
                bool slidingExpiration;

                if (GetTransaction()
                    .CachingPolicy.GetExpirationTimeout(this, out expirationTimeout, out slidingExpiration))
                {
                    GetTransaction()
                        .Cache.Add(GetClassInfo().GetRootClass().Name, GetPrimaryKeyValue(), GetCacheEntry(),
                            expirationTimeout,
                            slidingExpiration);
                    FromCache = true;
                }
            }

            // if we've started with a first table and there are more to be processed
            if (tableIndex == 0 && i != tables.Length)
            {
                // logger.Trace("Materializing extra objects...");
                for (; i < tables.Length; ++i)
                {
                    TableInfo table = tables[i];
                    if (table.OrdinalInClass == 0)
                    {
                        GetTransaction().Statistics.RegisterExtraMaterialization();
                        SoodaStatistics.Global.RegisterExtraMaterialization();

                        // logger.Trace("Materializing {0} at {1}", table.NameToken, recordPos);

                        int pkOrdinal = table.OwnerClass.GetFirstPrimaryKeyField().OrdinalInTable;
                        if (reader.IsDBNull(recordPos + pkOrdinal))
                        {
                            // logger.Trace("Object is null. Skipping.");
                        }
                        else
                        {
                            ISoodaObjectFactory factory = GetTransaction().GetFactory(table.OwnerClass);
                            GetRefFromRecordHelper(GetTransaction(), factory, reader, recordPos, tables, i);
                        }
                    }
                    //else
                    //{
                    // TODO - can this be safely called?
                    //}
                    recordPos += table.Fields.Count;
                }
                // logger.Trace("Finished materializing extra objects.");
            }
        }

        internal void EnsureFieldsInited()
        {
            EnsureFieldsInited(false);
        }

        private void EnsureFieldsInited(bool justLoading)
        {
            if (FieldValues == null)
                InitFieldData(justLoading);
        }

        internal void EnsureDataLoaded(int tableNumber)
        {
            if (!IsDataLoaded(tableNumber))
            {
                EnsureFieldsInited();
                LoadData(tableNumber);
            }
            else if (InsertMode)
            {
                EnsureFieldsInited();
            }
        }

        private void OldLoadAllData() //wash
        {
            // TODO - OPTIMIZE: LOAD DATA FROM ALL TABLES IN A SINGLE QUERY

            for (int i = 0; i < GetClassInfo().UnifiedTables.Count; ++i)
            {
                if (!IsDataLoaded(i))
                {
                    LoadData(i);
                }
            }
        }

        private void LoadData(int tableNumber)
        {
            LoadDataWithKey(GetPrimaryKeyValue(), tableNumber);
        }

        protected void LoadReadOnlyObject(object keyVal)
        {
            InsertMode = false;
            SetPrimaryKeyValue(keyVal);
            // #warning FIX ME
            LoadDataWithKey(keyVal, 0);
        }

        protected void LoadDataWithKey(object keyVal, int tableNumber)
        {
            EnsureFieldsInited();

            if (IsDataLoaded(tableNumber))
                return;

            if (Logger.IsTraceEnabled)
            {
                Logger.Trace("Loading data for {0}({1}) from table #{2}", GetClassInfo().Name, keyVal, tableNumber);
            }

            try
            {
                SoodaDataSource ds = GetTransaction().OpenDataSource(GetClassInfo().GetDataSource());
                TableInfo[] loadedTables;

                bool dsIsOpened = ds.IsOpen; //+wash
                if (!dsIsOpened) ds.Open(); //+wash

                using (IDataReader record = ds.LoadObjectTable(this, keyVal, tableNumber, out loadedTables))
                {
                    if (record == null)
                    {
                        Logger.Error("LoadObjectTable() failed for {0}", GetObjectKeyString());
                        GetTransaction().UnregisterObject(this);
                        throw new SoodaObjectNotFoundException(String.Format("Object {0} not found in the database",
                            GetObjectKeyString()));
                    }

                    if (Logger.IsTraceEnabled)
                    {
                        for (int i = 0; i < loadedTables.Length; ++i)
                        {
                            Logger.Trace("loadedTables[{0}] = {1}", i, loadedTables[i].NameToken);
                        }
                    }

                    LoadDataFromRecord(record, 0, loadedTables, 0);
                    record.Close();
                    if (!dsIsOpened) ds.Close(); //+wash
                }
            }
            catch (Exception ex)
            {
                GetTransaction().UnregisterObject(this);
                Logger.Error("Exception in LoadDataWithKey({0}): {1}", GetObjectKeyString(), ex);
                throw;
            }
        }

        protected void RegisterObjectInTransaction()
        {
            GetTransaction().RegisterObject(this);
        }

        protected bool IsRegisteredInTransaction()
        {
            return GetTransaction().IsRegistered(this);
        }

        private void SaveOuterReferences()
        {
            // iterate outer references
            List<KeyValuePair<int, SoodaObject>> brokenReferences = null;

            foreach (FieldInfo fi in GetClassInfo().UnifiedFields)
            {
                if (fi.ReferencedClass == null)
                    continue;

                object v = FieldValues.GetBoxedFieldValue(fi.ClassUnifiedOrdinal);
                if (v != null)
                {
                    ISoodaObjectFactory factory = GetTransaction().GetFactory(fi.ReferencedClass);
                    SoodaObject obj = factory.TryGet(GetTransaction(), v);

                    if (obj != null && obj != this && obj.IsInsertMode() && !obj.InsertedIntoDatabase)
                    {
                        if (obj.VisitedOnCommit && !obj.WrittenIntoDatabase)
                        {
                            // cyclic reference
                            if (!fi.IsNullable)
                                throw new Exception("Cyclic reference between " + GetObjectKeyString() + " and " +
                                                    obj.GetObjectKeyString());
                            if (brokenReferences == null)
                            {
                                CopyOnWrite();
                                brokenReferences = new List<KeyValuePair<int, SoodaObject>>();
                            }
                            brokenReferences.Add(new KeyValuePair<int, SoodaObject>(fi.ClassUnifiedOrdinal, obj));
                            FieldValues.SetFieldValue(fi.ClassUnifiedOrdinal, null);
                        }
                        else
                        {
                            obj.SaveObjectChanges();
                        }
                    }
                }
            }
            if (brokenReferences != null)
            {
                // insert this object without the cyclic references
                CommitObjectChanges();

                foreach (KeyValuePair<int, SoodaObject> pair in brokenReferences)
                {
                    int ordinal = pair.Key;
                    SoodaObject obj = pair.Value;
                    // insert referenced object
                    obj.SaveObjectChanges();
                    // restore reference
                    FieldValues.SetFieldValue(ordinal, obj.GetPrimaryKeyValue());
                }
            }
        }

        internal void SaveObjectChanges()
        {
            VisitedOnCommit = true;
            if (WrittenIntoDatabase)
                return;

            if (IsObjectDirty())
            {
                SaveOuterReferences();
            }

            if ((IsObjectDirty() || IsInsertMode()) && !WrittenIntoDatabase)
            {
                // deletes are performed in a separate pass
                if (!IsMarkedForDelete())
                {
                    CommitObjectChanges();
                }
                WrittenIntoDatabase = true;
            }
            else if (PostCommitForced)
            {
                GetTransaction().AddToPostCommitQueue(this);
            }
        }

        internal void CommitObjectChanges()
        {
            SoodaDataSource ds = GetTransaction().OpenDataSource(GetClassInfo().GetDataSource());

            try
            {
                EnsureFieldsInited();
                ds.SaveObjectChanges(this, GetTransaction().IsPrecommit);
            }
            catch (Exception e)
            {
                throw new SoodaDatabaseException("Cannot save object to the database " + e.Message, e);
            }

            //GetTransaction().AddToPostCommitQueue(this);
        }

        internal void InvalidateCacheAfterCommit()
        {
            var reason = IsMarkedForDelete()
                ? SoodaCacheInvalidateReason.Deleted
                : (IsInsertMode()
                    ? SoodaCacheInvalidateReason.Inserted
                    : SoodaCacheInvalidateReason.Updated);

            GetTransaction().Cache.Invalidate(GetClassInfo().GetRootClass().Name, GetPrimaryKeyValue(), reason);
        }

        internal void PostCommit()
        {
            if (IsInsertMode())
            {
                if (AreObjectTriggersEnabled()) AfterObjectInsert();
                InsertMode = false;
            }
            else
            {
                if (AreObjectTriggersEnabled())
                    AfterObjectUpdate();
            }
        }

        internal void CallBeforeCommitEvent()
        {
            if (AreObjectTriggersEnabled())
            {
                if (IsInsertMode())
                    BeforeObjectInsert();
                else
                    BeforeObjectUpdate();
            }
            GetTransaction().AddToPostCommitQueue(this);
        }

        private void SerializePrimaryKey(XmlWriter xw)
        {
            foreach (var ordinal in GetClassInfo().GetPrimaryKeyFields().Select(fi => fi.ClassUnifiedOrdinal))
            {
                xw.WriteStartElement("key");
                xw.WriteAttributeString("ordinal", ordinal.ToString(CultureInfo.InvariantCulture));
                GetFieldHandler(ordinal).Serialize(FieldValues.GetBoxedFieldValue(ordinal), xw);
                xw.WriteEndElement();
            }
        }

        // create an empty object just to make sure that the deserialization
        // will find it before any references are used.
        // 
        internal void PreSerialize(XmlWriter xw, SoodaSerializeOptions options)
        {
            if (!IsInsertMode() && !IsMarkedForDelete())
                return;

            xw.WriteStartElement("object");
            xw.WriteAttributeString("mode", IsMarkedForDelete() ? "update" : "insert");
            xw.WriteAttributeString("class", GetClassInfo().Name);

            if (IsMarkedForDelete())
                xw.WriteAttributeString("delete", "true");

            SerializePrimaryKey(xw);
            xw.WriteEndElement();
        }

        internal void Serialize(XmlWriter xw, SoodaSerializeOptions options)
        {
            if (IsMarkedForDelete())
                return;

            xw.WriteStartElement("object");
            xw.WriteAttributeString("mode", "update");
            xw.WriteAttributeString("class", GetClassInfo().Name);

            if (!IsObjectDirty())
                xw.WriteAttributeString("dirty", "false");

            if (!AreObjectTriggersEnabled())
                xw.WriteAttributeString("disableobjecttriggers", "true");

            if (PostCommitForced)
                xw.WriteAttributeString("forcepostcommit", "true");

            Logger.Trace("Serializing " + GetObjectKeyString() + "...");
            EnsureFieldsInited();

            if ((options & SoodaSerializeOptions.IncludeNonDirtyFields) != 0 && !IsAllDataLoaded())
                LoadAllData();

            SerializePrimaryKey(xw);

            foreach (FieldInfo fi in GetClassInfo().UnifiedFields)
            {
                if (fi.IsPrimaryKey)
                    continue;

                int ordinal = fi.ClassUnifiedOrdinal;
                bool dirty = IsFieldDirty(ordinal);
                if (dirty || (options & SoodaSerializeOptions.IncludeNonDirtyFields) != 0)
                {
                    xw.WriteStartElement("field");
                    xw.WriteAttributeString("name", fi.Name);
                    GetFieldHandler(ordinal).Serialize(FieldValues.GetBoxedFieldValue(ordinal), xw);
                    if (!dirty)
                        xw.WriteAttributeString("dirty", "false");
                    xw.WriteEndElement();
                }
            }

            if ((options & SoodaSerializeOptions.IncludeDebugInfo) != 0)
            {
                xw.WriteStartElement("debug");
                xw.WriteAttributeString("transaction", (_transaction != null) ? "notnull" : "null");
                xw.WriteAttributeString("objectDirty", IsObjectDirty() ? "true" : "false");
                xw.WriteAttributeString("dataLoaded", IsAllDataLoaded() ? "true" : "false");
                xw.WriteAttributeString("disableTriggers", AreFieldUpdateTriggersEnabled() ? "false" : "true");
                xw.WriteAttributeString("disableObjectTriggers", AreObjectTriggersEnabled() ? "false" : "true");
                xw.WriteEndElement();
            }

            NameValueCollection persistentValues = GetTransaction().GetPersistentValues(this);
            if (persistentValues != null)
            {
                foreach (string s in persistentValues.AllKeys)
                {
                    xw.WriteStartElement("persistent");
                    xw.WriteAttributeString("name", s);
                    xw.WriteAttributeString("value", persistentValues[s]);
                    xw.WriteEndElement();
                }
            }

            xw.WriteEndElement();
        }

        internal void DeserializePersistentField(XmlReader reader)
        {
            string name = reader.GetAttribute("name");
            string value = reader.GetAttribute("value");

            SetTransactionPersistentValue(name, value);
        }

        internal void DeserializeField(XmlReader reader)
        {
            var name = reader.GetAttribute("name");
            if (name == null || reader.GetAttribute("dirty") == "false") return;

            EnsureFieldsInited();
            CopyOnWrite();

            int fieldOrdinal = GetFieldInfo(name).ClassUnifiedOrdinal;
            SoodaFieldHandler field = GetFieldHandler(fieldOrdinal);
            object val = field.Deserialize(reader);

            // Console.WriteLine("Deserializing field: {0}", name);

            PropertyInfo pi = GetType().GetProperty(name);
            if (pi.PropertyType.IsSubclassOf(typeof (SoodaObject)))
            {
                if (val != null)
                {
                    ISoodaObjectFactory fact = GetTransaction().GetFactory(pi.PropertyType);
                    val = fact.GetRef(GetTransaction(), val);
                }
                pi.SetValue(this, val, null);
            }
            else
            {
                // set as raw
                FieldValues.SetFieldValue(fieldOrdinal, val);
                SetFieldDirty(fieldOrdinal, true);
            }

            SetObjectDirty();
            //else
            //{
            //    // Console.WriteLine("Not deserializing field: {0}", name);
            //}
        }

        private void SetFieldValue(int fieldOrdinal, object value)
        {
            CopyOnWrite();
            FieldValues.SetFieldValue(fieldOrdinal, value);
            SetFieldDirty(fieldOrdinal, true);
            SetObjectDirty();
        }

        internal void SetPlainFieldValue(int tableNumber, string fieldName, int fieldOrdinal, object newValue,
            SoodaFieldUpdateDelegate before, SoodaFieldUpdateDelegate after)
        {
            EnsureFieldsInited();

            if (AreFieldUpdateTriggersEnabled())
            {
                EnsureDataLoaded(tableNumber);
                try
                {
                    var oldValue = FieldValues.GetBoxedFieldValue(fieldOrdinal);
                    if (Equals(oldValue, newValue))
                        return;

                    if (before != null)
                        before(oldValue, newValue);

                    SetFieldValue(fieldOrdinal, newValue);
                    SetObjectDirty();
                    if (after != null)
                        after(oldValue, newValue);
                }
                catch (Exception e)
                {
                    throw new Exception("BeforeFieldUpdate raised an exception: ", e);
                }
            }
            else
            {
                // optimization here - we don't even need to load old values from database
                SetFieldValue(fieldOrdinal, newValue);
            }
        }

        internal void SetRefFieldValue(int tableNumber, string fieldName, int fieldOrdinal, SoodaObject newValue,
            SoodaObject[] refcache, int refCacheOrdinal, ISoodaObjectFactory factory)
        {
            if (newValue != null)
            {
                // transaction check
//wash{
                if (newValue.GetTransaction() != GetTransaction())
                {
                    if (newValue.GetTransaction().GetObject(newValue.GetType(), newValue.GetPrimaryKeyValue()) == null)
                        throw new SoodaException("Attempted to assign object " + newValue.GetObjectKeyString() +
                                                 " from another transaction to " + GetObjectKeyString() + "." +
                                                 fieldName);
                }
//}wash
            }

            EnsureFieldsInited();
            EnsureDataLoaded(tableNumber);

            SoodaObject oldValue = null;

            SoodaObjectImpl.GetRefFieldValue(ref oldValue, this, tableNumber, fieldOrdinal, GetTransaction(), factory);
            if (Equals(oldValue, newValue))
                return;
            var triggerArgs = new object[] {oldValue, newValue};

            if (AreFieldUpdateTriggersEnabled())
            {
                MethodInfo mi = GetType().GetMethod("BeforeFieldUpdate_" + fieldName,
                    BindingFlags.Instance | BindingFlags.FlattenHierarchy |
                    BindingFlags.NonPublic | BindingFlags.Public);
                if (mi != null)
                    mi.Invoke(this, triggerArgs);
            }
            FieldInfo fieldInfo = GetClassInfo().UnifiedFields[fieldOrdinal];
            StringCollection backRefCollections = GetTransaction().Schema.GetBackRefCollections(fieldInfo);
            if (oldValue != null && backRefCollections != null)
            {
                foreach (string collectionName in backRefCollections)
                {
                    PropertyInfo coll = oldValue.GetType().GetProperty(collectionName,
                        BindingFlags.Instance |
                        BindingFlags.FlattenHierarchy |
                        BindingFlags.Public);
                    if (coll == null)
                        throw new Exception(collectionName + " not found in " + oldValue.GetType().Name +
                                            " while setting " + GetType().Name + "." + fieldName);
                    var listInternal = (ISoodaObjectListInternal) coll.GetValue(oldValue, null);
                    listInternal.InternalRemove(this);
                }
            }
            SetFieldValue(fieldOrdinal, newValue == null ? null : newValue.GetPrimaryKeyValue());
            refcache[refCacheOrdinal] = null;
            if (newValue != null && backRefCollections != null)
            {
                foreach (string collectionName in backRefCollections)
                {
                    PropertyInfo coll = newValue.GetType().GetProperty(collectionName,
                        BindingFlags.Instance |
                        BindingFlags.FlattenHierarchy |
                        BindingFlags.Public);
                    if (coll == null)
                        throw new Exception(collectionName + " not found in " + newValue.GetType().Name +
                                            " while setting " + GetType().Name + "." + fieldName);
                    var listInternal = (ISoodaObjectListInternal) coll.GetValue(newValue, null);
                    listInternal.InternalAdd(this);
                }
            }
            if (AreFieldUpdateTriggersEnabled())
            {
                MethodInfo mi = GetType().GetMethod("AfterFieldUpdate_" + fieldName,
                    BindingFlags.Instance | BindingFlags.FlattenHierarchy |
                    BindingFlags.NonPublic | BindingFlags.Public);
                if (mi != null)
                    mi.Invoke(this, triggerArgs);
            }
        }

        public object Evaluate(SoqlExpression expr)
        {
            return Evaluate(expr, true);
        }

        public object Evaluate(SoqlExpression expr, bool throwOnError)
        {
            try
            {
                var ec = new EvaluateContext(this);
                return expr.Evaluate(ec);
            }
            catch
            {
                if (throwOnError) throw;
                return null;
            }
        }

        public object Evaluate(string[] propertyAccessChain, bool throwOnError)
        {
            try
            {
                object currentObject = this;

                for (int i = 0; i < propertyAccessChain.Length && currentObject != null; ++i)
                {
                    var pi = currentObject.GetType().GetProperty(propertyAccessChain[i]);
                    currentObject = pi.GetValue(currentObject, null) as SoodaObject;
                }
                return currentObject;
            }
            catch
            {
                if (throwOnError) throw;
                return null;
            }
        }

        public object Evaluate(string propertyAccessChain)
        {
            return Evaluate(propertyAccessChain, true);
        }

        public object Evaluate(string propertyAccessChain, bool throwOnError)
        {
            return Evaluate(propertyAccessChain.Split('.'), throwOnError);
        }

        private static ISoodaObjectFactory GetFactoryFromRecord(SoodaTransaction tran, ISoodaObjectFactory factory,
            IDataRecord record, int firstColumnIndex, object keyValue, bool loadData)
        {
            ClassInfo classInfo = factory.GetClassInfo();
            var subclasses = tran.Schema.GetSubclasses(classInfo);

            if (subclasses.Count == 0) return factory;

            // more complex case - we have to determine the actual factory to be used for object creation

            int selectorFieldOrdinal = loadData ? classInfo.SubclassSelectorField.OrdinalInTable : record.FieldCount - 1;
            object selectorActualValue = factory.GetFieldHandler(selectorFieldOrdinal)
                .RawRead(record, firstColumnIndex + selectorFieldOrdinal);

            IComparer comparer = selectorActualValue is string
                ? (IComparer) CaseInsensitiveComparer.DefaultInvariant
                : Comparer.DefaultInvariant;

            if (0 == comparer.Compare(selectorActualValue, classInfo.SubclassSelectorValue))
                return factory;

            ISoodaObjectFactory newFactory;
            if (!factory.GetClassInfo().DisableTypeCache)
            {
                newFactory = SoodaTransaction.SoodaObjectFactoryCache.FindObjectFactory(classInfo.Name, keyValue);
                if (newFactory != null)
                    return newFactory;
            }

            foreach (ClassInfo ci in subclasses)
            {
                if (0 == comparer.Compare(selectorActualValue, ci.SubclassSelectorValue))
                {
                    newFactory = tran.GetFactory(ci);
                    SoodaTransaction.SoodaObjectFactoryCache.SetObjectFactory(classInfo.Name, keyValue, newFactory);
                    return newFactory;
                }
            }

            throw new Exception("Cannot determine subclass. Selector actual value: " + selectorActualValue +
                                " base class: " + classInfo.Name);
        }

        private static SoodaObject GetRefFromRecordHelper(SoodaTransaction tran, ISoodaObjectFactory factory,
            IDataRecord record, int firstColumnIndex, TableInfo[] loadedTables, int tableIndex, bool loadData)
        {
            object keyValue;

            FieldInfo[] pkFields = factory.GetClassInfo().GetPrimaryKeyFields();
            if (pkFields.Length == 1)
            {
                int pkFieldOrdinal = loadData ? firstColumnIndex + pkFields[0].OrdinalInTable : 0;
                try
                {
                    keyValue = factory.GetPrimaryKeyFieldHandler().RawRead(record, pkFieldOrdinal);
                }
                catch (Exception ex)
                {
                    Logger.Error("Error while reading field {0}.{1}: {2}", factory.GetClassInfo().Name, pkFieldOrdinal,
                        ex);
                    throw;
                }
            }
            else
            {
                var pkParts = new object[pkFields.Length];
                for (int currentPkPart = 0; currentPkPart < pkFields.Length; currentPkPart++)
                {
                    int pkFieldOrdinal = loadData
                        ? firstColumnIndex + pkFields[currentPkPart].OrdinalInTable
                        : currentPkPart;
                    SoodaFieldHandler handler = factory.GetFieldHandler(pkFieldOrdinal);
                    pkParts[currentPkPart] = handler.RawRead(record, pkFieldOrdinal);
                }
                keyValue = new SoodaTuple(pkParts);
                //logger.Debug("Tuple: {0}", keyValue);
            }

            //wash{
            SoodaObject retVal = factory.TryGet(tran, keyValue);
            if (retVal != null)
            {
                if (loadData && !retVal.IsDataLoaded(0))
                    retVal.LoadDataFromRecord(record, firstColumnIndex, loadedTables, tableIndex);
                return retVal;
            }
            //}wash

            factory = GetFactoryFromRecord(tran, factory, record, firstColumnIndex, keyValue, loadData);
            retVal = factory.GetRawObject(tran);
            tran.Statistics.RegisterObjectUpdate();
            SoodaStatistics.Global.RegisterObjectUpdate();
            retVal.InsertMode = false;
            retVal.SetPrimaryKeyValue(keyValue);
            if (loadData)
                retVal.LoadDataFromRecord(record, firstColumnIndex, loadedTables, tableIndex);
            return retVal;
        }


        public static SoodaObject GetRefFromRecordHelper(SoodaTransaction tran, ISoodaObjectFactory factory,
            IDataRecord record, int firstColumnIndex, TableInfo[] loadedTables, int tableIndex)
        {
            return GetRefFromRecordHelper(tran, factory, record, firstColumnIndex, loadedTables, tableIndex, true);
        }

        internal static SoodaObject GetRefFromKeyRecordHelper(SoodaTransaction tran, ISoodaObjectFactory factory,
            IDataRecord record)
        {
            return GetRefFromRecordHelper(tran, factory, record, 0, null, -1, false);
        }

        public static SoodaObject GetRefHelper(SoodaTransaction tran, ISoodaObjectFactory factory, int keyValue)
        {
            return GetRefHelper(tran, factory, (object) keyValue);
        }

        public static SoodaObject GetRefHelper(SoodaTransaction tran, ISoodaObjectFactory factory, string keyValue)
        {
            return GetRefHelper(tran, factory, (object) keyValue);
        }

        public static SoodaObject GetRefHelper(SoodaTransaction tran, ISoodaObjectFactory factory, long keyValue)
        {
            return GetRefHelper(tran, factory, (object) keyValue);
        }

        public static SoodaObject GetRefHelper(SoodaTransaction tran, ISoodaObjectFactory factory, Guid keyValue)
        {
            return GetRefHelper(tran, factory, (object) keyValue);
        }

        public static SoodaObject GetRefHelper(SoodaTransaction tran, ISoodaObjectFactory factory, object keyValue)
        {
            SoodaObject retVal = factory.TryGet(tran, keyValue);
            if (retVal != null)
                return retVal;

            ClassInfo classInfo = factory.GetClassInfo();
            if (classInfo.InheritsFromClass != null && tran.ExistsObjectWithKey(classInfo.GetRootClass().Name, keyValue))
                throw new SoodaObjectNotFoundException();

            if (classInfo.GetSubclassesForSchema(tran.Schema).Count > 0)
            {
                ISoodaObjectFactory newFactory = null;

                if (!classInfo.DisableTypeCache)
                {
                    newFactory = SoodaTransaction.SoodaObjectFactoryCache.FindObjectFactory(classInfo.Name, keyValue);
                }

                if (newFactory != null)
                {
                    factory = newFactory;
                }
                else
                {
                    // if the class is actually inherited, we delegate the responsibility
                    // to the appropriate GetRefFromRecord which will be called by the snapshot

                    SoqlBooleanExpression where = null;
                    FieldInfo[] pkFields = classInfo.GetPrimaryKeyFields();
                    var par = new object[pkFields.Length];
                    for (int i = 0; i < pkFields.Length; ++i)
                    {
                        par[i] = SoodaTuple.GetValue(keyValue, i);
                        //SoqlBooleanExpression cmp = new SoqlBooleanRelationalExpression(
                        //    new SoqlPathExpression(pkFields[i].Name),
                        //    new SoqlParameterLiteralExpression(i),
                        //    SoqlRelationalOperator.Equal);
                        SoqlBooleanExpression cmp = Soql.FieldEqualsParam(pkFields[i].Name, i);
                        where = where == null ? cmp : where.And(cmp);
                    }
                    var whereClause = new SoodaWhereClause(where, par);
                    IList list = factory.GetList(tran, whereClause, SoodaOrderBy.Unsorted,
                        SoodaSnapshotOptions.NoTransaction | SoodaSnapshotOptions.NoWriteObjects |
                        SoodaSnapshotOptions.NoCache);
                    if (list.Count == 1)
                        return (SoodaObject) list[0];
                    if (list.Count == 0)
                        throw new SoodaObjectNotFoundException("No matching object.");

                    throw new SoodaObjectNotFoundException("More than one object found. Fatal error.");
                }
            }

            retVal = factory.GetRawObject(tran);
            tran.Statistics.RegisterObjectUpdate();
            SoodaStatistics.Global.RegisterObjectUpdate();
            if (factory.GetClassInfo().ReadOnly)
            {
                retVal.LoadReadOnlyObject(keyValue);
            }
            else
            {
                retVal.SetUpdateMode(keyValue);
            }
            return retVal;
        }

        public override string ToString()
        {
            var keyVal = GetPrimaryKeyValue();
            return keyVal == null ? string.Empty : keyVal.ToString();
        }

        public void InitRawObject(SoodaTransaction tran)
        {
            _transaction = tran;
            _dataLoadedMask = 0;
            _flags = SoodaObjectFlags.InsertMode;
            _primaryKeyValue = null;
        }

        internal void CopyOnWrite()
        {
            if (FromCache)
            {
                FieldValues = FieldValues.Clone();
                FromCache = false;
            }
        }

        protected NameValueCollection GetTransactionPersistentValues()
        {
            return GetTransaction().GetPersistentValues(this);
        }

        protected void SetTransactionPersistentValue(string name, string value)
        {
            SetObjectDirty();
            GetTransaction().SetPersistentValue(this, name, value);
        }

        protected string GetTransactionPersistentValue(string name)
        {
            return GetTransaction().GetPersistentValue(this, name);
        }

        public virtual string GetLabel(bool throwOnError)
        {
            string labelField = GetClassInfo().GetLabel();
            if (labelField == null)
                return null;

            object o = Evaluate(labelField, throwOnError);
            if (o == null)
                return String.Empty;

            var nullable = o as INullable;
            if (nullable != null && nullable.IsNull)
                return String.Empty;

            return Convert.ToString(o);
        }

        public static bool operator ==(SoodaObject obj1, SoodaObject obj2)
        {
            if (ReferenceEquals(obj1, obj2))
            {
                return true;
            }
            if (!ReferenceEquals(obj1, null) && !ReferenceEquals(obj2, null))
            {
                return obj1.GetObjectKeyString().Equals(obj2.GetObjectKeyString());
            }
            return false;
        }

        public static bool operator !=(SoodaObject obj1, SoodaObject obj2)
        {
            return !(obj1 == obj2);
        }

        //public static explicit operator int(SoodaObject value)
        //{
        //    return value == null?0: (int)value._primaryKeyValue;
        //}

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj)) return true;
            return Equals(obj as SoodaObject);
        }


        public override int GetHashCode()
        {
            return GetObjectKeyString().GetHashCode();
        }

        #region 'Loaded' state management

        private bool IsAnyDataLoaded()
        {
            return _dataLoadedMask != 0;
        }

        private bool IsAllDataLoaded()
        {
            // 2^N-1 has exactly N lower bits set to 1
            return _dataLoadedMask == (1 << GetClassInfo().UnifiedTables.Count) - 1;
        }

        private void SetAllDataLoaded()
        {
            // 2^N-1 has exactly N lower bits set to 1
            _dataLoadedMask = (1 << GetClassInfo().UnifiedTables.Count) - 1;
        }

        private void SetAllDataNotLoaded()
        {
            _dataLoadedMask = 0;
        }

        private bool IsDataLoaded(int tableNumber)
        {
            return (_dataLoadedMask & (1 << tableNumber)) != 0;
        }

        private void SetDataLoaded(int tableNumber)
        {
            _dataLoadedMask |= (1 << tableNumber);
        }

        #endregion

        #region Nested type: EvaluateContext

        private class EvaluateContext : ISoqlEvaluateContext
        {
            private readonly SoodaObject _rootObject;

            public EvaluateContext(SoodaObject rootObject)
            {
                _rootObject = rootObject;
            }

            #region ISoqlEvaluateContext Members

            public object GetRootObject()
            {
                return _rootObject;
            }

            public object GetParameter(int position)
            {
                throw new Exception("No parameters allowed in evaluation.");
            }

            #endregion
        }

        #endregion

        private FieldInfo GetFieldInfo(string name)
        {
            var ci = GetClassInfo();
            var fi = ci.FindFieldByName(name);
            if (fi == null)
                throw new Exception("Field " + name + " not found in " + ci.Name);
            return fi;
        }

        private object GetTypedFieldValue(FieldInfo fi)
        {
            object value = FieldValues.GetBoxedFieldValue(fi.ClassUnifiedOrdinal);
            if (value != null && fi.References != null)
                value = GetTransaction().GetFactory(fi.References).GetRef(GetTransaction(), value);
            return value;
        }

        public object this[string fieldName]
        {
            get
            {
                var fi = GetFieldInfo(fieldName);
                EnsureDataLoaded(fi.Table.OrdinalInClass);
                return GetTypedFieldValue(fi);
            }
            set
            {
                var fi = GetFieldInfo(fieldName);
                // FIXME: make sure not a static field - because of triggers, refcache and collections
                //if (!fi.IsDynamic)
                //{
                //    // Disallow because:
                //    // - the per-field update triggers wouldn't be called
                //    // - for references, refcache would get out-of-date
                //    // - for references, collections would not be updated
                //    // Alternatively we might just set the property via reflection. This wouldn't suffer from the above problems.
                //    throw new InvalidOperationException("Cannot set non-dynamic field " + fieldName + " with an indexer");
                //}
                if (value == null)
                {
                    if (!fi.IsNullable)
                        throw new ArgumentNullException("Cannot set non-nullable " + fieldName + " to null");
                }
                else
                {
                    Type type = fi.References != null
                        ? GetTransaction().GetFactory(fi.References).TheType
                        : fi.GetNullableFieldHandler().GetFieldType();
                    if (!type.IsInstanceOfType(value))
                        throw new InvalidCastException("Cannot set " + fieldName + " of type " + type + " to " +
                                                       value.GetType());
                }
                EnsureDataLoaded(fi.Table.OrdinalInClass);
                if (AreFieldUpdateTriggersEnabled())
                {
                    var oldValue = GetTypedFieldValue(fi);
                    if (Equals(oldValue, value))
                        return;

                    BeforeFieldUpdate(fieldName, oldValue, value);
                    var so = value as SoodaObject;
                    SetFieldValue(fi.ClassUnifiedOrdinal, so != null ? so.GetPrimaryKeyValue() : value);
                    AfterFieldUpdate(fieldName, oldValue, value);
                }
                else
                {
                    var so = value as SoodaObject;
                    SetFieldValue(fi.ClassUnifiedOrdinal, so != null ? so.GetPrimaryKeyValue() : value);
                }
            }
        }

#if DOTNET4
        public override bool TryGetMember(System.Dynamic.GetMemberBinder binder, out object result)
        {
            result = this[binder.Name];
            return true;
        }

        public override bool TrySetMember(System.Dynamic.SetMemberBinder binder, object value)
        {
            this[binder.Name] = value;
            return true;
        }
#endif

        //+wash

        #region LoadAllData

        internal void LoadAllData()
        {
            if (IsAllDataLoaded()) return;

            if (IsAnyDataLoaded())
            {
                OldLoadAllData();
                return;
            }

            EnsureFieldsInited();

            if (IsAllDataLoaded()) return;

            if (IsAnyDataLoaded())
            {
                OldLoadAllData();
                return;
            }

            var keyVal = GetPrimaryKeyValue();

            try
            {
                var ds = GetTransaction().OpenDataSource(GetClassInfo().GetDataSource());
                TableInfo[] loadedTables;

                var dsIsOpened = ds.IsOpen;
                if (!dsIsOpened) ds.Open(); //+wash

                using (var record = ds.LoadAllObjectTables(this, keyVal, out loadedTables))
                {
                    if (record == null)
                    {
                        GetTransaction().UnregisterObject(this);
                        throw new SoodaObjectNotFoundException(String.Format("Object {0} not found in the database",
                            GetObjectKeyString()));
                    }

                    if (Logger.IsTraceEnabled)
                    {
                        for (int i = 0; i < loadedTables.Length; ++i)
                        {
                            Logger.Trace("loadedTables[{0}] = {1}", i, loadedTables[i].NameToken);
                        }
                    }

                    LoadAllDataFromRecord(record, 0, loadedTables, 0);
                    record.Close();
                    if (!dsIsOpened) ds.Close(); //+wash
                }
            }
            catch (Exception ex)
            {
                GetTransaction().UnregisterObject(this);
                Logger.Error("Exception in LoadDataWithKey({0}): {1}", GetObjectKeyString(), ex);
                throw;
            }
        }

        private void LoadAllDataFromRecord(IDataRecord reader, int firstColumnIndex, TableInfo[] tables,
            int tableIndex)
        {
            int recordPos = firstColumnIndex;
            bool first = true;

            EnsureFieldsInited(true);

            int i;
            int oldDataLoadedMask = _dataLoadedMask;

            for (i = tableIndex; i < tables.Length; ++i)
            {
                TableInfo table = tables[i];
                // logger.Debug("Loading data from table {0}. Number of fields: {1} Record pos: {2} Table index {3}.", table.NameToken, table.Fields.Count, recordPos, tableIndex);

                if (table.OrdinalInClass == 0 && !first)
                {
                    // logger.Trace("Found table 0 of another object. Exiting.");
                    break;
                }

                foreach (var field in table.Fields)
                {
                    // don't load primary keys 
                    if (field.IsPrimaryKey)
                    {
                        if (first) //wash
                            recordPos++;
                        continue;
                    }

                    try
                    {
                        var ordinal = field.ClassUnifiedOrdinal;

                        if (!IsFieldDirty(ordinal))
                        {
                            FieldValues.SetFieldValue(ordinal,
                                reader.IsDBNull(recordPos) ? null : GetFieldHandler(ordinal).RawRead(reader, recordPos));
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.Error("Error while reading field {0}.{1}: {2}", table.NameToken, field.Name, ex);
                        throw;
                    }
                    recordPos++;
                }

                SetDataLoaded(table.OrdinalInClass);
                first = false;
            }

            if (!IsObjectDirty() && (!FromCache || _dataLoadedMask != oldDataLoadedMask) &&
                GetTransaction().CachingPolicy.ShouldCacheObject(this))
            {
                TimeSpan expirationTimeout;
                bool slidingExpiration;

                if (GetTransaction()
                    .CachingPolicy.GetExpirationTimeout(this, out expirationTimeout, out slidingExpiration))
                {
                    GetTransaction()
                        .Cache.Add(GetClassInfo().GetRootClass().Name, GetPrimaryKeyValue(), GetCacheEntry(),
                            expirationTimeout, slidingExpiration);
                    FromCache = true;
                }
            }

            // if we've started with a first table and there are more to be processed
            if (tableIndex == 0 && i != tables.Length)
            {
                // logger.Trace("Materializing extra objects...");
                for (; i < tables.Length; ++i)
                {
                    var table = tables[i];
                    if (table.OrdinalInClass == 0)
                    {
                        GetTransaction().Statistics.RegisterExtraMaterialization();
                        SoodaStatistics.Global.RegisterExtraMaterialization();

                        // logger.Trace("Materializing {0} at {1}", table.NameToken, recordPos);

                        int pkOrdinal = table.OwnerClass.GetFirstPrimaryKeyField().OrdinalInTable;
                        if (reader.IsDBNull(recordPos + pkOrdinal))
                        {
                            // logger.Trace("Object is null. Skipping.");
                        }
                        else
                        {
                            ISoodaObjectFactory factory = GetTransaction().GetFactory(table.OwnerClass);
                            GetRefFromRecordHelper(GetTransaction(), factory, reader, recordPos, tables, i);
                        }
                    }
                    //else
                    //{
                    // TODO - can this be safely called?
                    //}
                    recordPos += table.Fields.Count;
                }
                // logger.Trace("Finished materializing extra objects.");
            }
        }

        #endregion

        //-wash
    }
}