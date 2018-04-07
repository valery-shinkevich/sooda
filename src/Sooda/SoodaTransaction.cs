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

using Sooda.Caching;
using Sooda.Logging;
using Sooda.ObjectMapper;
using Sooda.Schema;
using System;
using System.Collections;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Reflection;
using System.Xml;

namespace Sooda
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;

    public class SoodaTransaction : Component
    {
        #region Fields

        private static readonly Logger TransactionLogger;
        private static IDefaultSoodaTransactionStrategy _defaultTransactionStrategy;

        private readonly SoodaTransaction _previousTransaction;

        private readonly SoodaTransactionOptions _transactionOptions;
        private readonly Dictionary<Type, SoodaRelationTable> _relationTables;
        //private KeyToSoodaObjectMap _objects = new KeyToSoodaObjectMap();
        private readonly SoodaStatistics _statistics;
        private readonly List<WeakSoodaObject> _objectList;
        private Queue _precommitQueue;
        private readonly List<SoodaObject> _deletedObjects;
        private readonly Hashtable _precommittedClassOrRelation;
        private readonly List<SoodaObject> _postCommitQueue;
        private readonly List<SoodaObject> _dirtyObjects;
        private readonly List<SoodaObject> _strongReferences;
        private readonly Dictionary<string, List<WeakSoodaObject>> _objectsByClass;
        private readonly Dictionary<string, List<WeakSoodaObject>> _dirtyObjectsByClass;
        private readonly Dictionary<string, Dictionary<object, WeakSoodaObject>> _objectDictByClass;
        private readonly StringCollection _disabledKeyGenerators = new StringCollection();

        internal readonly List<SoodaDataSource> DataSources;
        private readonly Dictionary<string, ISoodaObjectFactory> _factoryForClassName;
        private readonly Dictionary<Type, ISoodaObjectFactory> _factoryForType;
        private readonly Dictionary<SoodaObject, NameValueCollection> _persistentValues;
        private IsolationLevel _isolationLevel;
        private Assembly _assembly;
        private SchemaInfo _schema;
        internal bool SavingObjects;
        private bool _isPrecommit;

        public static Assembly DefaultObjectsAssembly;

        #endregion

        #region Constructors, Dispose & Finalizer

        static SoodaTransaction()
        {
            TransactionLogger = LogManager.GetLogger("Sooda.Transaction");
            _defaultTransactionStrategy = new SoodaThreadBoundTransactionStrategy();

            var defaultObjectsAssembly = SoodaConfig.GetString("sooda.defaultObjectsAssembly");
            if (defaultObjectsAssembly != null)
                DefaultObjectsAssembly = Assembly.Load(defaultObjectsAssembly);
        }

        public SoodaTransaction() : this(null, SoodaTransactionOptions.Implicit, Assembly.GetCallingAssembly())
        {
        }

        public SoodaTransaction(Assembly objectsAssembly)
            : this(objectsAssembly, SoodaTransactionOptions.Implicit, Assembly.GetCallingAssembly())
        {
        }

        public SoodaTransaction(SoodaTransactionOptions options) : this(null, options, Assembly.GetCallingAssembly())
        {
        }

        public SoodaTransaction(Assembly objectsAssembly, SoodaTransactionOptions options)
            : this(objectsAssembly, options, Assembly.GetCallingAssembly())
        {
        }

        private SoodaTransaction(Assembly objectsAssembly, SoodaTransactionOptions options, Assembly callingAssembly)
        {
            Cache = SoodaCache.DefaultCache;
            CachingPolicy = SoodaCache.DefaultCachingPolicy;
            _persistentValues = new Dictionary<SoodaObject, NameValueCollection>();
            _factoryForType = new Dictionary<Type, ISoodaObjectFactory>();
            _factoryForClassName = new Dictionary<string, ISoodaObjectFactory>();
            DataSources = new List<SoodaDataSource>();
            _objectDictByClass = new Dictionary<string, Dictionary<object, WeakSoodaObject>>();
            _dirtyObjectsByClass = new Dictionary<string, List<WeakSoodaObject>>();
            _objectsByClass = new Dictionary<string, List<WeakSoodaObject>>();
            _strongReferences = new List<SoodaObject>();
            _dirtyObjects = new List<SoodaObject>();
            _postCommitQueue = new List<SoodaObject>();
            _precommittedClassOrRelation = new Hashtable();
            _deletedObjects = new List<SoodaObject>();
            _objectList = new List<WeakSoodaObject>();
            _statistics = new SoodaStatistics();
            _isolationLevel = IsolationLevel.ReadCommitted;
            _relationTables = new Dictionary<Type, SoodaRelationTable>();

            if (objectsAssembly != null)
            {
                ObjectsAssembly = objectsAssembly;
            }
            else
            {
                var attrs =
                    (SoodaStubAssemblyAttribute[])
                        callingAssembly.GetCustomAttributes(typeof (SoodaStubAssemblyAttribute), false);

                ObjectsAssembly = attrs.Length == 1 ? attrs[0].Assembly : DefaultObjectsAssembly;
            }

            _transactionOptions = options;
            if ((options & SoodaTransactionOptions.Implicit) != 0)
            {
                _previousTransaction = _defaultTransactionStrategy.SetDefaultTransaction(this);
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            try
            {
                TransactionLogger.Debug("Disposing transaction");
                if (!disposing) return;
                foreach (var source in DataSources)
                {
                    source.Close();
                    source.Dispose();
                }
#if DOTNET35
                DynamicFieldManager.CloseTransaction(this);
#endif
                if ((_transactionOptions & SoodaTransactionOptions.Implicit) != 0 &&
                    this != _defaultTransactionStrategy.SetDefaultTransaction(_previousTransaction))
                {
                    TransactionLogger.Warn("ActiveTransactionDataStoreSlot has been overwritten by someone.");
                }
            }
            catch (Exception ex)
            {
                TransactionLogger.Error("Error while disposing transaction {0}", ex);
                throw;
            }
        }

        #endregion

        public bool UseWeakReferences { get; set; }

        public static IDefaultSoodaTransactionStrategy DefaultTransactionStrategy
        {
            get { return _defaultTransactionStrategy; }
            set { _defaultTransactionStrategy = value; }
        }

        public static SoodaTransaction ActiveTransaction
        {
            get
            {
                SoodaTransaction retVal = _defaultTransactionStrategy.GetDefaultTransaction();
                if (retVal == null)
                    throw new InvalidOperationException(
                        "There's no implicit transaction currently active. Either use explicit transactions or create a new implicit one.");

                return retVal;
            }
        }

        public static bool HasActiveTransaction
        {
            [DebuggerStepThrough] get { return _defaultTransactionStrategy.GetDefaultTransaction() != null; }
        }

        internal List<WeakSoodaObject> GetObjectsByClassName(string className)
        {
            List<WeakSoodaObject> objects;
            _objectsByClass.TryGetValue(className, out objects);
            return objects;
        }

        internal List<WeakSoodaObject> GetDirtyObjectsByClassName(string className)
        {
            List<WeakSoodaObject> objects;
            _dirtyObjectsByClass.TryGetValue(className, out objects);
            return objects;
        }

        public List<SoodaObject> DirtyObjects
        {
            get { return _dirtyObjects; }
        }

        private Dictionary<object, WeakSoodaObject> GetObjectDictionaryForClass(string className)
        {
            Dictionary<object, WeakSoodaObject> dict;
            if (!_objectDictByClass.TryGetValue(className, out dict))
            {
                dict = new Dictionary<object, WeakSoodaObject>();
                _objectDictByClass[className] = dict;
            }
            return dict;
        }

        private void AddObjectWithKey(string className, object keyValue, SoodaObject obj)
        {
            // Console.WriteLine("AddObjectWithKey('{0}',{1})", className, keyValue);
            if (keyValue == null) keyValue = "";
            GetObjectDictionaryForClass(className)[keyValue] = new WeakSoodaObject(obj);
        }

        private void UnregisterObjectWithKey(string className, object keyValue)
        {
            if (keyValue == null) keyValue = "";
            GetObjectDictionaryForClass(className).Remove(keyValue);
        }

        internal bool ExistsObjectWithKey(string className, object keyValue)
        {
            if (keyValue == null) keyValue = "";
            return FindObjectWithKey(className, keyValue) != null;
        }

        private SoodaObject FindObjectWithKey(string className, object keyValue)
        {
            if (keyValue == null) keyValue = "";
            WeakSoodaObject wo;
            if (!GetObjectDictionaryForClass(className).TryGetValue(keyValue, out wo))
                return null;
            return wo.TargetSoodaObject;
        }

        protected internal void RegisterObject(SoodaObject o)
        {
            // Console.WriteLine("Registering object {0}...", o.GetObjectKey());

            object pkValue = o.GetPrimaryKeyValue();
            // Console.WriteLine("Adding key: " + o.GetObjectKey() + " of type " + o.GetType());
            for (ClassInfo ci = o.GetClassInfo(); ci != null; ci = ci.InheritsFromClass)
            {
                AddObjectWithKey(ci.Name, pkValue, o);

                List<WeakSoodaObject> al;
                if (!_objectsByClass.TryGetValue(ci.Name, out al))
                {
                    al = new List<WeakSoodaObject>();
                    _objectsByClass[ci.Name] = al;
                }
                al.Add(new WeakSoodaObject(o));
            }

            if (!UseWeakReferences)
                _strongReferences.Add(o);

            _objectList.Add(new WeakSoodaObject(o));

            if (_precommitQueue != null)
                _precommitQueue.Enqueue(o);
        }

        protected internal void RegisterDirtyObject(SoodaObject o)
        {
            // transactionLogger.Debug("RegisterDirtyObject({0})", o.GetObjectKeyString());
            _dirtyObjects.Add(o);
            for (ClassInfo ci = o.GetClassInfo(); ci != null; ci = ci.InheritsFromClass)
            {
                List<WeakSoodaObject> al;
                if (!_dirtyObjectsByClass.TryGetValue(ci.Name, out al))
                {
                    al = new List<WeakSoodaObject>();
                    _dirtyObjectsByClass[ci.Name] = al;
                }
                al.Add(new WeakSoodaObject(o));
            }
        }

        protected internal bool IsRegistered(SoodaObject o)
        {
            var pkValue = o.GetPrimaryKeyValue();

            return ExistsObjectWithKey(o.GetClassInfo().Name, pkValue);
        }

        private static void RemoveWeakSoodaObjectFromCollection(List<WeakSoodaObject> collection, SoodaObject o)
        {
            for (int i = 0; i < collection.Count; ++i)
            {
                if (collection[i].TargetSoodaObject == o)
                {
                    collection.RemoveAt(i);
                    break;
                }
            }
        }

        protected internal void UnregisterObject(SoodaObject o)
        {
            object pkValue = o.GetPrimaryKeyValue();

            if (ExistsObjectWithKey(o.GetClassInfo().Name, pkValue))
            {
                UnregisterObjectWithKey(o.GetClassInfo().Name, pkValue);
                for (ClassInfo ci = o.GetClassInfo().InheritsFromClass; ci != null; ci = ci.InheritsFromClass)
                {
                    UnregisterObjectWithKey(ci.Name, pkValue);
                }
                RemoveWeakSoodaObjectFromCollection(_objectList, o);

                List<WeakSoodaObject> al;
                if (_objectsByClass.TryGetValue(o.GetClassInfo().Name, out al))
                {
                    RemoveWeakSoodaObjectFromCollection(al, o);
                }
            }
        }

        public object FindObjectWithKey(string className, object keyValue, Type expectedType)
        {
            if (keyValue == null) keyValue = "";
            object o = FindObjectWithKey(className, keyValue);
            if (o == null)
                return null;

            return expectedType.IsAssignableFrom(o.GetType()) ? o : null;
            // Console.WriteLine("FAILING TryGet for {0}:{1} because it's of type {2} instead of {3}", className, keyValue, o.GetType(), expectedType);
        }

        public object FindObjectWithKey(string className, int keyValue, Type expectedType)
        {
            return FindObjectWithKey(className, (object) keyValue, expectedType);
        }

        public object FindObjectWithKey(string className, long keyValue, Type expectedType)
        {
            return FindObjectWithKey(className, (object) keyValue, expectedType);
        }

        public object FindObjectWithKey(string className, string keyValue, Type expectedType)
        {
            return FindObjectWithKey(className, (object) keyValue, expectedType);
        }

        public object FindObjectWithKey(string className, Guid keyValue, Type expectedType)
        {
            return FindObjectWithKey(className, (object) keyValue, expectedType);
        }

        public void RegisterDataSource(SoodaDataSource dataSource)
        {
            dataSource.Statistics = Statistics;
            dataSource.IsolationLevel = IsolationLevel;
            DataSources.Add(dataSource);
        }

        public SoodaDataSource OpenDataSource(string name, IDbConnection connection)
        {
            return OpenDataSource(Schema.GetDataSourceInfo(name), connection);
        }

        public SoodaDataSource OpenDataSource(string name)
        {
            return OpenDataSource(name, null);
        }

        public SoodaDataSource OpenDataSource(DataSourceInfo dataSourceInfo, IDbConnection connection)
        {
            foreach (SoodaDataSource dataSource in DataSources)
            {
                if (dataSource.Name == dataSourceInfo.Name)
                    return dataSource;
            }

            var ds = dataSourceInfo.CreateDataSource();
            DataSources.Add(ds);
            ds.Statistics = Statistics;
            ds.IsolationLevel = IsolationLevel;
            if (connection != null)
                ds.Connection = connection;
            else
                ds.Open();
            if (SavingObjects)
                ds.BeginSaveChanges();
            return ds;
        }

        public SoodaDataSource OpenDataSource(DataSourceInfo dataSourceInfo)
        {
            return OpenDataSource(dataSourceInfo, null);
        }

        public IsolationLevel IsolationLevel
        {
            get { return _isolationLevel; }
            set
            {
                _isolationLevel = value;
                foreach (SoodaDataSource sds in DataSources)
                {
                    sds.IsolationLevel = value;
                }
            }
        }

        private void CallBeforeCommitEvents()
        {
            foreach (SoodaObject o in _dirtyObjects)
            {
                _precommitQueue.Enqueue(o);
            }

            while (_precommitQueue.Count > 0)
            {
                var o = (SoodaObject) _precommitQueue.Dequeue();

                if (!o.IsMarkedForDelete() && o.IsObjectDirty())
                {
                    o.CallBeforeCommitEvent();
                }
            }

            _precommitQueue = null;
        }

        private void CallPostcommits()
        {
            for (int i = 0; i < _postCommitQueue.Count; ++i)
            {
                _postCommitQueue[i].PostCommit();
            }
        }

        public void SaveObjectChanges()
        {
            SaveObjectChanges(true, null);
        }

        internal void MarkPrecommitted(SoodaObject o)
        {
            _precommittedClassOrRelation[o.GetClassInfo().GetRootClass().Name] = true;
        }

        internal void PrecommitObject(SoodaObject o)
        {
            if (!o.VisitedOnCommit && !o.IsMarkedForDelete())
            {
                MarkPrecommitted(o); //_precommittedClassOrRelation[o.GetClassInfo().GetRootClass().Name] = true;
                o.SaveObjectChanges();
            }
        }

        internal void PrecommitRelation(RelationInfo ri)
        {
            _precommittedClassOrRelation[ri.Name] = true;
        }

        internal void SaveObjectChanges(bool isPrecommit, List<SoodaObject> objectsToPrecommit)
        {
            try
            {
                if (objectsToPrecommit == null)
                    objectsToPrecommit = _dirtyObjects;

                _isPrecommit = isPrecommit;
                foreach (SoodaDataSource source in DataSources)
                {
                    if (source.IsOpen) //+wash
                        source.BeginSaveChanges();
                }

                SavingObjects = true;

                foreach (SoodaObject o in objectsToPrecommit)
                {
                    o.VisitedOnCommit = false;
                }

                foreach (SoodaObject o in objectsToPrecommit)
                {
                    PrecommitObject(o);
                }

                foreach (SoodaRelationTable rel in _relationTables.Values)
                {
                    rel.SaveTuples(this, isPrecommit);
                }

                foreach (SoodaDataSource source in DataSources)
                {
                    if (source.IsOpen) //+wash
                        source.FinishSaveChanges();
                }
            }
            finally
            {
                SavingObjects = false;
            }
        }

        internal void PrecommitClasses(IEnumerable<string> classes)
        {
            if (classes == null)
            {
                // don't know what to precommit - precommit everything
                SaveObjectChanges(true, null);
                return;
            }

            var objectsToPrecommit = new List<SoodaObject>();
            foreach (string className in classes)
            {
                List<WeakSoodaObject> dirtyObjects = GetDirtyObjectsByClassName(className);
                if (dirtyObjects != null)
                {
                    foreach (WeakSoodaObject wr in dirtyObjects)
                    {
                        SoodaObject o = wr.TargetSoodaObject;
                        if (o != null)
                            objectsToPrecommit.Add(o);
                    }
                }
            }
            SaveObjectChanges(true, objectsToPrecommit);
        }

        private void Reset()
        {
            _dirtyObjects.Clear();
            _dirtyObjectsByClass.Clear();
            _objectDictByClass.Clear();
            _objectList.Clear();
            _objectsByClass.Clear();
            _relationTables.Clear();
        }

        public virtual void Rollback()
        {
            Reset();

            // rollback all transactions on all data sources

            foreach (SoodaDataSource source in DataSources)
            {
                source.Rollback();
            }
        }

        public void CheckCommitConditions()
        {
            foreach (SoodaObject o in _dirtyObjects)
            {
                if (o.IsObjectDirty() || o.IsInsertMode())
                    o.CheckForNulls();
            }

            foreach (SoodaObject o in _dirtyObjects)
            {
                o.CheckAssertions();
            }
        }

        public virtual void Commit()
        {
            _precommitQueue = new Queue(_dirtyObjects.Count);
            CallBeforeCommitEvents();
            CheckCommitConditions();

            if (!IsDeserialized && (_dirtyObjects.Count > 0 || _deletedObjects.Count > 0) &&
                !string.IsNullOrWhiteSpace(AutoSerializeTransactionPath))
            {
                var file = Path.Combine(AutoSerializeTransactionPath, $"{DateTime.Now:yyyy-MM-dd HH-mm-ss} {Guid.NewGuid().ToString().Substring(0,5)}.xml");
                var sw = new XmlTextWriter(file, Encoding.UTF8);
                Serialize(sw, SoodaSerializeOptions.IncludeDebugInfo);
                sw.Flush();
                sw.Close();
            }

            SaveObjectChanges(false, _dirtyObjects);

            // commit all transactions on all data sources
            foreach (SoodaRelationTable rel in _relationTables.Values)
            {
                rel.Commit();
            }

            foreach (SoodaDataSource source in DataSources)
            {
                source.Commit();
            }

            using (Cache.Lock())
            {
                foreach (SoodaObject o in _dirtyObjects)
                {
                    o.InvalidateCacheAfterCommit();
                }
                foreach (SoodaRelationTable rel in _relationTables.Values)
                {
                    rel.InvalidateCacheAfterCommit(Cache);
                }
            }

            _precommittedClassOrRelation.Clear();

            CallPostcommits();

            foreach (SoodaObject o in _dirtyObjects)
            {
                o.ResetObjectDirty();
            }

            _dirtyObjects.Clear();
            _dirtyObjectsByClass.Clear();
        }

        private SoodaObject GetObject(ISoodaObjectFactory factory, string keyString)
        {
            var keyValue = factory.GetPrimaryKeyFieldHandler().RawDeserialize(keyString) ?? string.Empty;
            return factory.GetRef(this, keyValue);
        }

        public SoodaObject GetObject(string className, string keyString)
        {
            return GetObject(GetFactory(className), keyString);
        }

        public SoodaObject GetObject(Type type, string keyString)
        {
            return GetObject(GetFactory(type), keyString);
        }

        public SoodaObject GetObject(Type type, object keyValue)
        {
            return GetFactory(type).GetRef(this, keyValue);
        }

        private SoodaObject LoadObject(ISoodaObjectFactory factory, string keyString)
        {
            var keyValue = factory.GetPrimaryKeyFieldHandler().RawDeserialize(keyString) ?? string.Empty;
            var obj = factory.GetRef(this, keyValue);
            obj.LoadAllData();
            return obj;
        }

        public SoodaObject LoadObject(Type type, object keyValue)
        {
            SoodaObject obj = GetFactory(type).GetRef(this, keyValue);
            obj.LoadAllData();
            return obj;
        }


        public SoodaObject LoadObject(string className, string keyString)
        {
            return LoadObject(GetFactory(className), keyString);
        }

        public SoodaObject LoadObject(Type type, string keyString)
        {
            return LoadObject(GetFactory(type), keyString);
        }

        private SoodaObject GetNewObject(ISoodaObjectFactory factory)
        {
            return factory.CreateNew(this);
        }

        public SoodaObject GetNewObject(string className)
        {
            return GetNewObject(GetFactory(className));
        }

        public SoodaObject GetNewObject(Type type)
        {
            return GetNewObject(GetFactory(type));
        }

        internal SoodaRelationTable GetRelationTable(Type relationType)
        {
            SoodaRelationTable table;
            if (_relationTables.TryGetValue(relationType, out table))
                return table;

            table = (SoodaRelationTable) Activator.CreateInstance(relationType);
            _relationTables[relationType] = table;
            return table;
        }

        public Assembly ObjectsAssembly
        {
            get { return _assembly; }
            set
            {
                _assembly = value;

                if (value != null)
                {
                    if (!_assembly.IsDefined(typeof (SoodaObjectsAssemblyAttribute), false))
                    {
                        var sa =
                            (SoodaStubAssemblyAttribute)
                                Attribute.GetCustomAttribute(_assembly, typeof (SoodaStubAssemblyAttribute), false);
                        if (sa != null)
                            _assembly = sa.Assembly;
                    }

                    var soa =
                        (SoodaObjectsAssemblyAttribute)
                            Attribute.GetCustomAttribute(_assembly, typeof (SoodaObjectsAssemblyAttribute), false);
                    if (soa == null)
                    {
                        throw new ArgumentException("Invalid objects assembly: " + _assembly.FullName +
                                                    ". Must be the stubs assembly and define assembly:SoodaObjectsAssemblyAttribute");
                    }

                    var schema = Activator.CreateInstance(soa.DatabaseSchemaType) as ISoodaSchema;
                    if (schema == null)
                        throw new ArgumentException("Invalid objects assembly: " + _assembly.FullName +
                                                    ". Must define a class implementing ISoodaSchema interface.");

                    foreach (ISoodaObjectFactory fact in schema.GetFactories())
                    {
                        _factoryForClassName[fact.GetClassInfo().Name] = fact;
                        _factoryForType[fact.TheType] = fact;
                    }
                    _schema = schema.Schema;
#if DOTNET35
                    DynamicFieldManager.OpenTransaction(this);
#endif
                }
            }
        }

        public ISoodaObjectFactory GetFactory(string className)
        {
            return GetFactory(className, true);
        }

        public ISoodaObjectFactory GetFactory(string className, bool throwOnError)
        {
            ISoodaObjectFactory factory;
            if (!_factoryForClassName.TryGetValue(className, out factory) && throwOnError)

                throw new SoodaException("Class " + className + " not registered for Sooda");
            return factory;
        }

        public ISoodaObjectFactory GetFactory(Type type)
        {
            return GetFactory(type, true);
        }

        public ISoodaObjectFactory GetFactory(Type type, bool throwOnError)
        {
            ISoodaObjectFactory factory;
            if (!_factoryForType.TryGetValue(type, out factory) && throwOnError)
                throw new SoodaException("Class " + type.Name + " not registered for Sooda");
            return factory;
        }

        public ISoodaObjectFactory GetFactory(ClassInfo classInfo)
        {
            return GetFactory(classInfo, true);
        }

        public ISoodaObjectFactory GetFactory(ClassInfo classInfo, bool throwOnError)
        {
            return GetFactory(classInfo.Name, throwOnError);
            //if (throwOnError && !factoryForClassName.Contains(classInfo.Name))
            //    throw new SoodaException("Class " + classInfo.Name + " not registered for Sooda");
            //return factoryForClassName[classInfo.Name];
        }

        internal void AddToPostCommitQueue(SoodaObject o)
        {
            if (TransactionLogger.IsTraceEnabled)
                TransactionLogger.Trace("Adding {0} to post-commit queue", o.GetObjectKeyString());
            _postCommitQueue.Add(o);
        }

        public string Serialize()
        {
            var sw = new StringWriter();
            Serialize(sw, SoodaSerializeOptions.DirtyOnly);
            return sw.ToString();
        }

        public string Serialize(SoodaSerializeOptions opt)
        {
            var sw = new StringWriter();
            Serialize(sw, opt);
            return sw.ToString();
        }

        public void Serialize(TextWriter tw, SoodaSerializeOptions options)
        {
            var xtw = new XmlTextWriter(tw) {Formatting = Formatting.Indented};

            Serialize(xtw, options);
        }

        private static int Compare(SoodaObject o1, SoodaObject o2)
        {
            int retval = string.CompareOrdinal(o1.GetClassInfo().Name, o2.GetClassInfo().Name);
            if (retval != 0)
                return retval;

            return ((IComparable) o1.GetPrimaryKeyValue()).CompareTo(o2.GetPrimaryKeyValue());
        }

        public void Serialize(XmlWriter xw, SoodaSerializeOptions options)
        {
            xw.WriteStartElement("transaction");

            var orderedObjects = new List<SoodaObject>();
            foreach (WeakSoodaObject wr in _objectList)
            {
                SoodaObject obj = wr.TargetSoodaObject;
                if (obj != null)
                    orderedObjects.Add(obj);
            }

            if ((options & SoodaSerializeOptions.Canonical) != 0)
            {
                orderedObjects.Sort(Compare);
            }

            foreach (SoodaObject o in DeletedObjects)
            {
                o.PreSerialize(xw, options);
            }
            foreach (SoodaObject o in orderedObjects)
            {
                if (!o.IsMarkedForDelete())
                {
                    if (o.IsObjectDirty() || (options & SoodaSerializeOptions.IncludeNonDirtyObjects) != 0)
                        o.PreSerialize(xw, options);
                }
            }
            foreach (SoodaObject o in orderedObjects)
            {
                if (o.IsObjectDirty() || (options & SoodaSerializeOptions.IncludeNonDirtyObjects) != 0)
                    o.Serialize(xw, options);
            }
            // serialize N-N relation tables
            foreach (SoodaRelationTable rel in _relationTables.Values)
            {
                rel.Serialize(xw, options);
            }
            xw.WriteEndElement();
        }

        public void Deserialize(string s)
        {
            var sr = new StringReader(s);
            var reader = new XmlTextReader(sr)
            {
                WhitespaceHandling = WhitespaceHandling.Significant
            };

            Deserialize(reader);
        }

        public void Deserialize(XmlReader reader)
        {
            Reset();

            SoodaObject currentObject = null;
            SoodaRelationTable currentRelation = null;
            bool inDebug = false;

            // state data for just-being-read object

            bool objectForcePostCommit = false;
            bool objectDisableObjectTriggers = false;
            bool objectDelete = false;
            string objectMode = null;
            object[] objectPrimaryKey = null;
            ClassInfo objectClassInfo;
            ISoodaObjectFactory objectFactory = null;
            int objectKeyCounter = 0;
            int objectTotalKeyCounter = 0;

            try
            {
                SavingObjects = true;

                // in case we get any "deleteobject" which require us to delete the objects
                // within transaction
                foreach (SoodaDataSource source in DataSources)
                {
                    source.BeginSaveChanges();
                }

                while (reader.Read())
                {
                    if (reader.NodeType == XmlNodeType.Element && !inDebug)
                    {
                        switch (reader.Name)
                        {
                            case "field":
                                if (currentObject == null)
                                    throw new Exception("Field without an object during deserialization!");

                                currentObject.DeserializeField(reader);
                                break;

                            case "persistent":
                                if (currentObject == null)
                                    throw new Exception("Field without an object during deserialization!");

                                currentObject.DeserializePersistentField(reader);
                                break;

                            case "object":
                                if (currentObject != null)
                                {
                                    // end deserialization

                                    currentObject.EnableFieldUpdateTriggers();
                                    currentObject = null;
                                }

                                objectKeyCounter = 0;
                                objectForcePostCommit = false;
                                objectDisableObjectTriggers = false;
                                var objectClassName = reader.GetAttribute("class");
                                objectMode = reader.GetAttribute("mode");
                                objectDelete = false;
                                objectFactory = GetFactory(objectClassName);
                                objectClassInfo = objectFactory.GetClassInfo();
                                objectTotalKeyCounter = objectClassInfo.GetPrimaryKeyFields().Length;
                                if (objectTotalKeyCounter > 1)
                                    objectPrimaryKey = new object[objectTotalKeyCounter];
                                if (reader.GetAttribute("forcepostcommit") != null)
                                    objectForcePostCommit = true;
                                if (reader.GetAttribute("disableobjecttriggers") != null)
                                    objectDisableObjectTriggers = true;
                                if (reader.GetAttribute("delete") != null)
                                    objectDelete = true;

                                break;

                            case "key":
                                int ordinal = Convert.ToInt32(reader.GetAttribute("ordinal"));
                                if (objectFactory != null)
                                {
                                    var val =
                                        objectFactory.GetFieldHandler(ordinal)
                                            .RawDeserialize(reader.GetAttribute("value"));

                                    if (objectTotalKeyCounter > 1)
                                    {
                                        if (objectPrimaryKey != null) objectPrimaryKey[objectKeyCounter] = val;
                                    }

                                    objectKeyCounter++;

                                    if (objectKeyCounter == objectTotalKeyCounter)
                                    {
                                        var primaryKey = objectTotalKeyCounter == 1
                                            ? val
                                            : new SoodaTuple(objectPrimaryKey);

                                        currentObject = BeginObjectDeserialization(objectFactory, primaryKey, objectMode);
                                        if (objectForcePostCommit)
                                            currentObject.ForcePostCommit();
                                        if (objectDisableObjectTriggers)
                                            currentObject.DisableObjectTriggers();
                                        currentObject.DisableFieldUpdateTriggers();
                                        if (objectDelete)
                                        {
                                            DeletedObjects.Add(currentObject);
                                            currentObject.DeleteMarker = true;
                                            currentObject.CommitObjectChanges();
                                            currentObject.SetObjectDirty();
                                        }
                                    }
                                }
                                break;

                            case "transaction":
                                break;

                            case "relation":
                                currentRelation = GetRelationFromXml(reader);
                                break;

                            case "tuple":
                                if (currentRelation != null) currentRelation.DeserializeTuple(reader);
                                break;

                            case "debug":
                                if (!reader.IsEmptyElement)
                                {
                                    inDebug = true;
                                }
                                break;

                            default:
                                throw new NotImplementedException("Element not implemented in deserialization: " +
                                                                  reader.Name);
                        }
                    }
                    else if (reader.NodeType == XmlNodeType.EndElement)
                    {
                        if (reader.Name == "debug")
                        {
                            inDebug = false;
                        }
                        else if (reader.Name == "object")
                        {
                            if (currentObject != null) currentObject.EnableFieldUpdateTriggers();
                        }
                    }
                }

                foreach (WeakSoodaObject wr in _objectList)
                {
                    SoodaObject ob = wr.TargetSoodaObject;
                    if (ob != null)
                    {
                        ob.AfterDeserialize();
                    }
                }
            }
            finally
            {
                IsDeserialized = true;
                SavingObjects = false;

                foreach (SoodaDataSource source in DataSources)
                {
                    source.FinishSaveChanges();
                }
            }
        }

        private SoodaRelationTable GetRelationFromXml(XmlReader reader)
        {
            string className = reader.GetAttribute("type");
            Type t = Type.GetType(className, true, false);
            ConstructorInfo ci = t.GetConstructor(Type.EmptyTypes);

            var retVal = (SoodaRelationTable) ci.Invoke(null);
            _relationTables[t] = retVal;
            retVal.BeginDeserialization(Int32.Parse(reader.GetAttribute("tupleCount")));
            return retVal;
        }

        private SoodaObject BeginObjectDeserialization(ISoodaObjectFactory factory, object pkValue, string mode)
        {
            SoodaObject retVal = factory.TryGet(this, pkValue);

            if (retVal == null)
            {
                if (mode == "update")
                {
                    TransactionLogger.Debug("Object not found. GetRef() ing");
                    retVal = factory.GetRef(this, pkValue);
                }
                else
                {
                    TransactionLogger.Debug("Object not found. Getting new raw object.");
                    retVal = factory.GetRawObject(this);
                    Statistics.RegisterObjectUpdate();
                    SoodaStatistics.Global.RegisterObjectUpdate();
                    retVal.SetPrimaryKeyValue(pkValue);
                    retVal.SetInsertMode();
                }
            }
            else if (mode == "insert")
            {
                retVal.SetInsertMode();
            }
            return retVal;
        }

        public SchemaInfo Schema
        {
            get { return _schema; }
        }

        public static ISoodaObjectFactoryCache SoodaObjectFactoryCache = new SoodaObjectFactoryCache();
        public static string AutoSerializeTransactionPath = null;
        private bool IsDeserialized;

        internal NameValueCollection GetPersistentValues(SoodaObject obj)
        {
            NameValueCollection dict;
            _persistentValues.TryGetValue(obj, out dict);
            return dict;
        }

        internal string GetPersistentValue(SoodaObject obj, string name)
        {
            NameValueCollection dict;
            if (!_persistentValues.TryGetValue(obj, out dict))
                return null;
            return dict[name];
        }

        internal void SetPersistentValue(SoodaObject obj, string name, string value)
        {
            NameValueCollection dict;
            if (!_persistentValues.TryGetValue(obj, out dict))
            {
                dict = new NameValueCollection();
                _persistentValues.Add(obj, dict);
            }
            dict[name] = value;
        }

        internal bool IsPrecommit
        {
            get { return _isPrecommit; }
        }

        public SoodaStatistics Statistics
        {
            get { return _statistics; }
        }

        internal bool HasBeenPrecommitted(ClassInfo ci)
        {
            return _precommittedClassOrRelation.Contains(ci.GetRootClass().Name);
        }

        internal bool HasBeenPrecommitted(RelationInfo ri)
        {
            return _precommittedClassOrRelation.Contains(ri.Name);
        }

        public List<SoodaObject> DeletedObjects
        {
            get { return _deletedObjects; }
        }

        public ISoodaCachingPolicy CachingPolicy { get; set; }

        public ISoodaCache Cache { get; set; }

        public bool IsKeyGeneratorDisabled(string className)
        {
            return _disabledKeyGenerators.Contains(className);
        }

        private class RevertDisableKeyGenerators : IDisposable
        {
            private SoodaTransaction _trans;
            private StringCollection _classNames;

            internal RevertDisableKeyGenerators(SoodaTransaction trans, StringCollection classNames)
            {
                _trans = trans;
                _classNames = classNames;
            }

            public void Dispose()
            {
                foreach (string className in _classNames)
                    _trans._disabledKeyGenerators.Remove(className);
            }
        }

        public IDisposable DisableKeyGenerators(params string[] classNames)
        {
            StringCollection disabled = new StringCollection();
            foreach (string className in classNames)
            {
                if (_disabledKeyGenerators.Contains(className))
                    continue;
                _disabledKeyGenerators.Add(className);
                disabled.Add(className);
            }
            return new RevertDisableKeyGenerators(this, disabled);
        }


        internal IEnumerable LoadCollectionFromCache(string cacheKey, Logger logger)
        {
            IEnumerable keysCollection = Cache.LoadCollection(cacheKey);
            if (keysCollection != null)
            {
                SoodaStatistics.Global.RegisterCollectionCacheHit();
                Statistics.RegisterCollectionCacheHit();
            }
            else if (cacheKey != null)
            {
                logger.Debug("Cache miss. {0} not found in cache.", cacheKey);
                SoodaStatistics.Global.RegisterCollectionCacheMiss();
                Statistics.RegisterCollectionCacheMiss();
            }
            return keysCollection;
        }

        internal void StoreCollectionInCache(string cacheKey, ClassInfo classInfo, IList list, string[] dependentClasses,
            bool evictWhenItemRemoved, TimeSpan expirationTimeout, bool slidingExpiration)
        {
            object[] keys = new object[list.Count];
            for (int i = 0; i < list.Count; ++i)
            {
                keys[i] = ((SoodaObject) list[i]).GetPrimaryKeyValue();
            }

            Cache.StoreCollection(cacheKey, classInfo.GetRootClass().Name, keys, dependentClasses,
                evictWhenItemRemoved, expirationTimeout, slidingExpiration);
        }
    }
}