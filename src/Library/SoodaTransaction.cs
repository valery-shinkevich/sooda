// 
// Copyright (c) 2002-2004 Jaroslaw Kowalski <jaak@polbox.com>
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
// * Neither the name of the Jaroslaw Kowalski nor the names of its 
//   contributors may be used to endorse or promote products derived from this
//   software without specific prior written permission. 
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

using System;
using System.Diagnostics;
using System.Data;
using System.Reflection;
using System.Collections;
using System.Xml;
using System.IO;
using System.Runtime.InteropServices;

using Sooda.Schema;

using Sooda.ObjectMapper;

namespace Sooda
{
	[Flags]
	public enum TransactionOptions
	{
		NoImplicit = 0x0000,
		Implicit   = 0x0001,

		NoInit     = 0x0002,
		WithInit   = 0x0000,
	}

	public class SoodaTransaction : IDisposable
	{
		private static NLog.Logger transactionLogger = NLog.LogManager.GetLogger("Sooda.Transaction");
		private static LocalDataStoreSlot g_activeTransactionDataStoreSlot = System.Threading.Thread.AllocateDataSlot();

		private TransactionOptions transactionOptions;
		private TypeToSoodaRelationTableDictionary _relationTables = new TypeToSoodaRelationTableDictionary();
		//private KeyToSoodaObjectMap _objects = new KeyToSoodaObjectMap();
		private SoodaObjectCollection _objectList = new SoodaObjectCollection ();
		private Queue _precommitQueue = null;
		private Queue _deleteQueue = null;
		private SoodaObjectCollection _postCommitQueue = null;
		private StringToSoodaObjectCollectionDictionary _objectsByClass = new StringToSoodaObjectCollectionDictionary();
        private StringToSoodaObjectDictionaryDictionary _objectDictByClass = new StringToSoodaObjectDictionaryDictionary();

		private DataSourceCollection _dataSources = new DataSourceCollection(10);
		private StringToSoodaObjectFactoryMap factoryForClassName = new StringToSoodaObjectFactoryMap();
        private TypeToSoodaObjectFactoryMap factoryForType = new TypeToSoodaObjectFactoryMap();
        private Assembly _assembly;

		public static Assembly DefaultObjectsAssembly = null;

		#region Constructors, Dispose & Finalizer

		public SoodaTransaction() : this(null, TransactionOptions.Implicit | TransactionOptions.WithInit, Assembly.GetCallingAssembly())
		{
		}

		public SoodaTransaction(Assembly objectsAssembly) : this(objectsAssembly, TransactionOptions.Implicit | TransactionOptions.WithInit, Assembly.GetCallingAssembly())
		{
		}

		public SoodaTransaction(TransactionOptions options) : this(null, options, Assembly.GetCallingAssembly())
		{
		}

		public SoodaTransaction(Assembly objectsAssembly, TransactionOptions options) : this(objectsAssembly, options, Assembly.GetCallingAssembly())
		{
		}

		private SoodaTransaction(Assembly objectsAssembly, TransactionOptions options, Assembly callingAssembly)
        {
            if (objectsAssembly != null)
                ObjectsAssembly = objectsAssembly;

            if (ObjectsAssembly == null)
            {
                SoodaStubAssemblyAttribute[] attrs = (SoodaStubAssemblyAttribute[])callingAssembly.GetCustomAttributes(typeof(SoodaStubAssemblyAttribute), false);
                if (attrs != null && attrs.Length == 1)
                {
                    ObjectsAssembly = attrs[0].Assembly;
                }
            }

            if (ObjectsAssembly == null)
                ObjectsAssembly = DefaultObjectsAssembly;

            this.transactionOptions = options;
            if ((options & TransactionOptions.Implicit) != 0)
            {
                if (null != System.Threading.Thread.GetData(g_activeTransactionDataStoreSlot))
                {
                    transactionLogger.Warn("Nested transaction detected! Overriding active transaction.");
                };
                System.Threading.Thread.SetData(g_activeTransactionDataStoreSlot, this);
            }
            if ((options & TransactionOptions.NoInit) == 0)
            {
                InitTransaction();
            }
        }

		~SoodaTransaction()
		{
			Dispose(false);
		}

		void IDisposable.Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		void Dispose(bool disposing)
		{
			// Log("Disposing transaction", "Materialize");
			if (disposing)
			{
				foreach (SoodaDataSource source in _dataSources)
				{
					source.Close();
				}
			};
			if ((transactionOptions & TransactionOptions.Implicit) != 0)
			{
				if (System.Threading.Thread.GetData(g_activeTransactionDataStoreSlot) == this)
				{
					System.Threading.Thread.SetData(g_activeTransactionDataStoreSlot, null);
				}
				else
				{
					transactionLogger.Warn("ActiveTransactionDataStoreSlot has been overwritten by someone.");
				};
			}
		}

		#endregion

		public static SoodaTransaction ActiveTransaction
		{
            [DebuggerStepThrough]
            get
			{
				SoodaTransaction retVal = (SoodaTransaction)System.Threading.Thread.GetData(g_activeTransactionDataStoreSlot);

				if (retVal == null)
				{
					throw new InvalidOperationException("There's no implicit transaction currently active. Either use explicit transactions or create a new implicit one.");
				}

				return retVal;
			}
		}

		protected void InitTransaction()
		{
		}

		public SoodaObjectCollection GetObjects()
		{
			return _objectList;
		}

		public SoodaObjectCollection GetObjectsByClassName(string className)
		{
			return _objectsByClass[className];
		}

		public SoodaObjectCollection DirtyObjects
		{
			get
			{
				SoodaObjectCollection dirty = new SoodaObjectCollection();
				foreach (SoodaObject o in _objectList)
				{
					if (o.IsObjectDirty())
						dirty.Add(o);
				}
				return dirty;
			}
		}

        private ObjectToSoodaObjectDictionary GetObjectDictionaryForClass(string className)
        {
            ObjectToSoodaObjectDictionary dict = _objectDictByClass[className];
            if (dict == null)
            {
                dict = new ObjectToSoodaObjectDictionary();
                _objectDictByClass[className] = dict;
            }
            return dict;
        }

        private void AddObjectWithKey(string className, object keyValue, SoodaObject obj)
        {
            // Console.WriteLine("AddObjectWithKey('{0}',{1})", className, keyValue);
            GetObjectDictionaryForClass(className).Add(keyValue, obj);
        }

        private void UnregisterObjectWithKey(string className, object keyValue)
        {
            GetObjectDictionaryForClass(className).Remove(keyValue);
        }

        internal bool ExistsObjectWithKey(string className, object keyValue)
        {
            return GetObjectDictionaryForClass(className).Contains(keyValue);
        }

        private SoodaObject FindObjectWithKey(string className, object keyValue)
        {
            return GetObjectDictionaryForClass(className)[keyValue];
        }

        protected internal void RegisterObject(SoodaObject o)
		{
			// Console.WriteLine("Registering object {0}...", o.GetObjectKey());

            object pkValue = o.GetPrimaryKeyValue();
            AddObjectWithKey(o.GetClassInfo().Name, pkValue, o);
            // Console.WriteLine("Adding key: " + o.GetObjectKey() + " of type " + o.GetType());
            for (ClassInfo ci = o.GetClassInfo().InheritsFromClass; ci != null; ci = ci.InheritsFromClass)
            {
                AddObjectWithKey(ci.Name, pkValue, o);
            }

			_objectList.Add(o);

			if (_precommitQueue != null)
				_precommitQueue.Enqueue(o);

			SoodaObjectCollection al = _objectsByClass[o.GetClassInfo().Name];
			if (al == null)
			{
				al = new SoodaObjectCollection();
				_objectsByClass[o.GetClassInfo().Name] = al;
			}

			al.Add(o);
		}

		protected internal void UnregisterObject(SoodaObject o)
		{
            ObjectToSoodaObjectDictionary classDict = _objectDictByClass[o.GetClassInfo().Name];
            object pkValue = o.GetPrimaryKeyValue();

			if (ExistsObjectWithKey(o.GetClassInfo().Name, pkValue))
			{
                UnregisterObjectWithKey(o.GetClassInfo().Name, pkValue);
                for (ClassInfo ci = o.GetClassInfo().InheritsFromClass; ci != null; ci = ci.InheritsFromClass)
                {
                    UnregisterObjectWithKey(ci.Name, pkValue);
                }
                _objectList.Remove(o);

				SoodaObjectCollection al = _objectsByClass[o.GetClassInfo().Name];
				if (al != null)
				{
					al.Remove(o);
				}
			}
		}

		public object FindObjectWithKey(string className, object keyValue, Type expectedType)
		{
			object o = FindObjectWithKey(className, keyValue);
            if (o == null)
                return null;

            if (expectedType.IsAssignableFrom(o.GetType()))
                return o;
            else
            {
                // Console.WriteLine("FAILING TryGet for {0}:{1} because it's of type {2} instead of {3}", className, keyValue, o.GetType(), expectedType);
                return null;
            }
		}

        public void RegisterDataSource(SoodaDataSource dataSource)
        {
            _dataSources.Add(dataSource);
        }

		public SoodaDataSource OpenDataSource(Sooda.Schema.DataSourceInfo dataSourceInfo)
		{
			if (_dataSources.Count == 0)
			{
				SoodaDataSource ds = (SoodaDataSource)dataSourceInfo.CreateDataSource();
				_dataSources.Add(ds);
				ds.Open();
			}

			return _dataSources[0] as SoodaDataSource;
		}

		void CallPrecommits()
		{
			foreach (SoodaObject o in _objectList)
			{
				if (o.IsObjectDirty())
				{
					_precommitQueue.Enqueue(o);
				};
			};

			while (_precommitQueue.Count > 0)
			{
				SoodaObject o = (SoodaObject)_precommitQueue.Dequeue();
				if (o.IsObjectDirty())
				{
					o.PreCommit();
				}
			}

			_precommitQueue = null;
		}

		void CallPostcommits()
		{
			foreach (SoodaObject o in _postCommitQueue)
			{
				o.PostCommit();
			}
		}

        protected static internal SoodaObject SaveOuterReferences(SoodaObject theObject, string fieldName, object fieldValue, bool isDirty, SoodaObject refcache, ISoodaObjectFactory factory, object context)
        {
            if (fieldValue != null)
            {
                SoodaObject obj = RefCache.TryGetObject(refcache, fieldValue, theObject.GetTransaction(), factory);
                refcache = obj;
                if (obj != null && (object)obj != (object)theObject)
                {
                    if (obj.IsInsertMode())
                    {
                        if (obj.VisitedOnCommit && !obj.WrittenIntoDatabase)
                        {
                            throw new Exception("Cyclic reference between " + theObject.GetObjectKeyString() + " and " + obj.GetObjectKeyString());
                            // cyclic reference
                        }
                        else
                        {
                            SaveObjectChanges(obj);
                        }
                    }
                };
            };
            return refcache;
        }

        private static SoodaObjectRefFieldIterator saveOuterReferencesIterator = new SoodaObjectRefFieldIterator(SoodaTransaction.SaveOuterReferences);

        static void SaveObjectChanges(SoodaObject o)
        {
            o.VisitedOnCommit = true;
            o.IterateOuterReferences(saveOuterReferencesIterator, null);
            if (o.WrittenIntoDatabase)
                return;

            if ((o.IsObjectDirty() || o.IsInsertMode()) && !o.WrittenIntoDatabase)
            {
                o.CommitObjectChanges();
            }
            o.WrittenIntoDatabase = true;
        }

        internal void SaveObjectChanges()
        {
            foreach (SoodaObject o in _objectList)
            {
                o.VisitedOnCommit = false;
            }

            foreach (SoodaObject o in _objectList)
            {
                if (!o.VisitedOnCommit)
                {
                    SaveObjectChanges(o);
                }
            }
#warning TODO - restore support for deletion

			if (_relationTables != null)
			{
				foreach (SoodaRelationTable rel in _relationTables.Values)
				{
					rel.SaveTuples(this);
				}
			}
		}

        private void Reset()
        {
            _objectDictByClass.Clear();
            _objectList.Clear();
            _objectsByClass.Clear();
            _relationTables.Clear();
        }

		public void Rollback()
		{
            Reset();

			// rollback all transactions on all data sources

			foreach (SoodaDataSource source in _dataSources)
			{
				source.Rollback();
			}
		}

        private void CheckCommitConditions()
        {
            foreach (SoodaObject o in _objectList)
            {
                if (o.IsObjectDirty() || o.IsInsertMode())
                    o.CheckForNulls();
            }

            foreach (SoodaObject o in _objectList)
            {
                o.CheckAssertions();
            }
        }

		public void Commit()
		{
			_precommitQueue = new Queue(_objectList.Count);
			CallPrecommits();
            CheckCommitConditions();

			_postCommitQueue = new SoodaObjectCollection(_objectList.Count);
			SaveObjectChanges();
			CallPostcommits();

			// commit all transactions on all data sources

			foreach (SoodaDataSource source in _dataSources)
			{
				source.Commit();
                // source.Rollback();
			}

            if (_relationTables != null)
            {
                foreach (SoodaRelationTable rel in _relationTables.Values)
                {
                    rel.Commit();
                }
            }

            foreach (SoodaObject o in _objectList)
			{
				o.ResetObjectDirty();
			}

            foreach (SoodaObject o in _objectList)
			{
                if (o.CanBeCached() && o.IsAnyDataLoaded() && !o.FromCache)
                {
                    SoodaCache.AddObject(o.GetClassInfo().Name, o.GetPrimaryKeyValue(), o.GetCacheEntry());
                    o.FromCache = true;
                }
			}
        }

		private void _Reset()
		{
			_objectList.Clear();
			_objectsByClass.Clear();
			_precommitQueue.Clear();
			_relationTables.Clear();
		}

		private static object[] relationTableConstructorArguments = new object[0] { };

        private SoodaObject GetObject(ISoodaObjectFactory factory , string keyString)
        {
            object keyValue = factory.GetPrimaryKeyFieldHandler().RawDeserialize(keyString);
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
			SoodaRelationTable table = _relationTables[relationType];
			if (table != null)
				return table;

			table = (SoodaRelationTable)Activator.CreateInstance(relationType);
			_relationTables[relationType] = table;
			return table;
		}

		public Assembly ObjectsAssembly
		{
			get
			{
				return _assembly;
			}
			set
			{
				_assembly = value;

                if (_assembly != null)
                {
                    foreach (Type t in _assembly.GetExportedTypes())
                    {
                        SoodaObjectFactoryAttribute[] attr = (SoodaObjectFactoryAttribute[])t.GetCustomAttributes(typeof(SoodaObjectFactoryAttribute), false);
                        if (attr != null && attr.Length == 1)
                        {
                            ISoodaObjectFactory fact =  (ISoodaObjectFactory)t.GetProperty("TheFactory").GetValue(null, new object[0]);
                            factoryForClassName[attr[0].ClassName] = fact;
                            factoryForType[attr[0].Type] = fact;
                        }
                    }
                }
            }
		}

		public ISoodaObjectFactory GetFactory(string className)
		{
			return factoryForClassName[className];
		}

        public ISoodaObjectFactory GetFactory(Type type)
        {
            return factoryForType[type];
        }

        public ISoodaObjectFactory GetFactory(ClassInfo classInfo)
		{
			return factoryForClassName[classInfo.Name];
		}

		public void MaterializeExtraObjects(IDataRecord record, TableInfo[] tables)
		{
			if (tables == null)
				return;

#if A
            // TODO - fix me

			foreach (ExtraLoadedData ad in extraData)
			{
                ISoodaObjectFactory factory = GetFactory(ad.classInfo.Name);
                if (factory != null)
                {
                    SoodaObject newObj = factory.GetRefFromRecord(this, record, ad.startingColumn);
                }
                else
                {
                    throw new Exception("Factory not found: " + ad.classInfo.Name);
                }
            }
#endif
		}

		internal void AddToDeleteQueue(SoodaObject o)
		{
			if (_deleteQueue != null)
				_deleteQueue.Enqueue(o);
		}

		internal void AddToPostCommitQueue(SoodaObject o)
		{
			if (_postCommitQueue != null)
				_postCommitQueue.Add(o);
		}

		#region Serialization

		public string Serialize()
		{
			StringWriter sw = new StringWriter();
			Serialize(sw, SerializeOptions.DirtyOnly);
			return sw.ToString();
		}

		public string Serialize(SerializeOptions opt)
		{
			StringWriter sw = new StringWriter();
			Serialize(sw, opt);
			return sw.ToString();
		}

		public void Serialize(TextWriter tw, SerializeOptions options)
		{
			XmlTextWriter xtw = new XmlTextWriter(tw);

			xtw.Formatting = Formatting.Indented;
			Serialize(xtw, options);
		}

		internal class KeyStringComparer : IComparer
		{
			public int Compare(object x, object y)
			{
				SoodaObject o1 = (SoodaObject)x;
				SoodaObject o2 = (SoodaObject)y;

                int retval = String.CompareOrdinal(o1.GetClassInfo().Name, o2.GetClassInfo().Name);
                if (retval != 0)
                    return retval;

                retval = ((IComparable)o1.GetPrimaryKeyValue()).CompareTo(o2.GetPrimaryKeyValue());
                if (retval != 0)
                    return retval;

                return 0;
			}
		}

		public void Serialize(XmlWriter xw, SerializeOptions options)
		{
			xw.WriteStartElement("transaction");

			SoodaObjectCollection orderedObjects = _objectList;

			if ((options & SerializeOptions.Canonical) != 0)
			{
				ArrayList al = new ArrayList(orderedObjects);
				al.Sort(new KeyStringComparer());
				orderedObjects = new SoodaObjectCollection((SoodaObject[])al.ToArray(typeof(SoodaObject)));
			}

			foreach (SoodaObject o in orderedObjects)
			{
				if (o.IsObjectDirty() || ((options & SerializeOptions.IncludeNonDirtyObjects) != 0))
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
			StringReader sr = new StringReader(s);
			XmlTextReader reader = new XmlTextReader(sr);

			reader.WhitespaceHandling = WhitespaceHandling.Significant;
			Deserialize(reader);
		}

		public void Deserialize(XmlReader reader)
		{
			Reset();

			SoodaObject currentObject = null;
			SoodaRelationTable currentRelation = null;
			bool inDebug = false;

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

						case "object":
							if (currentObject != null)
							{
								// end deserialization

								currentObject.DisableTriggers = false;
							};

							string isDirty = reader.GetAttribute("isDirty");

							currentObject = GetSoodaObjectFromXml(reader);
							currentObject.DisableTriggers = true;
							break;

						case "transaction":
							break;

						case "relation":
							currentRelation = GetRelationFromXml(reader);
							break;

						case "tuple":
							currentRelation.DeserializeTuple(reader);
							break;

						case "debug":
							if (!reader.IsEmptyElement)
							{
								inDebug = true;
							}
							break;

						default:
							throw new NotImplementedException("Element not implemented in deserialization: " + reader.Name);
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
						currentObject.DisableTriggers = false;
					}
				}
			};

			foreach (SoodaObject ob in _objectList)
			{
				ob.AfterDeserialize();
			}
		}

		private SoodaRelationTable GetRelationFromXml(XmlReader reader)
		{
			string className = reader.GetAttribute("type");
			Type t = _assembly.GetType(className, true, false);
			ConstructorInfo ci = t.GetConstructor(new Type[] { });

			SoodaRelationTable retVal = (SoodaRelationTable)ci.Invoke(new object[] {});
			_relationTables[t] = retVal;
			retVal.BeginDeserialization(Int32.Parse(reader.GetAttribute("tupleCount")));
			return retVal;
		}

		private SoodaObject GetSoodaObjectFromXml(XmlReader reader)
		{
			string className = reader.GetAttribute("class");
			string mode = reader.GetAttribute("mode");

			ISoodaObjectFactory factory = GetFactory(className);
			SoodaObject retVal = null;
			object pkValue = null;

            SoodaFieldHandler handler = factory.GetPrimaryKeyFieldHandler();
            pkValue = handler.RawDeserialize(reader.GetAttribute("value"));
			transactionLogger.Debug("Deserializing object {0} {1}.", className, pkValue);

			retVal = factory.TryGet(this, pkValue);
			if (retVal == null)
			{
				if (mode == "update")
				{
			        transactionLogger.Debug("Object not found. GetRef() ing");
					retVal = factory.GetRef(this, pkValue);
				}
				else
				{
			        transactionLogger.Debug("Object not found. Getting new raw object.");
					retVal = factory.GetRawObject(this);
					retVal.SetPrimaryKeyValue(pkValue);
                    retVal.SetInsertMode();
				}
			}
			else
			{
				if (mode == "insert")
				{
					retVal.SetInsertMode();
				}
			}

			return retVal;
		}

        #endregion

        public static ISoodaObjectFactoryCache SoodaObjectFactoryCache = new SoodaObjectFactoryCache();
	}
}
