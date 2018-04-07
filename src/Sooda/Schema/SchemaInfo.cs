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

using System;

[assembly: CLSCompliant(true)]

namespace Sooda.Schema
{
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Xml.Serialization;
    using ObjectMapper.FieldHandlers;

    [XmlType(Namespace = "http://www.sooda.org/schemas/SoodaSchema.xsd")]
    [XmlRoot("schema", Namespace = "http://www.sooda.org/schemas/SoodaSchema.xsd", IsNullable = false)]
    [Serializable]
    public class SchemaInfo
    {
        public static readonly string XmlNamespace = "http://www.sooda.org/schemas/SoodaSchema.xsd";

        [XmlElement("namespace")] public string Namespace;

        [XmlElement("assembly")] public string AssemblyName;

        [XmlElement("include", typeof (IncludeInfo))] public List<IncludeInfo> Includes = new List<IncludeInfo>();

        [XmlElement("datasource", typeof (DataSourceInfo))] public List<DataSourceInfo> DataSources =
            new List<DataSourceInfo>();

        [XmlElement("class", typeof (ClassInfo))] public List<ClassInfo> Classes = new List<ClassInfo>();

        [XmlElement("enum", typeof (EnumInfo))] public EnumInfoCollection Enums = new EnumInfoCollection();

        [XmlElement("defaultPrecommitValues")] [System.ComponentModel.DefaultValue(true)] public bool
            DefaultPrecommitValues = true;

        [XmlElement("precommitValue", typeof (PrecommitValueInfo))] public List<PrecommitValueInfo> PrecommitValues =
            new List<PrecommitValueInfo>();

        [XmlIgnore] public List<ClassInfo> LocalClasses;

        [XmlIgnore] public List<RelationInfo> LocalRelations;

        [XmlAnyAttribute] [NonSerialized] public System.Xml.XmlAttribute[] Extensions;

        [XmlElement("relation", typeof (RelationInfo))] public List<RelationInfo> Relations = new List<RelationInfo>();

        [XmlIgnore] [NonSerialized] private Dictionary<string, List<ClassInfo>> _subclasses;

        [XmlIgnore] [NonSerialized] private Dictionary<string, StringCollection> _backRefCollections;

#if DOTNET35
        [XmlIgnore] [NonSerialized] internal ReaderWriterLock RwLock;
#endif

        public bool Contains(string className)
        {
            return FindClassByName(className) != null;
        }

        public ClassInfo FindClassByName(string className)
        {
            if (Classes == null)
                return null;

            ClassInfo ci;
            _classNameHash.TryGetValue(className, out ci);
            return ci;
        }

        public RelationInfo FindRelationByName(string relationName)
        {
            if (Relations == null)
                return null;

            RelationInfo rel;
            _relationNameHash.TryGetValue(relationName, out rel);
            return rel;
        }

        public IFieldContainer FindContainerByName(string name)
        {
            IFieldContainer result = FindClassByName(name);
            if (result != null)
                return result;

            result = FindRelationByName(name);
            if (result != null)
                return result;

            throw new Exception(string.Format("'{0}' is neither a class nor a relation", name));
        }

        public void AddClass(ClassInfo ci)
        {
            Classes.Add(ci);
            Rehash();
        }

        [NonSerialized] private Dictionary<string, ClassInfo> _classNameHash;
        [NonSerialized] private Dictionary<string, RelationInfo> _relationNameHash;

        public void Include(SchemaInfo schema)
        {
        }

        internal void Resolve(IEnumerable<ClassInfo> classes)
        {
            foreach (ClassInfo ci in classes)
            {
                ci.FlattenTables();
            }
            foreach (ClassInfo ci in classes)
            {
                ci.Resolve(this);
            }
            foreach (ClassInfo ci in classes)
            {
                ci.MergeTables();
            }
            foreach (ClassInfo ci in classes)
            {
                ci.ResolveReferences(this);
            }
            foreach (ClassInfo ci in classes)
            {
                ci.ResolveCollections(this);
            }
            foreach (ClassInfo ci in classes)
            {
                ci.ResolvePrecommitValues();
            }
        }

        public void Resolve()
        {
            if (Includes == null)
                Includes = new List<IncludeInfo>();

            _classNameHash = new Dictionary<string, ClassInfo>(StringComparer.OrdinalIgnoreCase);
                // Hashtable(StringComparer.OrdinalIgnoreCase);
            _relationNameHash = new Dictionary<string, RelationInfo>(StringComparer.OrdinalIgnoreCase);
                // Hashtable(StringComparer.OrdinalIgnoreCase);
            Rehash();

            _backRefCollections = new Dictionary<string, StringCollection>();

            foreach (ClassInfo ci in Classes)
            {
                ci.ResolveInheritance(this);
            }

            Resolve(Classes);

            if (Relations != null)
            {
                foreach (RelationInfo ri in Relations)
                {
                    ri.Resolve(this);
                }
            }

            foreach (DataSourceInfo dsi in DataSources)
            {
                dsi.Resolve();
            }

            LocalClasses = new List<ClassInfo>();
            foreach (ClassInfo ci in Classes)
            {
                if (ci.Schema == this)
                    LocalClasses.Add(ci);
            }
            LocalRelations = new List<RelationInfo>();
            if (Relations != null)
                foreach (RelationInfo ri in Relations)
                {
                    if (ri.Schema == this)
                        LocalRelations.Add(ri);
                }

            _subclasses = new Dictionary<string, List<ClassInfo>>();

            foreach (ClassInfo ci in Classes)
            {
                _subclasses[ci.Name] = new List<ClassInfo>();
            }

            foreach (ClassInfo ci0 in Classes)
            {
                for (ClassInfo ci = ci0.InheritsFromClass; ci != null; ci = ci.InheritsFromClass)
                {
                    (_subclasses[ci.Name]).Add(ci0);
                }
            }
        }

        internal List<ClassInfo> GetSubclasses(ClassInfo ci)
        {
            List<ClassInfo> subclasses;
            _subclasses.TryGetValue(ci.Name, out subclasses);
            return subclasses;
        }

        private void Rehash()
        {
            _classNameHash.Clear();
            _relationNameHash.Clear();
            if (Classes != null)
            {
                foreach (ClassInfo ci in Classes)
                {
                    if (ci.Name != null)
                        _classNameHash[ci.Name] = ci;
                }
            }
            if (Relations != null)
            {
                foreach (RelationInfo ri in Relations)
                {
                    _relationNameHash[ri.Name] = ri;
                }
            }
        }

        public DataSourceInfo GetDataSourceInfo(string name)
        {
            if (name == null)
                name = "default";

            foreach (DataSourceInfo dsi in DataSources)
            {
                if (dsi.Name == name)
                    return dsi;
            }
            throw new SoodaSchemaException("Data source " + name + " not found. Available data sources: " +
                                           GetAvailableDataSources());
        }

        private string GetAvailableDataSources()
        {
            var sb = new StringBuilder();

            foreach (var dsi in DataSources)
            {
                if (sb.Length > 0)
                    sb.Append(',');
                sb.Append(dsi.Name);
                sb.Append(": ");
                sb.Append(dsi.DataSourceType);
            }

            return sb.ToString();
        }

        internal void MergeIncludedSchema(SchemaInfo includedSchema)
        {
// merge classes, relations and datasources
            if (includedSchema.Classes != null)
            {
                var classNames = includedSchema.Classes.ToDictionary(nci => nci.Name);

                var newClasses = new List<ClassInfo>();
                if (Classes != null)
                {
                    foreach (ClassInfo ci in Classes)
                    {
                        newClasses.Add(ci);
                        ClassInfo ci2;
                        if (classNames.TryGetValue(ci.Name, out ci2))
                        {
                            ci.Merge(ci2);
                            classNames.Remove(ci.Name);
                        }
                    }
                }
                newClasses.AddRange(classNames.Values);

                Classes = newClasses;
            }

            if (includedSchema.Relations != null)
            {
                var newRelations = includedSchema.Relations.ToList();

                if (Relations != null)
                {
                    newRelations.AddRange(Relations);
                }

                Relations = newRelations;
            }

            if (includedSchema.DataSources != null)
            {
                var sourceNames = new Dictionary<string, DataSourceInfo>();

                var newDataSources = new List<DataSourceInfo>();

                if (DataSources != null)
                {
                    foreach (DataSourceInfo ci in DataSources)
                    {
                        newDataSources.Add(ci);
                        sourceNames.Add(ci.Name, ci);
                    }
                }

                foreach (DataSourceInfo ci in includedSchema.DataSources)
                {
                    DataSourceInfo di;
                    if (sourceNames.TryGetValue(ci.Name, out di))
                        di.EnableDynamicFields |= ci.EnableDynamicFields;
                    else
                        newDataSources.Add(ci);
                }

                DataSources = newDataSources;
            }
        }

        public object GetDefaultPrecommitValueForDataType(FieldDataType dataType)
        {
            if (PrecommitValues != null)
            {
                // ReSharper disable once ForCanBeConvertedToForeach
                for (int i = 0; i < PrecommitValues.Count; ++i)
                    if (dataType == PrecommitValues[i].DataType)
                        return PrecommitValues[i].Value;
            }
            if (DefaultPrecommitValues)
            {
                return FieldHandlerFactory.GetFieldHandler(dataType).DefaultPrecommitValue();
            }
            return null;
        }

        public StringCollection GetBackRefCollections(FieldInfo fi)
        {
            StringCollection collections;
            _backRefCollections.TryGetValue(fi.NameTag, out collections);
            return collections;
        }

        internal void AddBackRefCollection(FieldInfo fi, string name)
        {
            StringCollection sc = GetBackRefCollections(fi);
            if (sc == null)
            {
                sc = new StringCollection();
                _backRefCollections[fi.NameTag] = sc;
            }
            sc.Add(name);
        }
    }
}