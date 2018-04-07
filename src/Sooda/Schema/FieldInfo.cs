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

namespace Sooda.Schema
{
    using System;
    using System.Collections.Specialized;
    using System.ComponentModel;
    using System.Xml;
    using System.Xml.Serialization;
    using ObjectMapper;
    using ObjectMapper.FieldHandlers;

    [XmlType(Namespace = "http://www.sooda.org/schemas/SoodaSchema.xsd")]
    [Serializable]
    public class FieldInfo : ICloneable
    {
        [XmlAttribute("name")] public string Name;

        private string _dbcolumn;

        [XmlAttribute("type")] public FieldDataType DataType;

        [XmlAttribute("size")] [DefaultValue(-1)] public int Size = -1;

        [XmlAttribute("enum")] public string Enum;

        [XmlAttribute("precision")] [DefaultValue(-1)] public int Precision = -1;

        [XmlAttribute("references")] public string References;

        [XmlAttribute("precommitValue")] public string PrecommitValue;

        //wash{
        [XmlAttribute("uid")] // ReSharper disable once InconsistentNaming
        public Guid uid;

        [XmlAttribute("UITypeEditor")] // ReSharper disable once InconsistentNaming
        public string UITypeEditor;

        [XmlAttribute("TypeConverter")] public string TypeConverter;

        [XmlAttribute("DisplayName")] public string DisplayName;

        [XmlAttribute("Browsable")] public bool Browsable = true;

        [XmlAttribute("Category")] public string Category;

        [XmlAttribute("Description")] public string Description;

        [XmlAttribute("ReadOnly")] [DefaultValue(false)] public bool ReadOnly;

        [XmlAttribute("UiReadOnly")] [DefaultValue(false)] public bool UiReadOnly;

        [XmlAttribute("Identity")] [DefaultValue(false)] public bool IsIdentity;

        [XmlAttribute("IsIndexed")] [DefaultValue(false)] public bool IsIndexed;

        [XmlAttribute("Modifier")] public string Modifier = String.Empty;

        [XmlAttribute("IsRowGuidCol")] [DefaultValue(false)] public bool IsRowGuidCol;

        [XmlAttribute("IsFileStream")] [DefaultValue(false)] public bool IsFileStream;

        //}wash

        [XmlIgnore] [NonSerialized] public object PrecommitTypedValue;

        [XmlAttribute("primaryKey")] [DefaultValue(false)] public bool IsPrimaryKey;

        [XmlAttribute("nullable")] [DefaultValue(false)] public bool IsNullable;

        [XmlAttribute("forceTrigger")] [DefaultValue(false)] public bool ForceTrigger;

        [XmlAttribute("onDelete")] [DefaultValue(DeleteAction.Nothing)] public DeleteAction DeleteAction =
            DeleteAction.Nothing;

        [XmlAnyAttribute, NonSerialized] public XmlAttribute[] Extensions;

        [XmlAttribute("label")] [DefaultValue(false)] public bool IsLabel;

        [XmlAttribute("prefetch")] [DefaultValue(0)] public int PrefetchLevel;

        [XmlAttribute("find")] [DefaultValue(false)] public bool FindMethod;

        [XmlAttribute("findList")] [DefaultValue(false)] public bool FindListMethod;

        [XmlAttribute("dbcolumn")]
        // ReSharper disable once InconsistentNaming
        public string DBColumnName
        {
            get { return _dbcolumn ?? Name; }
            set { _dbcolumn = value; }
        }

        public FieldInfo Clone()
        {
            return DoClone();
        }

        object ICloneable.Clone()
        {
            return DoClone();
        }

        [XmlIgnore] [NonSerialized] public int OrdinalInTable;

        [XmlIgnore] [NonSerialized] public int ClassLocalOrdinal;

        [XmlIgnore] [NonSerialized] public int ClassUnifiedOrdinal;

        [XmlIgnore] [NonSerialized] public TableInfo Table;

        [XmlIgnore] [NonSerialized] public ClassInfo ReferencedClass;

        [XmlIgnore] [NonSerialized] public ClassInfo ParentClass;

        [XmlIgnore] [NonSerialized] internal string NameTag;

        [XmlIgnore] [NonSerialized] public RelationInfo ParentRelation;

        public FieldInfo DoClone()
        {
            var fi = new FieldInfo
            {
                Name = Name,
                _dbcolumn = _dbcolumn,
                IsNullable = IsNullable,
                DataType = DataType,
                Size = Size,
                ForceTrigger = ForceTrigger,
                Description = Description,
                Browsable = Browsable,
                Category = Category,
                DisplayName = DisplayName,
                UITypeEditor = UITypeEditor,
                TypeConverter = TypeConverter,
                IsIdentity = IsIdentity,
                ReadOnly = ReadOnly,
                Precision = Precision,
                PrecommitValue = PrecommitValue,
                uid = uid,
                Modifier = Modifier,
                IsPrimaryKey = IsPrimaryKey,
                References = References,
                ReferencedClass = ReferencedClass,
                IsRowGuidCol = IsRowGuidCol,
                IsFileStream = IsFileStream
            };
            return fi;
        }

        public StringCollection GetBackRefCollections(SchemaInfo schema)
        {
            return schema.GetBackRefCollections(this);
        }

        public SoodaFieldHandler GetNullableFieldHandler()
        {
            return FieldHandlerFactory.GetFieldHandler(DataType);
        }

        public SoodaFieldHandler GetFieldHandler()
        {
            return FieldHandlerFactory.GetFieldHandler(DataType, IsNullable);
        }

        internal void Resolve(TableInfo parentTable, string parentName, int ordinal)
        {
            Table = parentTable;
            OrdinalInTable = ordinal;
            NameTag = parentTable.NameToken + "/" + ordinal;
        }

        internal void ResolvePrecommitValues()
        {
        }

        public override string ToString()
        {
            return String.Format("{0}.{1} ({2} ref {3})", ParentClass != null ? "class " + ParentClass.Name : "???",
                Name, DataType, References);
        }

        internal void Merge(FieldInfo merge)
        {
            DataType = merge.DataType;
            if (merge.Description != null)
                Description = (Description != null ? Description + "\n" : "") + merge.Description;
            if (merge.Size != -1)
                Size = merge.Size;
            if (merge.Precision != -1)
                Precision = merge.Size;
            if (merge.References != null)
                References = merge.References;
            if (merge.PrecommitValue != null)
                PrecommitValue = merge.PrecommitValue;
            IsPrimaryKey = merge.IsPrimaryKey;
            IsNullable = merge.IsNullable;
            ReadOnly = merge.ReadOnly;
            ForceTrigger = merge.ForceTrigger;
            DeleteAction = merge.DeleteAction;
            IsLabel = merge.IsLabel;
            PrefetchLevel = merge.PrefetchLevel;
            FindMethod = merge.FindMethod;
            FindListMethod = merge.FindListMethod;
            if (merge._dbcolumn != null)
                DBColumnName = merge._dbcolumn;
        }

        internal void ResolveReferences(SchemaInfo schema)
        {
            if (References == null) return;

            var ci = schema.FindClassByName(References);
            if (ci == null)
                throw new SoodaSchemaException("Class " + References + " not found.");

            DataType = ci.GetFirstPrimaryKeyField().DataType;
            ReferencedClass = ci;
        }

        [XmlIgnore]
        public string TypeName
        {
            get { return References ?? DataType.ToString(); }
            set
            {
#if DOTNET4
                if (System.Enum.TryParse(value, out DataType))
                {
                    References = null;
                    return;
                }
#else
                try
                {
                    DataType = (FieldDataType) System.Enum.Parse(typeof(FieldDataType), value);
                    References = null;
                    return;
                }
                catch (ArgumentException)
                {
                }
#endif
                References = value;
            }
        }

        [XmlIgnore]
        public Type Type
        {
            get
            {
                if (References != null)
                    return Type.GetType(ParentClass.Schema.Namespace + "." + References); // FIXME: included schema
                SoodaFieldHandler handler = GetFieldHandler();
                return IsNullable ? handler.GetNullableType() : handler.GetFieldType();
            }
            set
            {
                if (value == null)
                    throw new ArgumentNullException();
                if (value.IsSubclassOf(typeof (SoodaObject)))
                {
                    References = value.Name;
                }
                else
                {
                    DataType = FieldHandlerFactory.GetFieldDataType(value, out IsNullable);
                    References = null;
                }
            }
        }

        [XmlIgnore]
        public bool IsDynamic
        {
            get { return Table.IsDynamic; }
        }
    }
}