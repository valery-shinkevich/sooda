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
    using System.ComponentModel;
    using System.Xml.Serialization;

    /// <remarks/>
    [XmlType(Namespace = "http://www.sooda.org/schemas/SoodaSchema.xsd")]
    [Serializable]
    public class CollectionOnetoManyInfo : CollectionBaseInfo
    {
        [XmlAttribute("class")] public string ClassName;

        [NonSerialized] [XmlIgnore] public ClassInfo Class;

        [XmlAttribute("foreignField")] public string ForeignFieldName;

        [XmlAttribute("prefetch")] [DefaultValue(0)] public int PrefetchLevel;

        [NonSerialized] [XmlIgnore] public FieldInfo ForeignField2;

        public string ForeignColumn
        {
            get { return ForeignField2.DBColumnName; }
        }

        [XmlAttribute("where")] public string Where;

        [XmlAttribute("cache")] [DefaultValue(false)] public bool Cache;

        //wash{
        [XmlAttribute("UITypeEditor")] // ReSharper disable once InconsistentNaming
        public string UITypeEditor;

        [XmlAttribute("DisplayName")] public string DisplayName;
        [XmlAttribute("Browsable"), DefaultValue(false)] public bool Browsable;
        [XmlAttribute("Category")] public string Category;

        [XmlAttribute("ReadOnly"), DefaultValue(true)] public bool ReadOnly;
        //}wash


        public override ClassInfo GetItemClass()
        {
            return Class;
        }
    }
}