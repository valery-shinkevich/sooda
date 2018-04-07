namespace Sooda.Schema
{
    using System;
    using System.Xml.Serialization;

    [XmlType(Namespace = "http://www.sooda.org/schemas/SoodaSchema.xsd")]
    [Serializable]
    public class EnumInfo
    {
        [XmlAttribute("name")] public string Name;

        [XmlAttribute("label")] public string Label;

        [XmlElement("value")] public EnumValueInfoCollection Values = new EnumValueInfoCollection();
    }

    [Serializable]
    public class EnumInfoCollection : System.Collections.CollectionBase
    {
        public EnumInfoCollection()
        {
            // empty
        }

        public EnumInfoCollection(EnumInfo[] items)
        {
            AddRange(items);
        }

        public EnumInfoCollection(EnumInfoCollection items)
        {
            AddRange(items);
        }

        public virtual void AddRange(EnumInfo[] items)
        {
            foreach (EnumInfo item in items)
            {
                List.Add(item);
            }
        }

        public virtual void AddRange(EnumInfoCollection items)
        {
            foreach (EnumInfo item in items)
            {
                List.Add(item);
            }
        }

        public virtual void Add(EnumInfo value)
        {
            List.Add(value);
        }

        public virtual bool Contains(EnumInfo value)
        {
            return List.Contains(value);
        }

        public virtual int IndexOf(EnumInfo value)
        {
            return List.IndexOf(value);
        }

        public virtual void Insert(int index, EnumInfo value)
        {
            List.Insert(index, value);
        }

        public virtual EnumInfo this[int index]
        {
            get { return (EnumInfo) List[index]; }
            set { List[index] = value; }
        }

        public virtual void Remove(EnumInfo value)
        {
            List.Remove(value);
        }

        public class Enumerator : System.Collections.IEnumerator
        {
            private System.Collections.IEnumerator wrapped;

            public Enumerator(EnumInfoCollection collection)
            {
                wrapped = ((System.Collections.CollectionBase) collection).GetEnumerator();
            }

            public EnumInfo Current
            {
                get { return (EnumInfo) (wrapped.Current); }
            }

            object System.Collections.IEnumerator.Current
            {
                get { return (EnumInfo) (wrapped.Current); }
            }

            public bool MoveNext()
            {
                return wrapped.MoveNext();
            }

            public void Reset()
            {
                wrapped.Reset();
            }
        }

        public new virtual Enumerator GetEnumerator()
        {
            return new Enumerator(this);
        }
    }

    [XmlType(Namespace = "http://www.sooda.org/schemas/SoodaSchema.xsd")]
    [Serializable]
    public class EnumValueInfo
    {
        [XmlAttribute("name")] public string Name;

        [XmlAttribute("key")] public string Key;

        [XmlAttribute("label")] public string Label;

        [XmlIgnore] [NonSerialized] public EnumInfo Enum;
    }

    [Serializable]
    public class EnumValueInfoCollection : System.Collections.CollectionBase
    {
        public EnumValueInfoCollection()
        {
            // empty
        }

        public EnumValueInfoCollection(EnumValueInfo[] items)
        {
            AddRange(items);
        }

        public EnumValueInfoCollection(EnumValueInfoCollection items)
        {
            AddRange(items);
        }

        public virtual void AddRange(EnumValueInfo[] items)
        {
            foreach (EnumValueInfo item in items)
            {
                List.Add(item);
            }
        }

        public virtual void AddRange(EnumValueInfoCollection items)
        {
            foreach (EnumValueInfo item in items)
            {
                List.Add(item);
            }
        }

        public virtual void Add(EnumValueInfo value)
        {
            List.Add(value);
        }

        public virtual bool Contains(EnumValueInfo value)
        {
            return List.Contains(value);
        }

        public virtual int IndexOf(EnumValueInfo value)
        {
            return List.IndexOf(value);
        }

        public virtual void Insert(int index, EnumValueInfo value)
        {
            List.Insert(index, value);
        }

        public virtual EnumValueInfo this[int index]
        {
            get { return (EnumValueInfo) List[index]; }
            set { List[index] = value; }
        }

        public virtual void Remove(EnumValueInfo value)
        {
            List.Remove(value);
        }

        public class Enumerator : System.Collections.IEnumerator
        {
            private System.Collections.IEnumerator wrapped;

            public Enumerator(EnumValueInfoCollection collection)
            {
                wrapped = ((System.Collections.CollectionBase) collection).GetEnumerator();
            }

            public EnumValueInfo Current
            {
                get { return (EnumValueInfo) (wrapped.Current); }
            }

            object System.Collections.IEnumerator.Current
            {
                get { return (EnumValueInfo) (wrapped.Current); }
            }

            public bool MoveNext()
            {
                return wrapped.MoveNext();
            }

            public void Reset()
            {
                wrapped.Reset();
            }
        }

        public new virtual Enumerator GetEnumerator()
        {
            return new Enumerator(this);
        }
    }
}