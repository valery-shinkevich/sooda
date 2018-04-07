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
    using System.ComponentModel;
    using System.Data;
    using System.Diagnostics;
    using QL;

    [DebuggerStepThrough]
    public class SoodaObjectCollectionWrapperGeneric<T> : ISoodaObjectList, ISoodaObjectListInternal, IBindingListView,
        IList<T>
    {
        private ISoodaObjectList _theList;

        protected SoodaObjectCollectionWrapperGeneric()
        {
            _theList = new SoodaObjectListSnapshot();
            _mOriginalCollection = new SoodaObjectListSnapshot();
        }

        protected SoodaObjectCollectionWrapperGeneric(ISoodaObjectList list)
        {
            _theList = list;
            _mOriginalCollection = new SoodaObjectListSnapshot();
        }

        #region ISoodaObjectList

        SoodaObject ISoodaObjectList.GetItem(int pos)
        {
            return _theList.GetItem(pos);
        }

        public bool IsReadOnly
        {
            get { return _theList.IsReadOnly; }
        }

        object IList.this[int index]
        {
            get { return _theList[index]; }
            set { _theList[index] = value; }
        }

        public void RemoveAt(int index)
        {
            _theList.RemoveAt(index);
        }

        void IList.Insert(int index, object value)
        {
            _theList.Insert(index, value);
        }

        void IList.Remove(object value)
        {
            _theList.Remove(value);
        }

        bool IList.Contains(object value)
        {
            return _theList.Contains(value);
        }

        public void Clear()
        {
            _theList.Clear();
        }

        int IList.IndexOf(object value)
        {
            return _theList.IndexOf(value);
        }

        int IList.Add(object value)
        {
            return _theList.Add(value);
        }

        bool IList.IsFixedSize
        {
            get { return _theList.IsFixedSize; }
        }

        bool ICollection.IsSynchronized
        {
            get { return _theList.IsSynchronized; }
        }

        public int Count
        {
            get { return _theList.Count; }
        }

        public int PagedCount
        {
            get { return _theList.PagedCount; }
        }

        void ICollection.CopyTo(Array array, int index)
        {
            _theList.CopyTo(array, index);
        }

        object ICollection.SyncRoot
        {
            get { return _theList.SyncRoot; }
        }

        public IEnumerator GetEnumerator()
        {
            return _theList.GetEnumerator();
        }

        public ISoodaObjectList GetSnapshot2()
        {
            return _theList.GetSnapshot();
        }

        ISoodaObjectList ISoodaObjectList.GetSnapshot()
        {
            return _theList.GetSnapshot();
        }

        ISoodaObjectList ISoodaObjectList.Filter(SoodaWhereClause whereClause)
        {
            return _theList.Filter(whereClause);
        }

        ISoodaObjectList ISoodaObjectList.Filter(SoqlBooleanExpression filterExpression)
        {
            return _theList.Filter(filterExpression);
        }

        ISoodaObjectList ISoodaObjectList.Filter(SoodaObjectFilter filter)
        {
            return _theList.Filter(filter);
        }

        protected ISoodaObjectList Filter2(SoodaWhereClause whereClause)
        {
            return _theList.Filter(whereClause);
        }

        protected ISoodaObjectList Filter2(SoqlBooleanExpression filterExpression)
        {
            return _theList.Filter(filterExpression);
        }

        protected ISoodaObjectList Filter2(SoodaObjectFilter filter)
        {
            return _theList.Filter(filter);
        }

        ISoodaObjectList ISoodaObjectList.Sort(IComparer comparer)
        {
            return _theList.Sort(comparer);
        }

        ISoodaObjectList ISoodaObjectList.Sort(string sortOrder)
        {
            return _theList.Sort(SoodaOrderBy.Parse(sortOrder).GetComparer());
        }

        ISoodaObjectList ISoodaObjectList.Sort(SoqlExpression sortExpression)
        {
            return _theList.Sort(SoodaOrderBy.FromExpression(sortExpression, SortOrder.Ascending).GetComparer());
        }

        ISoodaObjectList ISoodaObjectList.Sort(SoqlExpression sortExpression, SortOrder sortOrder)
        {
            return _theList.Sort(SoodaOrderBy.FromExpression(sortExpression, sortOrder).GetComparer());
        }

        protected ISoodaObjectList Sort2(IComparer comparer)
        {
            return _theList.Sort(comparer);
        }

        protected ISoodaObjectList Sort2(string sortOrder)
        {
            return _theList.Sort(SoodaOrderBy.Parse(sortOrder).GetComparer());
        }

        protected ISoodaObjectList Sort2(SoqlExpression sortExpression)
        {
            return _theList.Sort(SoodaOrderBy.FromExpression(sortExpression, SortOrder.Ascending).GetComparer());
        }

        protected ISoodaObjectList Sort2(SoqlExpression sortExpression, SortOrder sortOrder)
        {
            return _theList.Sort(SoodaOrderBy.FromExpression(sortExpression, sortOrder).GetComparer());
        }

        public ISoodaObjectList SelectFirst2(int count)
        {
            return _theList.SelectFirst(count);
        }

        public ISoodaObjectList SelectLast2(int count)
        {
            return _theList.SelectLast(count);
        }

        public ISoodaObjectList SelectRange2(int from, int to)
        {
            return _theList.SelectRange(from, to);
        }

        ISoodaObjectList ISoodaObjectList.SelectFirst(int count)
        {
            return _theList.SelectFirst(count);
        }

        ISoodaObjectList ISoodaObjectList.SelectLast(int count)
        {
            return _theList.SelectLast(count);
        }

        ISoodaObjectList ISoodaObjectList.SelectRange(int from, int to)
        {
            return _theList.SelectRange(from, to);
        }

        public void InternalAdd(SoodaObject o)
        {
            ((ISoodaObjectListInternal) _theList).InternalAdd(o);
            // TODO:  Add SoodaObjectCollectionWrapper.InternalAdd implementation
        }

        public void InternalRemove(SoodaObject o)
        {
            ((ISoodaObjectListInternal) _theList).InternalRemove(o);
        }

        #endregion

        #region IBindingListView Members

        private string _mFilterString = String.Empty;
        private bool _mFiltered, _mSorted;
        private readonly ISoodaObjectList _mOriginalCollection;
        [NonSerialized] private PropertyDescriptor _mSortBy;
        private ListSortDirection _mSortDirection = ListSortDirection.Ascending;

        string IBindingListView.Filter
        {
            get { return _mFilterString; }
            set
            {
                string old = _mFilterString;
                _mFilterString = value;
                //Console.WriteLine("set to '{0}'", value);
                if (_mFilterString != old)
                {
                    UpdateFilter();
                }
            }
        }

        void IBindingListView.RemoveFilter()
        {
            Console.WriteLine("Removed");
            if (!_mFiltered)
                return;
            _mFilterString = null;
            _mFiltered = false;
            _mSorted = false;
            Clear();
            foreach (object item in _mOriginalCollection)
            {
                _theList.Add(item);
            }
            _mOriginalCollection.Clear();
        }

        protected virtual void UpdateFilter()
        {
            if (_mOriginalCollection.Count == 0)
            {
                foreach (object item in this)
                {
                    _mOriginalCollection.Add(item);
                }
            }

            ISoodaObjectList tCollection = _mOriginalCollection.Filter(
                string.IsNullOrEmpty(_mFilterString)
                    ? new SoodaWhereClause()
                    : new SoodaWhereClause(_mFilterString.Replace("[", string.Empty).Replace("]", string.Empty).Trim()));

            Clear();

            foreach (object item in tCollection)
            {
                _theList.Add(item);
            }

            _mFiltered = true;

            OnListChanged(this, new ListChangedEventArgs(ListChangedType.Reset, -1));
        }

        public bool SupportsFiltering
        {
            get { return true; }
        }

        public void ApplySort(ListSortDescriptionCollection sorts)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public ListSortDescriptionCollection SortDescriptions
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }

        public bool SupportsAdvancedSorting
        {
            get { return false; }
        }

        #endregion

        #region IBindingList Members

        public void AddIndex(PropertyDescriptor property)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public object AddNew()
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public bool AllowEdit
        {
            get { return true; }
        }

        public bool AllowNew
        {
            get { return false; }
        }

        public bool AllowRemove
        {
            get { return false; }
        }

        //wash{
        public void ApplySort(PropertyDescriptor property, ListSortDirection direction)
        {
            string sortOrder = property.Name + (direction == ListSortDirection.Ascending ? " asc" : " desc");
            _theList = _theList.Sort(SoodaOrderBy.Parse(sortOrder).GetComparer());
            _mSortBy = property;
            _mSorted = true;
            _mSortDirection = direction;
            OnListChanged(this, new ListChangedEventArgs(ListChangedType.Reset, 0));
        }

        //}wash
        public int Find(PropertyDescriptor property, object key)
        {
            for (int index = 0; index < Count; index++)
            {
                var o = this[index] as SoodaObject;

                if (o == null) throw new StrongTypingException("В списке найден объект с нестандартным типом.");

                var a = o.FieldValues.GetBoxedFieldValue(property.Name); //property.GetValue(o);)

                if (a == null && key == null) return index;
                if (a == null || key == null) continue;
                if (((IComparable) a).CompareTo(key) == 0)
                    return index;
            }
            return -1;
        }

        public bool IsSorted
        {
            get { return _mSorted; }
        }

        protected void OnListChanged(object sender, ListChangedEventArgs e)
        {
            if (_listChanged != null)
            {
                _listChanged(sender, e);
            }
        }

        private ListChangedEventHandler _listChanged;

        event ListChangedEventHandler IBindingList.ListChanged
        {
            add { _listChanged += value; }
            // ReSharper disable once DelegateSubtraction
            remove { if (_listChanged != null) _listChanged -= value; }
        }

        public void RemoveIndex(PropertyDescriptor property)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public void RemoveSort()
        {
            _mSortBy = null;
            _mSorted = false;
        }

        public ListSortDirection SortDirection
        {
            get { return _mSortDirection; }
        }

        public PropertyDescriptor SortProperty
        {
            get { return _mSortBy; }
        }

        public bool SupportsChangeNotification
        {
            get { return false; }
        }

        public bool SupportsSearching
        {
            get { return true; }
        }

        public bool SupportsSorting
        {
            get { return true; }
        }

        #endregion

        #region IList<T> Members

        public int IndexOf(T item)
        {
            return _theList.IndexOf(item);
        }

        public void Insert(int index, T item)
        {
            _theList.Insert(index, item);
        }

        public T this[int index]
        {
            get { return (T) _theList[index]; }
            set { _theList[index] = value; }
        }

        #endregion

        #region ICollection<T> Members

        public void Add(T item)
        {
            _theList.Add(item);
        }

        public bool Contains(T item)
        {
            return _theList.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _theList.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            _theList.Remove(item);
            return true;
        }

        #endregion

        #region IEnumerable<T> Members

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            for (var i = 0; i < Count; ++i)
            {
                yield return this[i];
            }
        }

        #endregion
    }
}