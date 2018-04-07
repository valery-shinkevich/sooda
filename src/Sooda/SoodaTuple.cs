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

namespace Sooda
{
    using System;
    using System.Text;

    [Serializable]
    public class SoodaTuple : IComparable
    {
        private object[] _items;

        public SoodaTuple(int size)
        {
            _items = new object[size];
        }

        public SoodaTuple(params object[] items)
        {
            _items = items;
        }

        public override bool Equals(object obj)
        {
            SoodaTuple tuple2 = obj as SoodaTuple;
            if (tuple2 == null)
                return false;

            if (_items.Length != tuple2._items.Length)
                return false;

            for (int i = 0; i < _items.Length; ++i)
            {
                if (!_items[i].Equals(tuple2._items[i]))
                    return false;
            }
            return true;
        }

        public override int GetHashCode()
        {
            int retVal = 0;

            for (int i = 0; i < _items.Length; ++i)
            {
                retVal = retVal ^ _items[i].GetHashCode();
            }
            return retVal;
        }

        public int Length
        {
            get { return _items.Length; }
        }

        public object GetValue(int ordinal)
        {
            return _items[ordinal];
        }

        public void SetValue(int ordinal, object v)
        {
            _items[ordinal] = v;
        }

        public static object GetValue(object tupleOrScalar, int ordinal)
        {
            SoodaTuple tuple = tupleOrScalar as SoodaTuple;
            if (tuple != null)
                return tuple.GetValue(ordinal);
            if (ordinal != 0)
                throw new ArgumentException("Ordinal must be zero for scalar values");
            return tupleOrScalar;
        }

        public static object[] GetValuesArray(object tupleOrScalar)
        {
            SoodaTuple tuple = tupleOrScalar as SoodaTuple;
            if (tuple != null)
            {
                object[] retval = new object[tuple.Length];
                for (int i = 0; i < tuple.Length; ++i)
                {
                    retval[i] = tuple.GetValue(i);
                }
                return retval;
            }
            return new[] {tupleOrScalar};
        }

        public bool IsAllNotNull()
        {
            for (int i = 0; i < _items.Length; ++i)
            {
                if (_items[i] == null)
                    return false;
            }
            return true;
        }

        public int CompareTo(object obj)
        {
            throw new NotImplementedException();
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append('[');
            for (int i = 0; i < _items.Length; ++i)
            {
                if (i > 0)
                    sb.Append(',');
                sb.Append(_items[i]);
            }
            sb.Append(']');
            return sb.ToString();
        }
    }
}