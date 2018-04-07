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
    using System.Reflection;

    public abstract class SoodaObjectReflectionBasedFieldValues : SoodaObjectFieldValues
    {
        private readonly string[] _orderedFieldNames;

        protected SoodaObjectReflectionBasedFieldValues(string[] orderedFieldNames)
        {
            _orderedFieldNames = orderedFieldNames;
        }

        protected SoodaObjectReflectionBasedFieldValues(SoodaObjectReflectionBasedFieldValues other)
        {
            _orderedFieldNames = other.GetFieldNames();
            for (int i = 0; i < _orderedFieldNames.Length; i++)
            {
                var fi = GetField(i);
                fi.SetValue(this, fi.GetValue(other));
            }
        }

        protected virtual FieldInfo GetField(string name)
        {
            return GetType().GetField(name);
        }

        private FieldInfo GetField(int fieldOrdinal)
        {
            return GetField(_orderedFieldNames[fieldOrdinal]);
        }

        public override void SetFieldValue(int fieldOrdinal, object val)
        {
            var fi = GetField(fieldOrdinal);
            if (typeof (System.Data.SqlTypes.INullable).IsAssignableFrom(fi.FieldType))
            {
                if (val == null)
                {
                    var nullProperty = fi.FieldType.GetField("Null", BindingFlags.Static | BindingFlags.Public);

                    // ReSharper disable AssignNullToNotNullAttribute
                    // ReSharper disable PossibleNullReferenceException
                    var sqlNullValue = nullProperty.GetValue(null);
                    // ReSharper restore PossibleNullReferenceException
                    // ReSharper restore AssignNullToNotNullAttribute
                    fi.SetValue(this, sqlNullValue);
                }
                else
                {
                    var constructorParameterTypes = new[] {val.GetType()};
                    var constructorInfo = fi.FieldType.GetConstructor(constructorParameterTypes);
                    if (constructorInfo != null)
                    {
                        var sqlValue = constructorInfo.Invoke(new[] {val});
                        fi.SetValue(this, sqlValue);
                    }
                }
            }
            else
            {
                fi.SetValue(this, val);
            }
        }

        public override void SetFieldValue(string fieldName, object val)
        {
            for (var i = 0; i < _orderedFieldNames.Length; i++)
            {
                if (fieldName != _orderedFieldNames[i]) continue;

                SetFieldValue(i, val);
                return;
            }
            throw new ArgumentOutOfRangeException("fieldName");
        }


        public override object GetBoxedFieldValue(int fieldOrdinal)
        {
            var fi = GetField(fieldOrdinal);
            var rawValue = fi.GetValue(this);

            if (rawValue == null)
                return null;

            // we got raw value, it's possible that it's a sqltype, nullables are already boxed here

            var sqlType = rawValue as System.Data.SqlTypes.INullable;
            if (sqlType != null)
            {
                return sqlType.IsNull
                    ? null
                    : rawValue.GetType().GetProperty("Value").GetValue(rawValue, null);
            }

            return rawValue;
        }

        public override object GetBoxedFieldValue(string fieldName)
        {
            for (var i = 0; i < _orderedFieldNames.Length; i++)
            {
                if (fieldName != _orderedFieldNames[i]) continue;

                return GetBoxedFieldValue(i);
            }
            throw new ArgumentOutOfRangeException("fieldName");
        }

        public override int Length
        {
            get { return _orderedFieldNames.Length; }
        }

        public override bool IsNull(int fieldOrdinal)
        {
            return GetBoxedFieldValue(fieldOrdinal) == null;
        }

        protected string[] GetFieldNames()
        {
            return _orderedFieldNames;
        }
    }
}