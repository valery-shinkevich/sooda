//
// Copyright (c) 2003-2006 Jaroslaw Kowalski <jaak@jkowalski.net>
// Copyright (c) 2006-2014 Piotr Fusik <piotr@fusik.info>
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
using System.Data;
using System.Data.SqlTypes;

namespace Sooda.ObjectMapper.FieldHandlers
{
    public class BooleanAsIntegerFieldHandler : SoodaFieldHandler
    {
        public BooleanAsIntegerFieldHandler(bool nullable) : base(nullable) { }

        protected override string TypeName
        {
            get
            {
                return "boolint";
            }
        }

        public override object RawRead(IDataRecord record, int pos)
        {
            return GetFromReader(record, pos);
        }

        public static bool GetFromReader(IDataRecord record, int pos)
        {
            return Convert.ToInt32(record.GetValue(pos)) != 0;
        }

        public override string RawSerialize(object val)
        {
            return SerializeToString(val);
        }

        public override object RawDeserialize(string s)
        {
            return DeserializeFromString(s);
        }

        public static string SerializeToString(object obj)
        {
            return Convert.ToBoolean(obj) ? "true" : "false";
        }

        public static object DeserializeFromString(string s)
        {
            return s == "true" ? true : false;
        }

        private static readonly object _zeroValue = false;
        private static readonly object _boxed1 = 1;
        private static readonly object _boxed0 = 0;

        public override object ZeroValue()
        {
            return _zeroValue;
        }

        public override Type GetFieldType()
        {
            return typeof(bool);
        }

        public override Type GetSqlType()
        {
            return typeof(SqlBoolean);
        }

        public override void SetupDBParameter(IDbDataParameter parameter, object value)
        {
            parameter.DbType = DbType.Int32;
            parameter.Value = Convert.ToBoolean(value) ? _boxed1 : _boxed0;
        }

        // type conversions - used in generated stub code

        public static System.Data.SqlTypes.SqlBoolean GetSqlNullableValue(object fieldValue)
        {
            if (fieldValue == null)
                return System.Data.SqlTypes.SqlBoolean.Null;
            else
                return new System.Data.SqlTypes.SqlBoolean((bool) fieldValue);
        }

        public static bool GetNotNullValue(object val)
        {
            if (val == null)
                throw new InvalidOperationException("Attempt to read a non-null value that isn't set yet");
            return (bool) val;
        }

        public static bool? GetNullableValue(object fieldValue)
        {
            if (fieldValue == null)
                return null;
            else
                return (bool) fieldValue;
        }

        public override Type GetNullableType()
        {
            return typeof(bool?);
        }
    }
}
