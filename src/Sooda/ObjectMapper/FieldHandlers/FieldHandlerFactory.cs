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

namespace Sooda.ObjectMapper.FieldHandlers
{
    using System;
    using System.Data.SqlTypes;
    using Schema;

    public class FieldHandlerFactory
    {
        private static readonly SoodaFieldHandler[] NullableHandlers = GetHandlers(true);
        private static readonly SoodaFieldHandler[] NotNullHandlers = GetHandlers(false);

        private static SoodaFieldHandler[] GetHandlers(bool nullable)
        {
            // the order must match FieldDataType
            return new SoodaFieldHandler[]
            {
                new Int32FieldHandler(nullable),
                new Int64FieldHandler(nullable),
                new BooleanFieldHandler(nullable),
                new BooleanAsIntegerFieldHandler(nullable),
                new DecimalFieldHandler(nullable),
                new FloatFieldHandler(nullable),
                new DoubleFieldHandler(nullable),
                new DateTimeFieldHandler(nullable),
                new StringFieldHandler(nullable),
                new BlobFieldHandler(nullable),
                new GuidFieldHandler(nullable),
                new ImageFieldHandler(nullable),
                new TimeSpanFieldHandler(nullable),
                new AnsiStringFieldHandler(nullable),
                new MoneyFieldHandler(nullable),
                new RowVersionFieldHandler(nullable),
                new DateFieldHandler(nullable)
            };
        }

        public static SoodaFieldHandler GetFieldHandler(FieldDataType type)
        {
            return NullableHandlers[(int) type];
        }

        public static SoodaFieldHandler GetFieldHandler(FieldDataType type, bool nullable)
        {
            return (nullable ? NullableHandlers : NotNullHandlers)[(int) type];
        }

        internal static FieldDataType GetFieldDataType(Type type, out bool nullable)
        {
            // make sure booleans returned as BooleanAsInteger instead of the more problematic Boolean
            if (type == typeof (bool))
            {
                nullable = false;
                return FieldDataType.BooleanAsInteger;
            }
            if (type == typeof (bool?) || type == typeof (SqlBoolean))
            {
                nullable = true;
                return FieldDataType.BooleanAsInteger;
            }

            for (int i = 0; i < NullableHandlers.Length; i++)
            {
                SoodaFieldHandler handler = NullableHandlers[i];
                if (type == handler.GetFieldType())
                {
                    nullable = false;
                    return (FieldDataType) i;
                }
                if (type == handler.GetNullableType() || type == handler.GetSqlType())
                {
                    nullable = true;
                    return (FieldDataType) i;
                }
            }
            throw new ArgumentException("Unknown Sooda type for " + type);
        }
    }
}