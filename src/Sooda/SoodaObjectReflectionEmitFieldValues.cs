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
    using System.Collections.Generic;
    using System.Data.SqlTypes;
    using System.Reflection;
    using System.Reflection.Emit;

    public abstract class SoodaObjectReflectionEmitFieldValues : SoodaObjectFieldValues
    {
        private readonly string[] _orderedFieldNames;
        private static readonly Dictionary<int, FieldAccessor[]> FieldAccessors = new Dictionary<int, FieldAccessor[]>();

        // ReSharper disable FieldCanBeMadeReadOnly.Local
        private static object _thisLock = new object();
        // ReSharper restore FieldCanBeMadeReadOnly.Local

        internal class FieldAccessor
        {
            private readonly Func<object, object> _getter;
            private readonly Action<object, object> _setter;

            internal FieldAccessor(Type type, string name)
            {
                _getter = CreateGetter(type.GetField(name));
                _setter = CreateSetter(type.GetField(name));
            }

            internal object this[object o]
            {
                get { return _getter(o); }
                set { _setter(o, value); }
            }

            protected static Func<object, object> CreateGetter(FieldInfo field)
            {
                var getMethod = new DynamicMethod("Get_" + field.Name, typeof (object), new[] {typeof (object)},
                    typeof (SoodaObjectReflectionEmitFieldValues), true);
                var il = getMethod.GetILGenerator();

                il.Emit(OpCodes.Ldarg_0);

                il.Emit(field.ReflectedType.IsValueType ? OpCodes.Unbox_Any : OpCodes.Castclass, field.ReflectedType);

                il.Emit(OpCodes.Ldfld, field);

                if (field.FieldType.IsValueType)
                {
                    il.Emit(OpCodes.Box, field.FieldType);
                }

                var local = il.DeclareLocal(typeof (object));
                var result = il.DeclareLocal(typeof (object));
                var label1 = il.DefineLabel();
                var label2 = il.DefineLabel();
                var label3 = il.DefineLabel();
                var label4 = il.DefineLabel();
                var labelEnd = il.DefineLabel();
                var sqlType = il.DeclareLocal(typeof (INullable));

                il.Emit(OpCodes.Stloc_0, local); //         stloc.1
                il.Emit(OpCodes.Ldloc_0, local); //IL_0012: ldloc.1
                il.Emit(OpCodes.Ldnull); //IL_0013: ldnull
                il.Emit(OpCodes.Ceq); //IL_0014: ceq
                il.Emit(OpCodes.Ldc_I4_0); //IL_0016: ldc.i4.0
                il.Emit(OpCodes.Ceq); //IL_0017: ceq

                il.Emit(OpCodes.Brtrue_S, label1); //IL_001d: brtrue.s IL_0023

                il.Emit(OpCodes.Ldnull); //IL_001f: ldnull
                il.Emit(OpCodes.Stloc_1, result); //IL_0020: stloc.3
                il.Emit(OpCodes.Br_S, labelEnd); //IL_0021: br.s IL_005e

                il.MarkLabel(label1);
                il.Emit(OpCodes.Ldloc_0, local); //IL_0023: ldloc.1

                il.Emit(OpCodes.Isinst, typeof (INullable));
                    //IL_0024: isinst class [System.Data]System.Data.SqlTypes.INullable

                il.Emit(OpCodes.Stloc_2, sqlType); //IL_0029: stloc.2
                il.Emit(OpCodes.Ldloc_2, sqlType); //IL_002a: ldloc.2
                il.Emit(OpCodes.Ldnull); //IL_002b: ldnull
                il.Emit(OpCodes.Ceq); //IL_002c: ceq
                il.Emit(OpCodes.Brtrue_S, label2); //IL_0032: brtrue.s IL_005a

                il.Emit(OpCodes.Ldloc_2, sqlType); //IL_0035: ldloc.2
                il.Emit(OpCodes.Callvirt, typeof (INullable).GetProperty("IsNull").GetGetMethod());
                //IL_0036: callvirt instance bool [System.Data]System.Data.SqlTypes.INullable::get_IsNull()
                il.Emit(OpCodes.Brtrue_S, label3); //IL_003b: brtrue.s IL_0056

                il.Emit(OpCodes.Ldloc_0, local); //IL_003d: ldloc.1
                il.Emit(OpCodes.Callvirt, typeof (object).GetMethod("GetType"));
                //IL_003e: callvirt instance class [mscorlib]System.Type object::GetType()                
                il.Emit(OpCodes.Ldstr, "Value"); //IL_0043: ldstr "Value"
                il.Emit(OpCodes.Callvirt, typeof (Type).GetMethod("GetProperty", new[] {typeof (string)}));
                //IL_0048: callvirt instance class [mscorlib]System.Reflection.PropertyInfo [mscorlib]System.Type::GetProperty(string)
                il.Emit(OpCodes.Ldloc_0, local); //IL_004d: ldloc.1
                il.Emit(OpCodes.Ldnull); //IL_004e: ldnull
                il.Emit(OpCodes.Callvirt,
                    typeof (PropertyInfo).GetMethod("GetValue", new[] {typeof (object), typeof (object[])}));
                //IL_004f: callvirt instance object [mscorlib]System.Reflection.PropertyInfo::GetValue(object, object[])
                il.Emit(OpCodes.Br_S, label4); //IL_0054: br.s IL_0057
                il.MarkLabel(label3);
                il.Emit(OpCodes.Ldnull); //IL_0056: ldnull
                il.MarkLabel(label4);
                il.Emit(OpCodes.Stloc_1, result); //IL_0057: stloc.3
                il.Emit(OpCodes.Br_S, labelEnd); //IL_0058: br.s IL_005e
                il.MarkLabel(label2);
                il.Emit(OpCodes.Ldloc_0, local); //IL_005a: ldloc.1
                il.Emit(OpCodes.Stloc_1, result); //IL_005b: stloc.3
                //il.Emit(OpCodes.Br_S, labelEnd);    //IL_005c: br.s IL_005e
                il.MarkLabel(labelEnd);
                il.Emit(OpCodes.Ldloc_1, result); //IL_005e: ldloc.3
                il.Emit(OpCodes.Ret);
                return (Func<object, object>) getMethod.CreateDelegate(typeof (Func<object, object>));
            }

            protected static Action<object, object> CreateSetter(FieldInfo field)
            {
                var setMethod = new DynamicMethod("Set_" + field.Name, typeof (void),
                    new[] {typeof (object), typeof (object)},
                    typeof (SoodaObjectReflectionEmitFieldValues), true);
                var il = setMethod.GetILGenerator();

                var localValue = il.DeclareLocal(field.FieldType);

                if (typeof (INullable).IsAssignableFrom(field.FieldType))
                {
                    var labElse = il.DefineLabel();
                    var endIf = il.DefineLabel();

                    il.Emit(OpCodes.Ldarg_1); // if(val==nul)
                    il.Emit(OpCodes.Ldnull);
                    il.Emit(OpCodes.Ceq);
                    il.Emit(OpCodes.Ldc_I4_0);
                    il.Emit(OpCodes.Ceq);
                    il.Emit(OpCodes.Brtrue_S, labElse); // переход если не null

                    //блок если val == null

                    // ReSharper disable AssignNullToNotNullAttribute                
                    il.Emit(OpCodes.Ldsfld, field.FieldType.GetField("Null", BindingFlags.Static | BindingFlags.Public));
                    // ReSharper restore AssignNullToNotNullAttribute

                    il.Emit(OpCodes.Stloc_0, localValue);

                    il.Emit(OpCodes.Br_S, endIf);

                    //блок если val != null
                    il.MarkLabel(labElse);
                    il.Emit(OpCodes.Nop);

                    il.Emit(OpCodes.Ldtoken, field.FieldType);

                    il.Emit(OpCodes.Call, typeof (Type).GetMethod("GetTypeFromHandle"));
                    il.Emit(OpCodes.Ldarg_1);

                    il.Emit(OpCodes.Call,
                        typeof (FieldAccessor).GetMethod("GetSqlValue", BindingFlags.Static | BindingFlags.NonPublic));

                    il.Emit(field.FieldType.IsValueType ? OpCodes.Unbox_Any : OpCodes.Castclass, field.FieldType);
                    il.Emit(OpCodes.Stloc_0, localValue);

                    il.MarkLabel(endIf);
                    il.Emit(OpCodes.Nop);
                }
                else
                {
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(field.FieldType.IsValueType ? OpCodes.Unbox_Any : OpCodes.Castclass, field.FieldType);
                    il.Emit(OpCodes.Stloc_0, localValue);
                }

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(field.ReflectedType.IsValueType ? OpCodes.Unbox_Any : OpCodes.Castclass, field.ReflectedType);

                il.Emit(OpCodes.Ldloc_0, localValue);

                il.Emit(OpCodes.Stfld, field);

                il.Emit(OpCodes.Ret);

                return (Action<object, object>) setMethod.CreateDelegate(typeof (Action<object, object>));
            }

            // ReSharper disable UnusedMember.Local
            private static object GetSqlValue(Type type, object value)
                // ReSharper restore UnusedMember.Local
            {
                object result = null;
                var constructorInfo = type.GetConstructor(new[] {value.GetType()});
                if (constructorInfo != null)
                {
                    result = constructorInfo.Invoke(new[] {value});
                }
                return result;
            }
        }

        protected SoodaObjectReflectionEmitFieldValues(string[] orderedFieldNames)
        {
            _orderedFieldNames = orderedFieldNames;
        }

        protected SoodaObjectReflectionEmitFieldValues(SoodaObjectReflectionEmitFieldValues other)
        {
            _orderedFieldNames = other.GetFieldNames();

            for (var i = 0; i < _orderedFieldNames.Length; ++i)
            {
                // ReSharper disable DoNotCallOverridableMethodsInConstructor
                SetFieldValue(i, other.GetBoxedFieldValue(i));
                // ReSharper restore DoNotCallOverridableMethodsInConstructor
            }
        }

        public override void SetFieldValue(int fieldOrdinal, object val)
        {
            var accessors = GetFieldAccessors();
            accessors[fieldOrdinal][this] = val;
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
            var accessors = GetFieldAccessors();
            return accessors[fieldOrdinal][this];
        }

        private FieldAccessor[] GetFieldAccessors()
        {
            FieldAccessor[] accessors;
            var type = GetType();
            if (!FieldAccessors.TryGetValue(type.MetadataToken, out accessors))
            {
                lock (_thisLock)
                {
                    if (!FieldAccessors.TryGetValue(type.MetadataToken, out accessors))
                    {
                        accessors = CreateAccessors(type);
                    }
                }
            }
            return accessors;
        }

        private FieldAccessor[] CreateAccessors(Type type)
        {
            var accessors = new FieldAccessor[_orderedFieldNames.Length];
            for (var index = 0; index < _orderedFieldNames.Length; index++)
            {
                accessors[index] = new FieldAccessor(type, _orderedFieldNames[index]);
            }
            FieldAccessors.Add(type.MetadataToken, accessors);
            return accessors;
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