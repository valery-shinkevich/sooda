namespace Sooda.Utils
{
    using System;
    using System.Data.SqlTypes;
    using System.Reflection;

    public class SqlTypesUtil
    {
        public static object Unwrap(object o)
        {
            var sqlType = o as INullable;
            if (sqlType == null)
                return o;

            return sqlType.IsNull ? null : o.GetType().GetProperty("Value").GetValue(o, null);
        }

        internal static object Wrap(Type fieldType, object val)
        {
            if (val == null)
            {
                FieldInfo nullProperty = fieldType.GetField("Null", BindingFlags.Static | BindingFlags.Public);
                return nullProperty.GetValue(null);
            }

            ConstructorInfo constructorInfo = fieldType.GetConstructor(new[] {val.GetType()});
            return constructorInfo.Invoke(new[] {val});
        }

        public static void SetValue(FieldInfo fi, object obj, object val)
        {
            Type fieldType = fi.FieldType;
            if (typeof (INullable).IsAssignableFrom(fieldType))
                val = Wrap(fieldType, val);
            fi.SetValue(obj, val);
        }
    }
}