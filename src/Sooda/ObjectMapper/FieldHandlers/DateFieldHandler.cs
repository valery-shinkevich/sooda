namespace Sooda.ObjectMapper.FieldHandlers
{
    using System;
    using System.Data;
    using System.Globalization;

    public class DateFieldHandler : SoodaFieldHandler
    {
        public DateFieldHandler(bool nullable) : base(nullable)
        {
        }

        protected override string TypeName
        {
            get { return "datetime"; }
        }

        public override object RawRead(IDataRecord record, int pos)
        {
            return GetFromReader(record, pos);
        }

        public static DateTime GetFromReader(IDataRecord record, int pos)
        {
            return record.GetDateTime(pos);
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
            return Convert.ToDateTime(obj).ToString(CultureInfo.InvariantCulture);
        }

        public static object DeserializeFromString(string s)
        {
            return DateTime.Parse(s, CultureInfo.InvariantCulture);
        }

        private static readonly object _zeroValue = new DateTime(1800, 1, 1);

        public override object ZeroValue()
        {
            return _zeroValue;
        }

        public override Type GetFieldType()
        {
            return typeof (DateTime);
        }

        public override Type GetSqlType()
        {
            return typeof (System.Data.SqlTypes.SqlDateTime);
        }


        public override void SetupDBParameter(IDbDataParameter parameter, object value)
        {
            parameter.DbType = DbType.DateTime;
            parameter.Value = value;
        }

        // type conversions - used in generated stub code

        public static System.Data.SqlTypes.SqlDateTime GetSqlNullableValue(object fieldValue)
        {
            return fieldValue == null
                ? System.Data.SqlTypes.SqlDateTime.Null
                : new System.Data.SqlTypes.SqlDateTime((DateTime) fieldValue);
        }

        public static DateTime GetNotNullValue(object val)
        {
            if (val == null)
                throw new InvalidOperationException("Attempt to read a non-null value that isn't set yet");
            return (DateTime) val;
        }

        public static DateTime? GetNullableValue(object fieldValue)
        {
            return fieldValue == null ? (DateTime?) null : (DateTime) fieldValue;
        }

        public override Type GetNullableType()
        {
            return typeof (DateTime?);
        }
    }
}