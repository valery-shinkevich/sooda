namespace Sooda.ObjectMapper.FieldHandlers
{
    using System;
    using System.Data;
    using System.Data.SqlTypes;
    using System.Globalization;

    public class MoneyFieldHandler : SoodaFieldHandler
    {
        private static readonly object _zeroValue = 0.0m;

        public MoneyFieldHandler(bool nullable) : base(nullable)
        {
        }

        protected override string TypeName
        {
            get { return "money"; }
        }

        public override object RawRead(IDataRecord record, int pos)
        {
            return GetFromReader(record, pos);
        }

        public static Decimal GetFromReader(IDataRecord record, int pos)
        {
            return record.GetDecimal(pos);
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
            return Convert.ToDecimal(obj).ToString(CultureInfo.InvariantCulture);
        }

        public static object DeserializeFromString(string s)
        {
            return Decimal.Parse(s, CultureInfo.InvariantCulture);
        }

        public override object ZeroValue()
        {
            return _zeroValue;
        }

        public override Type GetFieldType()
        {
            return typeof (Decimal);
        }

        public override Type GetSqlType()
        {
            return typeof (SqlMoney);
        }

        public override void SetupDBParameter(IDbDataParameter parameter, object value)
        {
            parameter.DbType = DbType.Currency;
            parameter.Value = value;
        }

        // type conversions - used in generated stub code

        public static SqlMoney GetSqlNullableValue(object fieldValue)
        {
            if (fieldValue == null)
                return SqlMoney.Null;
            return new SqlMoney((Decimal) fieldValue);
        }

        public static decimal GetNotNullValue(object val)
        {
            if (val == null)
                throw new InvalidOperationException("Attempt to read a non-null value that isn't set yet");
            return (decimal) val;
        }

        public static decimal? GetNullableValue(object fieldValue)
        {
            return fieldValue == null ? (decimal?) null : (decimal) fieldValue;
        }

        public override Type GetNullableType()
        {
            return typeof (decimal?);
        }
    }
}