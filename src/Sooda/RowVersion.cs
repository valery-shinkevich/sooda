namespace Sooda
{
    using System;
    using System.ComponentModel;
    using System.Data.SqlClient;
    using System.Globalization;

    /// <summary>
    /// RowVersion class for operate with SQL Server ROWVERSION (old TIMESTAMP)
    /// </summary>
    [TypeConverter(typeof (RowVersionConverter))]
    [Browsable(false)]
    public sealed class RowVersion : Object, IEquatable<RowVersion>, IComparable<RowVersion>
    {
        private byte[] byteTimeStamp;

        public RowVersion()
        {
            GetFromDB();
        }

        public RowVersion(byte[] value)
        {
            byteTimeStamp = value;
        }

        [Browsable(false)]
        public byte this[int index]
        {
            get { return byteTimeStamp[index]; }
            set { byteTimeStamp[index] = value; }
        }

        #region IComparable<RowVersion> Members

        ///<summary>
        ///Compares the current object with another object of the same type.
        ///</summary>
        ///
        ///<returns>
        ///A 32-bit signed integer that indicates the relative order of the objects being compared. The return value has the following meanings: Value Meaning Less than zero This object is less than the other parameter.Zero This object is equal to other. Greater than zero This object is greater than other. 
        ///</returns>
        ///
        ///<param name="other">An object to compare with this object.</param>
        public int CompareTo(RowVersion other)
        {
            int res = 0;

            for (int i = 0; i < 8; i++)
            {
                res = this[i].CompareTo(other[i]);
                if (res != 0) break;
            }

            return res;
        }

        #endregion

        #region IEquatable<RowVersion> Members

        ///<summary>
        ///Indicates whether the current object is equal to another object of the same type.
        ///</summary>
        ///
        ///<returns>
        ///true if the current object is equal to the other parameter; otherwise, false.
        ///</returns>
        ///
        ///<param name="other">An object to compare with this object.</param>
        public bool Equals(RowVersion other)
        {
            if (ReferenceEquals(this, other))
            {
                return true;
            }

            // If one is null, but not both, return false.
            if (other == null)
            {
                return false;
            }

            bool equal = true;

            for (int i = 0; i < 8; i++)
            {
                if (this[i] != other[i])
                {
                    equal = false;
                    break;
                }
            }
            return equal;
        }

        #endregion

        /// <summary>
        /// Update this from DB and check for change;
        /// </summary>
        /// <returns> true if RowVersion changed</returns>
        public bool DBisUpdated()
        {
            var _timestamp = new RowVersion(byteTimeStamp);
            GetFromDB();
            return _timestamp != this;
        }

        /// <summary>
        /// Get Timestamp value from DB
        /// </summary>
        public void GetFromDB()
        {
            using (var conn = new SqlConnection(SoodaConfig.GetString("default.ConnectionString")))
            {
                conn.Open();
                var cmd = new SqlCommand(@"SELECT @@DBTS", conn);
                var reader = cmd.ExecuteReader();
                if (reader != null)
                {
                    reader.Read();
                    byteTimeStamp = (byte[]) reader[0];
                    reader.Close();
                }
                conn.Close();
                conn.Dispose();
            }
        }

        public static implicit operator byte[](RowVersion value)
        {
            return value.byteTimeStamp;
        }

        public static implicit operator RowVersion(byte[] value)
        {
            return new RowVersion(value);
        }

        public static bool operator ==(RowVersion a, RowVersion b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (ReferenceEquals(a, null)) return false;
            return a.Equals(b);
        }

        public static bool operator !=(RowVersion a, RowVersion b)
        {
            return !(a == b);
        }


        public override String ToString()
        {
            return byteTimeStamp.ToString();
        }

        public override bool Equals(object obj)
        {
            if (obj is RowVersion)
            {
                return Equals(obj as RowVersion);
            }
            throw new ArgumentException("Сравнение RowVersion с объектом другого типа!");
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public string ToBase64String()
        {
            return Convert.ToBase64String(byteTimeStamp);
        }
    }

    public sealed class RowVersionConverter : TypeConverter
    {
        // Overrides the ConvertTo method of TypeConverter.
        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value,
            Type destinationType)
        {
            if (destinationType == typeof (string))
            {
                var version = value as RowVersion;
                if (version != null) return version.ToString();
            }
            return base.ConvertTo(context, culture, value, destinationType);
        }
    }
}