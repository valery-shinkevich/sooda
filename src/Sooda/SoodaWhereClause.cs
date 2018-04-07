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
    using System.Collections.Generic;
    using QL;

    public class SoodaWhereClause
    {
        private SoqlBooleanExpression whereExpression;
        private object[] parameters;

        public SoodaWhereClause() : this((string) null, null)
        {
        }

        public SoodaWhereClause(string whereText) : this(whereText, null)
        {
        }

        public SoodaWhereClause(string whereText, params object[] par)
        {
            Parameters = par;
            if (whereText != null)
            {
                WhereExpression = SoqlParser.ParseWhereClause(whereText);
            }
            else
            {
                WhereExpression = null;
            }
        }

        public SoodaWhereClause(SoqlBooleanExpression whereExpression)
        {
            WhereExpression = whereExpression;
        }

        public SoodaWhereClause(SoqlBooleanExpression whereExpression, params object[] par)
        {
            Parameters = par;
            WhereExpression = whereExpression;
        }

        public SoqlBooleanExpression WhereExpression
        {
            get { return whereExpression; }
            set { whereExpression = value; }
        }

        public object[] Parameters
        {
            get { return parameters; }
            set
            {
                if (value != null && value.Length != 0)
                {
                    parameters = value;
                }
                else
                {
                    parameters = null;
                }
            }
        }

        public SoodaWhereClause Append(SoodaWhereClause other)
        {
            if (other.WhereExpression == null)
                return this;
            if (WhereExpression == null)
                return other;

            object[] newParams = Parameters;

            if (Parameters == null)
                newParams = other.Parameters;
            else if (other.Parameters != null) //wash{
            {
                List<object> objlist = new List<object>(newParams);
                foreach (object o in other.Parameters)
                {
                    objlist.Add(o);
                }
                newParams = objlist.ToArray();
                //throw new SoodaException("You cannot merge two where clauses when they both have parameters");
            } //}wash
            return new SoodaWhereClause(new SoqlBooleanAndExpression(
                WhereExpression, other.WhereExpression), newParams);
        }

        public static SoodaWhereClause operator +(SoodaWhereClause where1, SoodaWhereClause where2)
        {
            return where1.Append(where2);
        }

        public bool Matches(SoodaObject obj, bool throwOnUnknown)
        {
            if (WhereExpression == null)
                return true;

            EvaluateContext context = new EvaluateContext(this, obj);
            object val = WhereExpression.Evaluate(context);
            //if (val == null && throwOnUnknown)
            // throw new SoqlException("Cannot evaluate expression '" + this.whereExpression.ToString() + " ' in memory.");

            if (val is bool)
                return (bool) val;
            return false;
        }

        public override string ToString()
        {
            return (WhereExpression != null) ? WhereExpression.ToString() : "";
        }

        public static readonly SoodaWhereClause Unrestricted = new SoodaWhereClause((string) null);

        private class EvaluateContext : ISoqlEvaluateContext
        {
            private SoodaWhereClause _whereClause;
            private SoodaObject _rootObject;

            public EvaluateContext(SoodaWhereClause whereClause, SoodaObject rootObject)
            {
                _whereClause = whereClause;
                _rootObject = rootObject;
            }

            public object GetRootObject()
            {
                return _rootObject;
            }

            public object GetParameter(int position)
            {
                return _whereClause.Parameters[position];
            }
        }
    }
}