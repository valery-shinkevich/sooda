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
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using QL;

    public class SoodaObjectExpressionComparer : IComparer
    {
        private class ExpressionCompareInfo
        {
            public ExpressionCompareInfo(SoqlExpression expression, SortOrder sortOrder)
            {
                Expression = expression;
                SortOrder = sortOrder;
            }

            public readonly SoqlExpression Expression;
            public readonly SortOrder SortOrder;
        }

        private class EvaluateContext : ISoqlEvaluateContext
        {
            private SoodaObject _rootObject;

            public EvaluateContext()
            {
                _rootObject = null;
            }

            public object GetRootObject()
            {
                return _rootObject;
            }

            public void SetRootObject(SoodaObject o)
            {
                _rootObject = o;
            }

            public object GetParameter(int position)
            {
                throw new Exception("No parameters are allowed in expression comparer.");
            }
        }

        private readonly List<ExpressionCompareInfo> _expressions = new List<ExpressionCompareInfo>();
        private readonly EvaluateContext _context1 = new EvaluateContext();
        private readonly EvaluateContext _context2 = new EvaluateContext();

        int IComparer.Compare(object o1, object o2)
        {
            var dbo1 = o1 as SoodaObject;
            var dbo2 = o2 as SoodaObject;

            return Compare(dbo1, dbo2);
        }

        public void AddExpression(SoqlExpression expression, SortOrder sortOrder)
        {
            _expressions.Add(new ExpressionCompareInfo(expression, sortOrder));
        }

        public void AddExpressions(SoodaObjectExpressionComparer other)
        {
            _expressions.AddRange(other._expressions);
        }

        public int Compare(SoodaObject dbo1, SoodaObject dbo2)
        {
            _context1.SetRootObject(dbo1);
            _context2.SetRootObject(dbo2);

            foreach (ExpressionCompareInfo eci in _expressions)
            {
                object v1 = eci.Expression.Evaluate(_context1);
                object v2 = eci.Expression.Evaluate(_context2);

                int result = DoCompare(v1, v2);
                if (result != 0)
                {
                    return eci.SortOrder == SortOrder.Ascending ? result : -result;
                }
            }

            return PrimaryKeyCompare(dbo1, dbo2);
        }

        private static int DoCompare(object v1, object v2)
        {
            if (v1 == null)
            {
                return v2 == null ? 0 : -1;
            }

            if (v2 == null)
            {
                return 1; // not null is greater than anything
            }

            return ((IComparable) v1).CompareTo(v2);
        }

        private static int PrimaryKeyCompare(SoodaObject dbo1, SoodaObject dbo2)
        {
            return ((IComparable) dbo1.GetPrimaryKeyValue()).CompareTo(dbo2.GetPrimaryKeyValue());
        }


        public SoqlExpression[] OrderByExpressions
        {
            get { return _expressions.Select(eci => eci.Expression).ToArray(); }
        }

        public SortOrder[] SortOrders
        {
            get { return _expressions.Select(eci => eci.SortOrder).ToArray(); }
        }
    }
}