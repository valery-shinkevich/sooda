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

namespace Sooda.QL
{
    using System;
    using System.Collections.Specialized;

    public class SoqlQueryExpression : SoqlExpression
    {
        public bool Distinct = false;

        public int StartIdx = 0;
        public int PageCount = -1;

        //public int LimitBegin = -1;
        //public int LimitEnd = -1;

        public readonly SoqlExpressionCollection SelectExpressions = new SoqlExpressionCollection();
        public readonly StringCollection SelectAliases = new StringCollection();

        public readonly StringCollection From = new StringCollection();
        public readonly StringCollection FromAliases = new StringCollection();

        public SoqlBooleanExpression WhereClause = null;
        public SoqlBooleanExpression Having = null;

        public readonly SoqlExpressionCollection GroupByExpressions = new SoqlExpressionCollection();
        public readonly SoqlExpressionCollection OrderByExpressions = new SoqlExpressionCollection();
        public readonly StringCollection OrderByOrder = new StringCollection();

        // visitor pattern
        public override void Accept(ISoqlVisitor visitor)
        {
            visitor.Visit(this);
        }

        public override object Evaluate(ISoqlEvaluateContext context)
        {
            throw new NotImplementedException();
        }

        public void SetOrderBy(SoodaOrderBy orderBy)
        {
            OrderByExpressions.AddRange(orderBy.OrderByExpressions);
            foreach (SortOrder so in orderBy.SortOrders)
            {
                if (so == SortOrder.Ascending)
                    OrderByOrder.Add("asc");
                else
                    OrderByOrder.Add("desc");
            }
        }
    }
}