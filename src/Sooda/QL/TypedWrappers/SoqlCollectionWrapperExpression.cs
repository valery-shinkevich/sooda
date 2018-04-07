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

namespace Sooda.QL.TypedWrappers
{
    public class SoqlCollectionWrapperExpression
    {
        private SoqlPathExpression _left;
        private string _collectionName;

        public SoqlCollectionWrapperExpression(SoqlPathExpression left, string collectionName)
        {
            _left = left;
            _collectionName = collectionName;
        }

        public SoqlInt32WrapperExpression Count
        {
            get { return new SoqlInt32WrapperExpression(new SoqlCountExpression(_left, _collectionName)); }
        }

        protected SoqlBooleanWrapperExpression ContainsImpl(SoqlExpression expr)
        {
            return new SoqlBooleanWrapperExpression(new SoqlContainsExpression(_left, _collectionName, expr));
        }

        protected SoqlBooleanWrapperExpression ContainsImpl(SoodaObject obj)
        {
            return
                new SoqlBooleanWrapperExpression(new SoqlContainsExpression(_left, _collectionName,
                    new SoqlLiteralExpression(obj == null ? null : obj.GetPrimaryKeyValue())));
        }

        protected SoqlBooleanWrapperExpression ContainsExprImpl(string fromClass, SoqlBooleanExpression expr)
        {
            SoqlQueryExpression query = new SoqlQueryExpression();
            query.From.Add(fromClass);
            query.FromAliases.Add("");
            query.WhereClause = expr;

            return new SoqlBooleanWrapperExpression(new SoqlContainsExpression(_left, _collectionName, query));
        }
    }
}