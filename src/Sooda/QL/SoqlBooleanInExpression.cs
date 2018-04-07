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
    using System.Collections;
    using System.Xml.Serialization;
    using TypedWrappers;

    public class SoqlBooleanInExpression : SoqlBooleanExpression
    {
        public SoqlExpression Left;

        [XmlArray("in")] [XmlArrayItem("item")] public SoqlExpressionCollection Right;

        public SoqlBooleanInExpression()
        {
        }

        public SoqlBooleanInExpression(SoqlExpression lhs, SoqlExpressionCollection rhs)
        {
            Left = lhs;
            Right = rhs;
        }

        public SoqlBooleanInExpression(SoqlExpression lhs, IEnumerable rhs)
        {
            Left = lhs;
            Right = new SoqlExpressionCollection();
            foreach (object expr in rhs)
            {
                if (expr is SoqlExpression)
                {
                    Right.Add((SoqlExpression) expr);
                    continue;
                }

                if (expr is SoodaObject)
                {
                    Right.Add(new SoqlLiteralExpression(((SoodaObject) expr).GetPrimaryKeyValue()));
                    continue;
                }

                Right.Add(new SoqlLiteralExpression(expr));
            }
        }

        public SoqlBooleanInExpression(SoqlExpression lhs, string subQuery)
        {
            Left = lhs;
            Right = new SoqlExpressionCollection {new SoqlStringWrapperExpression(new SoqlRawExpression(subQuery))};
        }

        public SoqlBooleanInExpression(SoqlExpression lhs, SoqlRawExpression subQuery)
        {
            Left = lhs;
            Right = new SoqlExpressionCollection {new SoqlStringWrapperExpression(subQuery)};
        }

        // visitor pattern
        public override void Accept(ISoqlVisitor visitor)
        {
            visitor.Visit(this);
        }

        public override SoqlExpression Simplify()
        {
            if (Right.Count == 0)
                return SoqlBooleanLiteralExpression.False;

            SoqlExpression lhsExpression = Left.Simplify();
            SoqlBooleanExpression retVal = null;

            for (int i = 0; i < Right.Count; ++i)
            {
                var e = Right[i];
                var bre =
                    new SoqlBooleanRelationalExpression(lhsExpression, e, SoqlRelationalOperator.Equal);

                if (retVal == null)
                    retVal = bre;
                else
                    retVal = new SoqlBooleanOrExpression(retVal, bre);
            }

            return retVal.Simplify();
        }

        public override object Evaluate(ISoqlEvaluateContext context)
        {
            // this should be evaluated on 
            return Simplify().Evaluate(context);
        }
    }
}