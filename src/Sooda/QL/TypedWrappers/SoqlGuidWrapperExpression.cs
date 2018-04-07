// 
// Copyright (c) 2002-2006 Jaroslaw Kowalski <jaak@jkowalski.net>
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
// * Neither the name of Jaroslaw Kowalski nor the names of its 
//   contributors may be used to endorse or promote products derived from this
//   software without specific prior written permission. 
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

// automatically generated by makewrappers.pl - do not modify

namespace Sooda.QL.TypedWrappers
{
    using System;

    public class SoqlGuidWrapperExpression : SoqlTypedWrapperExpression
    {
        public SoqlGuidWrapperExpression()
        {
        }

        public SoqlGuidWrapperExpression(SoqlExpression innerExpression) : base(innerExpression)
        {
        }

        public static implicit operator SoqlGuidWrapperExpression(Guid v)
        {
            return new SoqlGuidWrapperExpression(new SoqlLiteralExpression(v));
        }

        public static implicit operator SoqlGuidWrapperExpression(SoqlParameterLiteralExpression v)
        {
            return new SoqlGuidWrapperExpression(v);
        }

        public static SoqlBooleanExpression operator ==(SoqlGuidWrapperExpression left, SoqlGuidWrapperExpression right)
        {
            return new SoqlBooleanRelationalExpression(left, right, SoqlRelationalOperator.Equal);
        }

        public static SoqlBooleanExpression operator !=(SoqlGuidWrapperExpression left, SoqlGuidWrapperExpression right)
        {
            return new SoqlBooleanRelationalExpression(left, right, SoqlRelationalOperator.NotEqual);
        }

        public SoqlBooleanExpression In(params SoqlGuidWrapperExpression[] inExpressions)
        {
            SoqlExpressionCollection rhs = new SoqlExpressionCollection();
            foreach (SoqlGuidWrapperExpression e in inExpressions)
            {
                rhs.Add(e);
            }
            return new SoqlBooleanInExpression(this, rhs);
        }

        public SoqlBooleanExpression In(params Guid[] inExpressions)
        {
            SoqlExpressionCollection rhs = new SoqlExpressionCollection();
            foreach (Guid e in inExpressions)
            {
                rhs.Add(new SoqlLiteralExpression(e.ToString()));
            }
            return new SoqlBooleanInExpression(this, rhs);
        }

        public SoqlBooleanExpression In(string subQuery)
        {
            return new SoqlBooleanInExpression(this, subQuery);
        }

        public SoqlBooleanExpression In(SoqlRawExpression raw)
        {
            return new SoqlBooleanInExpression(this, raw);
        }

        public override bool Equals(object o)
        {
            return ReferenceEquals(this, o);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}