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
    using System.IO;
    using System.Xml.Serialization;
    using TypedWrappers;

    [XmlInclude(typeof (SoqlPathExpression))]
    [XmlInclude(typeof (SoqlExistsExpression))]
    [XmlInclude(typeof (SoqlPathExpression))]
    [XmlInclude(typeof (SoqlBooleanIsNullExpression))]
    [XmlInclude(typeof (SoqlBooleanExpression))]
    [XmlInclude(typeof (SoqlBooleanRelationalExpression))]
    [XmlInclude(typeof (SoqlLiteralExpression))]
    [XmlInclude(typeof (SoqlBooleanRelationalExpression))]
    [XmlInclude(typeof (SoqlBooleanAndExpression))]
    [XmlInclude(typeof (SoqlBooleanOrExpression))]
    [XmlInclude(typeof (SoqlBinaryExpression))]
    [XmlInclude(typeof (SoqlBooleanNegationExpression))]
    [XmlInclude(typeof (SoqlBooleanInExpression))]
    [XmlInclude(typeof (SoqlParameterLiteralExpression))]
    [XmlInclude(typeof (SoqlBooleanLiteralExpression))]
    [XmlInclude(typeof (SoqlFunctionCallExpression))]
    [XmlInclude(typeof (SoqlAsteriskExpression))]
    [XmlInclude(typeof (SoqlNullLiteral))]
    [XmlInclude(typeof (SoqlContainsExpression))]
    [XmlInclude(typeof (SoqlCountExpression))]
    public abstract class SoqlExpression
    {
        public virtual SoqlExpression Simplify()
        {
            return this;
        }

        // visitor pattern
        public abstract void Accept(ISoqlVisitor visitor);
        public abstract object Evaluate(ISoqlEvaluateContext context);

        public override string ToString()
        {
            StringWriter sw = new StringWriter();
            SoqlPrettyPrinter pp = new SoqlPrettyPrinter(sw);
            pp.IndentOutput = false;
            Accept(pp);
            return sw.ToString();
        }

        public static SoqlExpression Unwrap(SoqlExpression expr)
        {
            if (expr is SoqlTypedWrapperExpression)
                return Unwrap(((SoqlTypedWrapperExpression) expr).InnerExpression);
            if (expr is SoqlBooleanWrapperExpression)
                return Unwrap(((SoqlBooleanWrapperExpression) expr).InnerExpression);
            return expr;
        }
    }
}