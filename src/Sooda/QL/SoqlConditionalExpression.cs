// 
// Copyright (c) 2014 Piotr Fusik <piotr@fusik.info>
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
    public class SoqlConditionalExpression : SoqlExpression
    {
        public readonly SoqlBooleanExpression condition;
        public readonly SoqlExpression ifTrue;
        public readonly SoqlExpression ifFalse;

        public SoqlConditionalExpression(SoqlBooleanExpression condition, SoqlExpression ifTrue, SoqlExpression ifFalse)
        {
            this.condition = condition;
            this.ifTrue = ifTrue;
            this.ifFalse = ifFalse;
        }

        // visitor pattern
        public override void Accept(ISoqlVisitor visitor)
        {
            visitor.Visit(this);
        }

        public override object Evaluate(ISoqlEvaluateContext context)
        {
            object val = condition.Evaluate(context);

            if (val == null) return null;

            SoqlExpression expr = (bool) val ? ifTrue : ifFalse;
            return expr.Evaluate(context);
        }
    }
}