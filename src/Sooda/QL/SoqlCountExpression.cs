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
    using System.IO;

    public class SoqlCountExpression : SoqlExpression, ISoqlSelectAliasProvider
    {
        public SoqlPathExpression Path;
        public string CollectionName;

        public SoqlCountExpression()
        {
        }

        public SoqlCountExpression(SoqlPathExpression path, string collectionName)
        {
            Path = path;
            CollectionName = collectionName;
        }

        // visitor pattern
        public override void Accept(ISoqlVisitor visitor)
        {
            visitor.Visit(this);
        }

        public void WriteDefaultSelectAlias(TextWriter output)
        {
            if (Path != null)
            {
                Path.WriteDefaultSelectAlias(output);
                output.Write('_');
            }
            output.Write(CollectionName);
            output.Write("_Count");
        }

        public override object Evaluate(ISoqlEvaluateContext context)
        {
            object val;

            if (Path != null)
            {
                val = Path.Evaluate(context);
            }
            else
            {
                val = context.GetRootObject();
            }
            if (val == null)
                return null;

            IList sol = (IList) val.GetType().GetProperty(CollectionName).GetValue(val, null);
            return sol.Count;
        }
    }
}