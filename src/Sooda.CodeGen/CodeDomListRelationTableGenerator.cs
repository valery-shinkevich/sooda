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

namespace Sooda.CodeGen
{
    using System;
    using System.CodeDom;
    using System.Xml;
    using Schema;

    public class CodeDomListRelationTableGenerator : CodeDomHelpers
    {
        private readonly RelationInfo _relationInfo;

        public CodeDomListRelationTableGenerator(RelationInfo ri)
        {
            _relationInfo = ri;
        }

        public CodeConstructor Constructor_1()
        {
            var ctor = new CodeConstructor
            {
                Attributes = MemberAttributes.Public
            };
            ctor.BaseConstructorArgs.Add(new CodePrimitiveExpression(_relationInfo.Table.DBTableName));
            ctor.BaseConstructorArgs.Add(new CodePrimitiveExpression(_relationInfo.Table.Fields[0].DBColumnName));
            ctor.BaseConstructorArgs.Add(new CodePrimitiveExpression(_relationInfo.Table.Fields[1].DBColumnName));
            ctor.BaseConstructorArgs.Add(new CodeFieldReferenceExpression(null, "theRelationInfo"));

            return ctor;
        }

        public CodeMemberMethod Method_DeserializeTupleLeft()
        {
            // public virtual public CLASS_NAMEList GetSnapshot() { return new CLASS_NAMEListSnapshot(this, 0, Length); }
            var method = new CodeMemberMethod
            {
                Name = "DeserializeTupleLeft",
                ReturnType = new CodeTypeReference(typeof (Object))
            };
            method.Parameters.Add(new CodeParameterDeclarationExpression(typeof (XmlReader), "reader"));
            method.Attributes = MemberAttributes.Family | MemberAttributes.Override;

            var typeWrapper = _relationInfo.Table.Fields[0].GetNullableFieldHandler().GetType().FullName;

            method.Statements.Add(
                new CodeMethodReturnStatement(
                    new CodeMethodInvokeExpression(
                        new CodeTypeReferenceExpression(typeWrapper),
                        "DeserializeFromString", new CodeMethodInvokeExpression(
                            new CodeArgumentReferenceExpression("reader"),
                            "GetAttribute",
                            new CodePrimitiveExpression("r1")))));
            return method;
        }

        public CodeMemberMethod Method_DeserializeTupleRight()
        {
            var method = new CodeMemberMethod
            {
                Name = "DeserializeTupleRight",
                ReturnType = new CodeTypeReference(typeof (Object))
            };
            method.Parameters.Add(new CodeParameterDeclarationExpression(typeof (XmlReader), "reader"));
            method.Attributes = MemberAttributes.Family | MemberAttributes.Override;

            var typeWrapper = _relationInfo.Table.Fields[1].GetNullableFieldHandler().GetType().FullName;

            method.Statements.Add(
                new CodeMethodReturnStatement(
                    new CodeMethodInvokeExpression(
                        new CodeTypeReferenceExpression(typeWrapper),
                        "DeserializeFromString", new CodeMethodInvokeExpression(
                            new CodeArgumentReferenceExpression("reader"),
                            "GetAttribute",
                            new CodePrimitiveExpression("r2")))));
            return method;
        }
    }
}