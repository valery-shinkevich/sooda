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
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using System.Xml.Serialization;
    using Schema;
    using FieldInfo = Schema.FieldInfo;

    public class SoqlPathExpression : SoqlExpression, ISoqlSelectAliasProvider
    {
        public SoqlPathExpression Left;

        [XmlIgnore] public SoqlPathExpression Next;

        [XmlAttribute("property")] public string PropertyName;

        //     private System.Reflection.PropertyInfo _propInfoCache = null;

        public SoqlPathExpression()
        {
        }

        public SoqlPathExpression(string propertyName)
        {
            PropertyName = propertyName;
        }

        public SoqlPathExpression(string p1, string p2)
        {
            Left = new SoqlPathExpression(p1);
            PropertyName = p2;
        }

        public SoqlPathExpression(string p1, string p2, string p3)
        {
            Left = new SoqlPathExpression(p1, p2);
            PropertyName = p3;
        }

        public SoqlPathExpression(string[] parts)
        {
            SoqlPathExpression l = null;

            for (int i = 0; i < parts.Length - 1; ++i)
            {
                l = new SoqlPathExpression(l, parts[i]);
            }
            PropertyName = parts[parts.Length - 1];
            Left = l;
        }

        public SoqlPathExpression(SoqlPathExpression left, string propertyName)
        {
            Left = left;
            PropertyName = propertyName;
        }

        public void WriteDefaultSelectAlias(TextWriter output)
        {
            if (Left != null)
            {
                Left.WriteDefaultSelectAlias(output);
                output.Write('_');
            }
            output.Write(PropertyName);
        }

        public override void Accept(ISoqlVisitor visitor)
        {
            visitor.Visit(this);
        }

        internal ClassInfo GetAndAddClassInfo(IFieldContainer rootClass, List<IFieldContainer> result)
        {
            IFieldContainer leftClass;

            if (Left == null)
            {
                leftClass = rootClass;
                if (!result.Contains(rootClass))
                    result.Add(rootClass);
            }
            else
            {
                leftClass = Left.GetAndAddClassInfo(rootClass, result);
            }

            FieldInfo field = leftClass.FindFieldByName(PropertyName);
            if (field == null)
                throw new Exception("Field " + PropertyName + " not found in " + leftClass.Name);

            if (field.ReferencedClass != null)
            {
                if (!result.Contains(field.ReferencedClass))
                    result.Add(field.ReferencedClass);
            }
            return field.ReferencedClass;
        }

        public override object Evaluate(ISoqlEvaluateContext context)
        {
            object val;

            if (Left == null)
            {
                val = context.GetRootObject();
            }
            else
            {
                val = Left.Evaluate(context);
            }

            if (val == null)
                return null;

            //if (_propInfoCache == null)
            //{
            //    _propInfoCache = val.GetType().GetProperty(PropertyName);
            //    if (_propInfoCache == null)
            //        throw new SoodaException(PropertyName + " not found in " + val.GetType().Name);
            //}

            //try
            //{
            //    return _propInfoCache.GetValue(val, null);
            //}
            //catch (Exception tex)
            //{
            //    Console.WriteLine(tex);
            return val.GetType().InvokeMember(PropertyName, BindingFlags.GetProperty, null, val, null);
            //}
        }
    }
}