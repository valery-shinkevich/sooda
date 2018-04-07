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
    using System.Linq;
    using Linq;
    using ObjectMapper;
    using QL;
    using Schema;

    public class CodeDomClassStubGenerator : CodeDomHelpers
    {
        private readonly ClassInfo _classInfo;
        private readonly SoodaProject _options;
        public readonly string KeyGen;

        public CodeDomClassStubGenerator(ClassInfo ci, SoodaProject options)
        {
            _classInfo = ci;
            _options = options;
            var keyGen = "none";

            if (!ci.ReadOnly && ci.GetPrimaryKeyFields().Length == 1)
            {
                switch (ci.GetFirstPrimaryKeyField().DataType)
                {
                    case FieldDataType.Integer:
                        keyGen = "integer";
                        break;

                    case FieldDataType.Guid:
                        keyGen = "guid";
                        break;

                    case FieldDataType.Long:
                        keyGen = "long";
                        break;
                }
            }

            if (ci.KeyGenName != null)
                keyGen = ci.KeyGenName;

            KeyGen = keyGen;
        }

        private ClassInfo GetRootClass(ClassInfo ci)
        {
            return ci.InheritsFromClass != null ? GetRootClass(ci.InheritsFromClass) : ci;
        }

        public CodeMemberField Field_keyGenerator()
        {
            var field = new CodeMemberField("IPrimaryKeyGenerator", "keyGenerator")
            {
                Attributes = MemberAttributes.Private | MemberAttributes.Static
            };

            switch (KeyGen)
            {
                case "guid":
                    field.InitExpression =
                        new CodeObjectCreateExpression("Sooda.ObjectMapper.KeyGenerators.GuidGenerator");
                    break;

                case "integer":
                    field.InitExpression =
                        new CodeObjectCreateExpression("Sooda.ObjectMapper.KeyGenerators.TableBasedGenerator",
                            new CodePrimitiveExpression(GetRootClass(_classInfo).Name),
                            new CodeMethodInvokeExpression(
                                new CodeMethodInvokeExpression(
                                    new CodeTypeReferenceExpression(_options.OutputNamespace.Replace(".", "") + "." +
                                                                    "_DatabaseSchema"), "GetSchema"),
                                "GetDataSourceInfo",
                                new CodePrimitiveExpression(_classInfo.GetSafeDataSourceName())));
                    break;
                case "long":
                    field.InitExpression =
                        new CodeObjectCreateExpression("Sooda.ObjectMapper.KeyGenerators.TableBasedGeneratorBigint",
                            new CodePrimitiveExpression(GetRootClass(_classInfo).Name),
                            new CodeMethodInvokeExpression(
                                new CodeMethodInvokeExpression(
                                    new CodeTypeReferenceExpression(_options.OutputNamespace.Replace(".", "") + "." +
                                                                    "_DatabaseSchema"), "GetSchema"),
                                "GetDataSourceInfo",
                                new CodePrimitiveExpression(_classInfo.GetSafeDataSourceName())));
                    break;

                default:
                    field.InitExpression = new CodeObjectCreateExpression(KeyGen);
                    break;
            }
            return field;
        }

        public CodeMemberMethod Method_TriggerFieldUpdate(FieldInfo fi, string methodPrefix)
        {
            var method = new CodeMemberMethod {Name = methodPrefix + "_" + fi.Name};
            if (fi.References != null)
            {
                method.Parameters.Add(new CodeParameterDeclarationExpression(fi.References, "oldValue"));
                method.Parameters.Add(new CodeParameterDeclarationExpression(fi.References, "newValue"));
            }
            else
            {
                method.Parameters.Add(new CodeParameterDeclarationExpression(typeof (object), "oldValue"));
                method.Parameters.Add(new CodeParameterDeclarationExpression(typeof (object), "newValue"));
            }
            method.Attributes = MemberAttributes.Family;
            method.Statements.Add(new CodeMethodInvokeExpression(This, methodPrefix,
                new CodePrimitiveExpression(fi.Name), Arg("oldValue"), Arg("newValue")));
            return method;
        }

        public CodeConstructor Constructor_Raw()
        {
            var ctor = new CodeConstructor {Attributes = MemberAttributes.Public};

            ctor.Parameters.Add(new CodeParameterDeclarationExpression("SoodaConstructor", "c"));
            ctor.BaseConstructorArgs.Add(Arg("c"));

            return ctor;
        }

        public CodeConstructor Constructor_Mini_Inserting()
        {
            var ctor = new CodeConstructor {Attributes = MemberAttributes.Public};

            ctor.Parameters.Add(new CodeParameterDeclarationExpression("SoodaTransaction", "tran"));
            ctor.BaseConstructorArgs.Add(Arg("tran"));

            return ctor;
        }

        public CodeMemberProperty Prop_LiteralValue(string name, object val)
        {
            var prop = new CodeMemberProperty
            {
                Name = name,
                Attributes = MemberAttributes.Static | MemberAttributes.Public,
                Type = new CodeTypeReference(_classInfo.Name)
            };

            prop.GetStatements.Add(
                new CodeMethodReturnStatement(
                    new CodeMethodInvokeExpression(
                        LoaderClass(_classInfo), "GetRef", new CodePrimitiveExpression(val))));

            return prop;
        }

        private CodeTypeReferenceExpression LoaderClass(ClassInfo ci)
        {
            return _options.LoaderClass
                ? new CodeTypeReferenceExpression(ci.Name + "Loader")
                : new CodeTypeReferenceExpression(ci.Name + "_Stub");
        }

        public CodeTypeReference GetReturnType(PrimitiveRepresentation rep, FieldInfo fi)
        {
            switch (rep)
            {
                case PrimitiveRepresentation.Boxed:
                    return new CodeTypeReference(typeof (object));

                case PrimitiveRepresentation.SqlType:
                    var t = fi.GetNullableFieldHandler().GetSqlType();
                    return t == null
                        ? new CodeTypeReference(fi.GetNullableFieldHandler().GetFieldType())
                        : new CodeTypeReference(t);

                case PrimitiveRepresentation.RawWithIsNull:
                case PrimitiveRepresentation.Raw:
                    return new CodeTypeReference(fi.GetNullableFieldHandler().GetFieldType());

                case PrimitiveRepresentation.Nullable:
                    return new CodeTypeReference(fi.GetNullableFieldHandler().GetNullableType());

                default:
                    throw new NotImplementedException("Unknown PrimitiveRepresentation: " + rep);
            }
        }

        private static CodeExpression GetFieldValueExpression(FieldInfo fi)
        {
            return new CodeMethodInvokeExpression(
                new CodeTypeReferenceExpression(typeof (SoodaObjectImpl)),
                "GetBoxedFieldValue",
                new CodeThisReferenceExpression(),
                new CodePrimitiveExpression(fi.Table.OrdinalInClass),
                new CodePrimitiveExpression(fi.ClassUnifiedOrdinal));
        }

        private static CodeExpression GetFieldIsNullExpression(FieldInfo fi)
        {
            return new CodeMethodInvokeExpression(
                new CodeTypeReferenceExpression(typeof (SoodaObjectImpl)),
                "IsFieldNull",
                new CodeThisReferenceExpression(),
                new CodePrimitiveExpression(fi.Table.OrdinalInClass),
                new CodePrimitiveExpression(fi.ClassUnifiedOrdinal)
                );
        }

        private static CodeMemberProperty _IsNull(FieldInfo fi)
        {
            var prop = new CodeMemberProperty
            {
                Name = fi.Name + "_IsNull",
                Attributes = MemberAttributes.Final | MemberAttributes.Public,
                Type = new CodeTypeReference(typeof (bool))
            };

            prop.GetStatements.Add(
                new CodeMethodReturnStatement(
                    GetFieldIsNullExpression(fi)));

            return prop;
        }

        private static CodeExpression Box(CodeExpression expr)
        {
            return new CodeMethodInvokeExpression(new CodeTypeReferenceExpression(typeof (SoodaNullable)), "Box", expr);
        }

        private static CodeMemberMethod _SetNull(FieldInfo fi)
        {
            var method = new CodeMemberMethod
            {
                Name = "_SetNull_" + fi.Name,
                Attributes = MemberAttributes.Final | MemberAttributes.Public
            };

            method.Statements.Add(
                new CodeAssignStatement(
                    GetFieldValueExpression(fi), new CodePrimitiveExpression(null)));

            return method;
        }

        private static CodeExpression GetTransaction()
        {
            return new CodeMethodInvokeExpression(This, "GetTransaction");
        }

        private CodeExpression GetFieldValueForRead(FieldInfo fi)
        {
//wash{
            if (!string.IsNullOrEmpty(fi.Enum))
            {
                return new CodeCastExpression(fi.IsNullable ? fi.Enum + "?" : fi.Enum, new CodeFieldReferenceExpression(
                    new CodeMethodInvokeExpression(new CodeThisReferenceExpression(),
                        "Get" + fi.ParentClass.Name + "FieldValuesForRead",
                        new CodePrimitiveExpression(fi.Table.OrdinalInClass)), fi.Name));
            }
//}wash
            CodeExpression fieldValues = new CodeMethodInvokeExpression(
                new CodeThisReferenceExpression(),
                "Get" + fi.ParentClass.Name + "FieldValuesForRead",
                new CodePrimitiveExpression(fi.Table.OrdinalInClass));
            if (fi.ParentClass.GetDataSource().EnableDynamicFields)
            {
                return new CodeMethodInvokeExpression(
                    fieldValues,
                    "GetBoxedFieldValue",
                    new CodePrimitiveExpression(fi.ClassUnifiedOrdinal));
            }
            return new CodeFieldReferenceExpression(fieldValues, fi.Name);
        }

        private static int GetFieldRefCacheIndex(ClassInfo ci, FieldInfo fi0)
        {
            int p = 0;

            foreach (FieldInfo fi in ci.LocalFields)
            {
                if (fi == fi0)
                    return p;
                if (fi.ReferencedClass != null)
                    p++;
            }

            return -1;
        }

        private static int GetFieldRefCacheCount(ClassInfo ci)
        {
            return ci.LocalFields.Count(fi => fi.ReferencedClass != null);
        }

        private static CodeExpression RefCacheArray()
        {
            return new CodeFieldReferenceExpression(new CodeThisReferenceExpression(), "_refcache");
        }

        private static CodeExpression RefCacheExpression(ClassInfo ci, FieldInfo fi)
        {
            return new CodeArrayIndexerExpression(RefCacheArray(),
                new CodePrimitiveExpression(GetFieldRefCacheIndex(ci, fi)));
        }

        private static CodeExpression Factory(string className)
        {
            return new CodePropertyReferenceExpression(new CodeTypeReferenceExpression(className + "_Factory"),
                "TheFactory");
        }

        private CodeTypeReference GetCollectionPropertyType(string className)
        {
            return _options.WithSoql
                ? new CodeTypeReference(className + "List")
                : new CodeTypeReference("System.Collections.Generic.IList", new CodeTypeReference(className));
        }

        private CodeTypeReference GetCollectionWrapperType(string className)
        {
            return _options.WithSoql
                ? new CodeTypeReference(_options.OutputNamespace + "." + className + "List")
                : new CodeTypeReference("Sooda.ObjectMapper.SoodaObjectCollectionWrapperGeneric",
                    new CodeTypeReference(className));
        }

#if DOTNET35
        private static CodeMemberProperty GetCollectionLinqQuery(CollectionBaseInfo coli, CodeExpression whereExpression)
        {
            var elementType = coli.GetItemClass().Name;
            var prop = new CodeMemberProperty
            {
                Name = coli.Name + "Query",
                Attributes = MemberAttributes.Final | MemberAttributes.Public,
                Type =
                    new CodeTypeReference(new CodeTypeReference(typeof (IQueryable<>)).BaseType,
                        new CodeTypeReference(elementType))
            };

            prop.GetStatements.Add(
                new CodeMethodReturnStatement(
                    new CodeObjectCreateExpression(
                        new CodeTypeReference(new CodeTypeReference(typeof (SoodaQuerySource<>)).BaseType,
                            new CodeTypeReference(elementType)),
                        new CodeMethodInvokeExpression(This, "GetTransaction"),
                        new CodePropertyReferenceExpression(new CodeTypeReferenceExpression(elementType + "_Factory"),
                            "TheClassInfo"),
                        whereExpression
                        )));

            prop.CustomAttributes.Add(new CodeAttributeDeclaration("System.ComponentModel.BrowsableAttribute",
                new CodeAttributeArgument(new CodePrimitiveExpression(false))));

            return prop;
        }
#endif

        public void GenerateProperties(CodeTypeDeclaration ctd, ClassInfo ci)
        {
            CodeMemberProperty prop;

            foreach (FieldInfo fi in _classInfo.LocalFields)
            {
                if (fi.References != null)
                    continue;

                if (fi.IsNullable)
                {
                    if (_options.NullableRepresentation == PrimitiveRepresentation.RawWithIsNull)
                    {
                        ctd.Members.Add(_IsNull(fi));
                        if (!ci.ReadOnly)
                        {
                            ctd.Members.Add(_SetNull(fi));
                        }
                    }
                }
                else
                {
                    if (_options.NotNullRepresentation == PrimitiveRepresentation.RawWithIsNull)
                    {
                        if (!ci.ReadOnly)
                        {
                            // if it's read-only, not-null means not-null and there's no
                            // exception
                            ctd.Members.Add(_IsNull(fi));
                        }
                    }
                }
            }

            int primaryKeyComponentNumber = 0;

            foreach (FieldInfo fi in _classInfo.LocalFields)
            {
                PrimitiveRepresentation actualNullableRepresentation = _options.NullableRepresentation;
                PrimitiveRepresentation actualNotNullRepresentation = _options.NotNullRepresentation;

                if (fi.GetNullableFieldHandler().GetSqlType() == null)
                {
                    if (actualNotNullRepresentation == PrimitiveRepresentation.SqlType)
                        actualNotNullRepresentation = PrimitiveRepresentation.Raw;

                    if (actualNullableRepresentation == PrimitiveRepresentation.SqlType)
                        actualNullableRepresentation = PrimitiveRepresentation.Raw;
                }

                CodeTypeReference returnType;

//wash{
                if (!string.IsNullOrEmpty(fi.Enum) && fi.DataType == FieldDataType.Integer)
                {
                    returnType = new CodeTypeReference(fi.IsNullable ? fi.Enum + "?" : fi.Enum);
                }
                else
                {
//}wash
                    if (fi.References != null)
                    {
                        returnType = new CodeTypeReference(fi.References);
                    }
                    else if (fi.IsNullable)
                    {
                        returnType = GetReturnType(actualNullableRepresentation, fi);
                    }
                    else
                    {
                        returnType = GetReturnType(actualNotNullRepresentation, fi);
                    }
                }

                prop = new CodeMemberProperty();
//wash{
                AddCustomPropAttributes(fi, prop);
//}wash
                prop.Name = fi.Name;
                prop.Attributes = MemberAttributes.Final | MemberAttributes.Public;

                if (fi.Modifier == "new")
                    prop.Attributes = prop.Attributes | MemberAttributes.New;
                if (fi.Modifier == "override")
                    prop.Attributes = prop.Attributes | MemberAttributes.Override;

                prop.Type = returnType;
                //prop.GetStatements.Add(new CodeMethodReturnStatement(new CodeFieldReferenceExpression(null, "_FieldNames")));
                if (!string.IsNullOrEmpty(fi.Description) || !string.IsNullOrEmpty(fi.DisplayName))
                {
                    prop.Comments.Add(new CodeCommentStatement("<summary>", true));
                    if (!string.IsNullOrEmpty(fi.DisplayName))
                        prop.Comments.Add(new CodeCommentStatement(fi.DisplayName, true));
                    if (!string.IsNullOrEmpty(fi.Description))
                        prop.Comments.Add(new CodeCommentStatement(fi.Description, true));
                    prop.Comments.Add(new CodeCommentStatement("</summary>", true));
                }

                ctd.Members.Add(prop);

                if (fi.Size != -1)
                {
                    var cad = new CodeAttributeDeclaration("Sooda.SoodaFieldSizeAttribute");
                    cad.Arguments.Add(new CodeAttributeArgument(new CodePrimitiveExpression(fi.Size)));
                    prop.CustomAttributes.Add(cad);
                }

                if (fi.IsPrimaryKey)
                {
                    CodeExpression getPrimaryKeyValue = new CodeMethodInvokeExpression(
                        new CodeThisReferenceExpression(), "GetPrimaryKeyValue");

                    if (_classInfo.GetPrimaryKeyFields().Length > 1)
                    {
                        getPrimaryKeyValue = new CodeMethodInvokeExpression(
                            new CodeTypeReferenceExpression(typeof (SoodaTuple)), "GetValue", getPrimaryKeyValue,
                            new CodePrimitiveExpression(primaryKeyComponentNumber));
                    }

                    if (fi.References != null)
                    {
                        prop.GetStatements.Add(
                            new CodeMethodReturnStatement(
                                new CodeMethodInvokeExpression(
                                    LoaderClass(fi.ReferencedClass),
                                    "GetRef",
                                    GetTransaction(),
                                    new CodeCastExpression(
                                        GetReturnType(actualNotNullRepresentation, fi),
                                        getPrimaryKeyValue
                                        ))));
                    }
                    else
                    {
                        prop.GetStatements.Add(
                            new CodeMethodReturnStatement(
                                new CodeCastExpression(
                                    prop.Type,
                                    getPrimaryKeyValue
                                    )));
                    }

                    if (!_classInfo.ReadOnly)
                    {
                        if (_classInfo.GetPrimaryKeyFields().Length == 1)
                        {
                            prop.SetStatements.Add(
                                new CodeExpressionStatement(
                                    new CodeMethodInvokeExpression(
                                        new CodeThisReferenceExpression(), "SetPrimaryKeyValue",
                                        new CodePropertySetValueReferenceExpression())));
                        }
                        else
                        {
                            CodeExpression plainValue = new CodePropertySetValueReferenceExpression();
                            if (fi.References != null)
                                plainValue = new CodeMethodInvokeExpression(plainValue, "GetPrimaryKeyValue");

                            prop.SetStatements.Add(
                                new CodeExpressionStatement(
                                    new CodeMethodInvokeExpression(
                                        new CodeThisReferenceExpression(),
                                        "SetPrimaryKeySubValue",
                                        plainValue,
                                        //new CodePropertySetValueReferenceExpression(),
                                        new CodePrimitiveExpression(primaryKeyComponentNumber),
                                        new CodePrimitiveExpression(_classInfo.GetPrimaryKeyFields().Length))));
                        }
                    }
                    primaryKeyComponentNumber++;
                    continue;
                }

                if (_options.NullPropagation && (fi.References != null || fi.IsNullable) &&
                    actualNullableRepresentation != PrimitiveRepresentation.Raw)
                {
                    CodeExpression retVal = new CodePrimitiveExpression(null);

                    if (fi.References == null && actualNullableRepresentation == PrimitiveRepresentation.SqlType)
                    {
                        retVal = new CodePropertyReferenceExpression(
                            new CodeTypeReferenceExpression(fi.GetNullableFieldHandler().GetSqlType()), "Null");
                    }

                    prop.GetStatements.Add(
                        new CodeConditionStatement(
                            new CodeBinaryOperatorExpression(
                                new CodeThisReferenceExpression(),
                                CodeBinaryOperatorType.IdentityEquality,
                                new CodePrimitiveExpression(null)),
                            new CodeStatement[]
                            {
                                new CodeMethodReturnStatement(retVal)
                            },
                            new CodeStatement[]
                            {
                            }));
                }

                if (fi.References != null)
                {
                    // reference field getter
                    //
                    CodeExpression pk = new CodeVariableReferenceExpression("pk");
                    Type pkType;
                    CodeExpression isFieldNotNull;
                    CodeExpression getRef;
                    if (fi.ParentClass.GetDataSource().EnableDynamicFields)
                    {
                        pkType = typeof (object);
                        isFieldNotNull = new CodeBinaryOperatorExpression(
                            pk,
                            CodeBinaryOperatorType.IdentityInequality,
                            new CodePrimitiveExpression(null));
                        getRef = new CodeMethodInvokeExpression(
                            new CodeTypeReferenceExpression(typeof (SoodaObject)),
                            "GetRefHelper",
                            GetTransaction(),
                            Factory(fi.References),
                            pk);
                    }
                    else
                    {
                        pkType = fi.GetNullableFieldHandler().GetSqlType();
                        isFieldNotNull = new CodeBinaryOperatorExpression(
                            new CodePropertyReferenceExpression(pk, "IsNull"),
                            CodeBinaryOperatorType.ValueEquality,
                            new CodePrimitiveExpression(false));
                        getRef = new CodeMethodInvokeExpression(
                            LoaderClass(fi.ReferencedClass),
                            "GetRef",
                            GetTransaction(),
                            new CodePropertyReferenceExpression(pk, "Value"));
                    }

                    prop.GetStatements.Add(
                        new CodeConditionStatement(
                            new CodeBinaryOperatorExpression(
                                RefCacheExpression(ci, fi),
                                CodeBinaryOperatorType.IdentityEquality,
                                new CodePrimitiveExpression(null)),
                            new CodeVariableDeclarationStatement(pkType, "pk", GetFieldValueForRead(fi)),
                            new CodeConditionStatement(
                                isFieldNotNull, new CodeAssignStatement(RefCacheExpression(ci, fi), getRef))));

                    prop.GetStatements.Add(
                        new CodeMethodReturnStatement(
                            new CodeCastExpression(returnType,
                                RefCacheExpression(ci, fi))));


                    // reference field setter
                    if (!_classInfo.ReadOnly)
                    {
                        prop.SetStatements.Add(
                            new CodeExpressionStatement(
                                new CodeMethodInvokeExpression(
                                    new CodeTypeReferenceExpression(typeof (SoodaObjectImpl)),
                                    "SetRefFieldValue",

                                    // parameters
                                    new CodeThisReferenceExpression(),
                                    new CodePrimitiveExpression(fi.Table.OrdinalInClass),
                                    new CodePrimitiveExpression(fi.Name),
                                    new CodePrimitiveExpression(fi.ClassUnifiedOrdinal),
                                    new CodePropertySetValueReferenceExpression(),
                                    RefCacheArray(),
                                    new CodePrimitiveExpression(GetFieldRefCacheIndex(ci, fi)),
                                    Factory(returnType.BaseType)
                                    )));
                    }
                }
                else
                {
                    // plain field getter
                    prop.GetStatements.Clear();

                    CodeExpression fieldValue = GetFieldValueForRead(fi);
                    if (fi.ParentClass.GetDataSource().EnableDynamicFields)
                    {
                        switch (fi.IsNullable ? actualNullableRepresentation : actualNotNullRepresentation)
                        {
                            case PrimitiveRepresentation.Boxed:
                                break;

                            case PrimitiveRepresentation.SqlType:
                            case PrimitiveRepresentation.RawWithIsNull:
                            case PrimitiveRepresentation.Raw:
                                fieldValue =
                                    new CodeCastExpression(
                                        new CodeTypeReference(fi.GetNullableFieldHandler().GetFieldType()),
                                        fieldValue);
                                break;

                            case PrimitiveRepresentation.Nullable:
                                fieldValue =
                                    new CodeCastExpression(
                                        new CodeTypeReference(fi.GetNullableFieldHandler().GetNullableType()),
                                        fieldValue);
                                break;

                            default:
                                throw new NotImplementedException("Unknown PrimitiveRepresentation");
                        }
                    }
                    prop.GetStatements.Add(new CodeMethodReturnStatement(fieldValue));

                    if (!_classInfo.ReadOnly && !fi.ReadOnly)
                    {
                        //plain field setter

                        CodeExpression beforeDelegate = new CodePrimitiveExpression(null);
                        CodeExpression afterDelegate = new CodePrimitiveExpression(null);

                        if (_classInfo.Triggers)
                        {
                            beforeDelegate =
                                new CodeDelegateCreateExpression(
                                    new CodeTypeReference(typeof (SoodaFieldUpdateDelegate)),
                                    new CodeThisReferenceExpression(), "BeforeFieldUpdate_" + fi.Name);
                            afterDelegate =
                                new CodeDelegateCreateExpression(
                                    new CodeTypeReference(typeof (SoodaFieldUpdateDelegate)),
                                    new CodeThisReferenceExpression(), "AfterFieldUpdate_" + fi.Name);
                        }
//wash{
                        if (string.IsNullOrEmpty(fi.Enum))
                        {
//}wash
                            prop.SetStatements.Add(
                                new CodeExpressionStatement(
                                    new CodeMethodInvokeExpression(
                                        new CodeTypeReferenceExpression(typeof (SoodaObjectImpl)),
                                        "SetPlainFieldValue",

                                        // parameters
                                        new CodeThisReferenceExpression(),
                                        new CodePrimitiveExpression(fi.Table.OrdinalInClass),
                                        new CodePrimitiveExpression(fi.Name),
                                        new CodePrimitiveExpression(fi.ClassUnifiedOrdinal),
                                        Box(new CodePropertySetValueReferenceExpression()),
                                        beforeDelegate,
                                        afterDelegate
                                        )));
//wash{
                        }
                        else
                        {
                            prop.SetStatements.Add(
                                new CodeExpressionStatement(
                                    new CodeMethodInvokeExpression(
                                        new CodeTypeReferenceExpression(typeof (SoodaObjectImpl)),
                                        "SetPlainFieldValue",

                                        // parameters
                                        new CodeThisReferenceExpression(),
                                        new CodePrimitiveExpression(fi.Table.OrdinalInClass),
                                        new CodePrimitiveExpression(fi.Name),
                                        new CodePrimitiveExpression(fi.ClassUnifiedOrdinal),
                                        Box(new CodeCastExpression(fi.IsNullable ? typeof (int?) : typeof (int),
                                            new CodePropertySetValueReferenceExpression())),
                                        beforeDelegate,
                                        afterDelegate
                                        )));
                        }
//}wash
                    }
                }
            }

            if (_classInfo.Collections1toN != null)
            {
                foreach (CollectionOnetoManyInfo coli in _classInfo.Collections1toN)
                {
                    prop = new CodeMemberProperty
                    {
                        Name = coli.Name,
                        Attributes = MemberAttributes.Final | MemberAttributes.Public,
                        Type = GetCollectionPropertyType(coli.ClassName)
                    };

                    prop.GetStatements.Add(
                        new CodeConditionStatement(
                            new CodeBinaryOperatorExpression(
                                new CodeFieldReferenceExpression(This, "_collectionCache_" + coli.Name),
                                CodeBinaryOperatorType.IdentityEquality,
                                new CodePrimitiveExpression(null)), new CodeStatement[]
                                {
                                    new CodeAssignStatement(
                                        new CodeFieldReferenceExpression(This, "_collectionCache_" + coli.Name),
                                        new CodeObjectCreateExpression(
                                            GetCollectionWrapperType(coli.ClassName),
                                            new CodeObjectCreateExpression(
                                                new CodeTypeReference(
                                                    typeof (SoodaObjectOneToManyCollection)),
                                                new CodeMethodInvokeExpression(This, "GetTransaction"),
                                                new CodeTypeOfExpression(new CodeTypeReference(coli.ClassName)),
                                                new CodeThisReferenceExpression(),
                                                new CodePrimitiveExpression(coli.ForeignFieldName),
                                                new CodePropertyReferenceExpression(
                                                    new CodeTypeReferenceExpression(coli.ClassName + "_Factory"),
                                                    "TheClassInfo"), new CodeFieldReferenceExpression(null,
                                                        "_collectionWhere_" + coli.Name),
                                                new CodePrimitiveExpression(coli.Cache)))
                                        )
                                }, new CodeStatement[] {}));

                    prop.GetStatements.Add(
                        new CodeMethodReturnStatement(new CodeFieldReferenceExpression(This,
                            "_collectionCache_" + coli.Name)));
//wash{
                    AddCollectionPropAttributes(coli, prop);
//}wash
                    ctd.Members.Add(prop);

#if DOTNET35
                    CodeExpression whereExpression = new CodeMethodInvokeExpression(
                        new CodeTypeReferenceExpression(typeof (Soql)),
                        "FieldEquals",
                        new CodePrimitiveExpression(coli.ForeignFieldName),
                        This);
                    if (!string.IsNullOrEmpty(coli.Where))
                    {
                        whereExpression = new CodeObjectCreateExpression(
                            typeof (SoqlBooleanAndExpression),
                            whereExpression,
                            new CodePropertyReferenceExpression(
                                new CodeFieldReferenceExpression(null, "_collectionWhere_" + coli.Name),
                                "WhereExpression"));
                    }
                    prop = GetCollectionLinqQuery(coli, whereExpression);
                    ctd.Members.Add(prop);
#endif
                }
            }

            if (_classInfo.CollectionsNtoN != null)
            {
                foreach (CollectionManyToManyInfo coli in _classInfo.CollectionsNtoN)
                {
                    RelationInfo relationInfo = coli.GetRelationInfo();
                    // FieldInfo masterField = relationInfo.Table.Fields[1 - coli.MasterField];

                    string relationTargetClass = relationInfo.Table.Fields[coli.MasterField].References;

                    prop = new CodeMemberProperty
                    {
                        Name = coli.Name,
                        Attributes = MemberAttributes.Final | MemberAttributes.Public,
                        Type = GetCollectionPropertyType(relationTargetClass)
                    };

                    prop.GetStatements.Add(
                        new CodeConditionStatement(
                            new CodeBinaryOperatorExpression(
                                new CodeFieldReferenceExpression(This, "_collectionCache_" + coli.Name),
                                CodeBinaryOperatorType.IdentityEquality,
                                new CodePrimitiveExpression(null)), new CodeStatement[]
                                {
                                    new CodeAssignStatement(
                                        new CodeFieldReferenceExpression(This, "_collectionCache_" + coli.Name),
                                        new CodeObjectCreateExpression(
                                            GetCollectionWrapperType(relationTargetClass),
                                            new CodeObjectCreateExpression(
                                                new CodeTypeReference(
                                                    typeof (SoodaObjectManyToManyCollection)),
                                                new CodeMethodInvokeExpression(This, "GetTransaction"),
                                                new CodePrimitiveExpression(coli.MasterField),
                                                new CodeMethodInvokeExpression(This, "GetPrimaryKeyValue"),
                                                new CodeTypeOfExpression(relationInfo.Name + "_RelationTable"),
                                                new CodeFieldReferenceExpression(
                                                    new CodeTypeReferenceExpression(relationInfo.Name +
                                                                                    "_RelationTable"),
                                                    "theRelationInfo")))
                                        )
                                }
                            , new CodeStatement[] {}));

                    prop.GetStatements.Add(
                        new CodeMethodReturnStatement(new CodeFieldReferenceExpression(This,
                            "_collectionCache_" + coli.Name)));

                    ctd.Members.Add(prop);

#if DOTNET35
                    CodeExpression whereExpression = new CodeMethodInvokeExpression(
                        new CodeTypeReferenceExpression(typeof (Soql)),
                        "CollectionFor",
                        new CodeMethodInvokeExpression(
                            new CodePropertyReferenceExpression(
                                new CodeTypeReferenceExpression(_classInfo.Name + "_Factory"), "TheClassInfo"),
                            "FindCollectionManyToMany",
                            new CodePrimitiveExpression(coli.Name)),
                        This);

                    prop = GetCollectionLinqQuery(coli, whereExpression);
                    ctd.Members.Add(prop);
#endif
                }
            }
        }

        private CodeMemberField GetCollectionCache(CollectionBaseInfo coli)
        {
            CodeMemberField field = new CodeMemberField(GetCollectionPropertyType(coli.GetItemClass().Name),
                "_collectionCache_" + coli.Name);
            field.Attributes = MemberAttributes.Private;
            field.InitExpression = new CodePrimitiveExpression(null);
            return field;
        }

        public void GenerateFields(CodeTypeDeclaration ctd, ClassInfo ci)
        {
            if (GetFieldRefCacheCount(ci) > 0)
            {
                CodeMemberField field =
                    new CodeMemberField(new CodeTypeReference(new CodeTypeReference("SoodaObject"), 1), "_refcache")
                    {
                        Attributes = MemberAttributes.Private,
                        InitExpression = new CodeArrayCreateExpression(
                            new CodeTypeReference(typeof (SoodaObject)),
                            new CodePrimitiveExpression(GetFieldRefCacheCount(ci)))
                    };
                ctd.Members.Add(field);
            }

            if (_classInfo.Collections1toN != null)
            {
                foreach (CollectionOnetoManyInfo coli in _classInfo.Collections1toN)
                {
                    ctd.Members.Add(GetCollectionCache(coli));
                }
                foreach (CollectionOnetoManyInfo coli in _classInfo.Collections1toN)
                {
                    CodeMemberField field = new CodeMemberField("Sooda.SoodaWhereClause",
                        "_collectionWhere_" + coli.Name)
                    {
                        Attributes = MemberAttributes.Static | MemberAttributes.Private
                    };
                    if (!string.IsNullOrEmpty(coli.Where))
                    {
                        field.InitExpression = new CodeObjectCreateExpression(
                            "Sooda.SoodaWhereClause",
                            new CodePrimitiveExpression(coli.Where));
                    }
                    else
                    {
                        field.InitExpression = new CodePrimitiveExpression(null);
                    }
                    ctd.Members.Add(field);
                }
            }

            if (_classInfo.CollectionsNtoN != null)
            {
                foreach (CollectionManyToManyInfo coli in _classInfo.CollectionsNtoN)
                {
                    ctd.Members.Add(GetCollectionCache(coli));
                }
            }
        }

//wash{
        private static void AddCustomPropAttributes(FieldInfo fi, CodeMemberProperty prop)
        {
            if (!string.IsNullOrEmpty(fi.DisplayName))
            {
                var declaration1 =
                    new CodeAttributeDeclaration("System.ComponentModel.DisplayNameAttribute",
                        new CodeAttributeArgument(new CodePrimitiveExpression(fi.DisplayName)));
                //prop.Comments.Add(new CodeCommentStatement(fi.DisplayName, true));
                prop.CustomAttributes.Add(declaration1);
            }
            if (!string.IsNullOrEmpty(fi.UITypeEditor))
            {
                var declaration1 =
                    new CodeAttributeDeclaration("System.ComponentModel.EditorAttribute",
                        new CodeAttributeArgument(new CodePrimitiveExpression(fi.UITypeEditor)));
                prop.CustomAttributes.Add(declaration1);
            }
            if (!string.IsNullOrEmpty(fi.TypeConverter))
            {
                var declaration1 =
                    new CodeAttributeDeclaration("System.ComponentModel.TypeConverterAttribute",
                        new CodeAttributeArgument(new CodeTypeOfExpression(fi.TypeConverter)));
                prop.CustomAttributes.Add(declaration1);
            }

            if (!string.IsNullOrEmpty(fi.Enum))
            {
                var declaration1 =
                    new CodeAttributeDeclaration("System.ComponentModel.TypeConverterAttribute",
                        new CodeAttributeArgument(new CodeTypeOfExpression("AttributesEnumConverter")));
                prop.CustomAttributes.Add(declaration1);
            }

            if (!fi.Browsable)
            {
                var declaration1 =
                    new CodeAttributeDeclaration("System.ComponentModel.BrowsableAttribute",
                        new CodeAttributeArgument(new CodePrimitiveExpression(fi.Browsable)));
                prop.CustomAttributes.Add(declaration1);
            }

            if (!string.IsNullOrEmpty(fi.Category))
            {
                var declaration1 =
                    new CodeAttributeDeclaration("System.ComponentModel.CategoryAttribute",
                        new CodeAttributeArgument(new CodePrimitiveExpression(fi.Category)));
                prop.CustomAttributes.Add(declaration1);
            }

            if (!string.IsNullOrEmpty(fi.Description))
            {
                var declaration1 =
                    new CodeAttributeDeclaration("System.ComponentModel.DescriptionAttribute",
                        new CodeAttributeArgument(new CodePrimitiveExpression(fi.Description)));
                prop.CustomAttributes.Add(declaration1);
            }

            if (fi.ReadOnly || fi.UiReadOnly)
            {
                var declaration1 =
                    new CodeAttributeDeclaration("System.ComponentModel.ReadOnlyAttribute",
                        new CodeAttributeArgument(new CodePrimitiveExpression(fi.ReadOnly)));
                prop.CustomAttributes.Add(declaration1);
            }
        }

        private static void AddCollectionPropAttributes(CollectionOnetoManyInfo coli, CodeMemberProperty prop)
        {
            if (coli.DisplayName != null)
            {
                var declaration1 = new CodeAttributeDeclaration("System.ComponentModel.DisplayNameAttribute",
                    new CodeAttributeArgument(new CodePrimitiveExpression(coli.DisplayName)));
                prop.CustomAttributes.Add(declaration1);
            }
            if (coli.UITypeEditor != null)
            {
                var declaration1 = new CodeAttributeDeclaration("System.ComponentModel.EditorAttribute",
                    new CodeAttributeArgument(new CodePrimitiveExpression(coli.UITypeEditor)));
                prop.CustomAttributes.Add(declaration1);
            }
            if (!coli.Browsable)
            {
                var declaration1 = new CodeAttributeDeclaration("System.ComponentModel.BrowsableAttribute",
                    new CodeAttributeArgument(new CodePrimitiveExpression(coli.Browsable)));
                prop.CustomAttributes.Add(declaration1);
            }

            if (coli.Category != null)
            {
                var declaration1 = new CodeAttributeDeclaration("System.ComponentModel.CategoryAttribute",
                    new CodeAttributeArgument(new CodePrimitiveExpression(coli.Category)));
                prop.CustomAttributes.Add(declaration1);
            }
            if (coli.Description != null)
            {
                var declaration1 = new CodeAttributeDeclaration("System.ComponentModel.DescriptionAttribute",
                    new CodeAttributeArgument(new CodePrimitiveExpression(coli.Description)));
                prop.CustomAttributes.Add(declaration1);
            }
            if (coli.ReadOnly)
            {
                var declaration1 = new CodeAttributeDeclaration("System.ComponentModel.ReadOnlyAttribute",
                    new CodeAttributeArgument(new CodePrimitiveExpression(coli.ReadOnly)));
                prop.CustomAttributes.Add(declaration1);
            }
        }

        public CodeMemberProperty Prop_GuidValue(string name, object val)
        {
            var prop = new CodeMemberProperty
            {
                Name = name,
                Attributes = MemberAttributes.Static | MemberAttributes.Public,
                Type = new CodeTypeReference(_classInfo.Name)
            };
            //prop.CustomAttributes.Add(NoStepThrough());
            prop.GetStatements.Add(new CodeVariableDeclarationStatement(
                // Type of the variable to declare.
                typeof (SoodaTransaction), "t",
                new CodeObjectCreateExpression(typeof (SoodaTransaction))));
            prop.GetStatements.Add(
                new CodeTryCatchFinallyStatement(new CodeStatement[]
                {
                    new CodeMethodReturnStatement(
                        new CodeMethodInvokeExpression(
                            LoaderClass(_classInfo), "Load", new CodeVariableReferenceExpression("t"),
                            new CodeObjectCreateExpression("System.Guid",
                                new CodePrimitiveExpression
                                    (Convert.
                                        ToInt32
                                        (val)),
                                new CodePrimitiveExpression
                                    (0),
                                new CodePrimitiveExpression
                                    (0),
                                new CodePrimitiveExpression
                                    (0),
                                new CodePrimitiveExpression
                                    (0),
                                new CodePrimitiveExpression
                                    (0),
                                new CodePrimitiveExpression
                                    (0),
                                new CodePrimitiveExpression
                                    (0),
                                new CodePrimitiveExpression
                                    (0),
                                new CodePrimitiveExpression
                                    (0),
                                new CodePrimitiveExpression
                                    (0))))
                }, new CodeCatchClause[] {}, new CodeStatement[]
                {
                    new CodeExpressionStatement(
                        new CodeMethodInvokeExpression
                            (new CodeVariableReferenceExpression
                                ("t"), "Dispose"))
                }));

            return prop;
        }

//}wash
    }
}