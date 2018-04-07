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
    using System.CodeDom.Compiler;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.IO;
    using System.Reflection;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Xml;
    using CDIL;
    using Microsoft.CSharp;
    using Microsoft.VisualBasic;
    using Schema;

    // ReSharper disable BitwiseOperatorOnEnumWithoutFlags

    public class CodeGenerator
    {
        private CodeDomProvider _codeProvider;
        private SchemaInfo _schema;
        private CodeGeneratorOptions _codeGeneratorOptions;

        private CodeDomProvider _codeGenerator;
        private CodeDomProvider _csharpCodeGenerator;

        private string _fileExtensionWithoutPeriod;

        private bool _rebuildIfChanged = true;

        public bool RebuildIfChanged
        {
            get { return _rebuildIfChanged; }
            set { _rebuildIfChanged = value; }
        }

        public bool RewriteSkeletons { get; set; }

        public bool RewriteProjects { get; set; }

        public SoodaProject Project { get; set; }

        public ICodeGeneratorOutput Output { get; set; }

        public CodeGenerator(SoodaProject project, ICodeGeneratorOutput output)
        {
            Project = project;
            Output = output;
        }

        public void GenerateClassValues(CodeNamespace nspace, ClassInfo ci, bool miniStub)
        {
            if (!string.IsNullOrEmpty(ci.Schema.AssemblyName) || ci.GetDataSource().EnableDynamicFields)
                return;
            var gen = new CodeDomClassStubGenerator(ci, Project);

            var baseClass = typeof (SoodaObjectReflectionEmitFieldValues);
            //wash SoodaObjectReflectionCachingFieldValues

            var ctd = new CodeTypeDeclaration(ci.Name + "_Values");
            ctd.StartDirectives.Add(new CodeRegionDirective(CodeRegionMode.Start, ci.Name + "_Values"));
            ctd.EndDirectives.Add(new CodeRegionDirective(CodeRegionMode.End, ci.Name + "_Values"));

            if (ci.InheritFrom != null)
                ctd.BaseTypes.Add(ci.InheritFrom + "_Values");
            else
                ctd.BaseTypes.Add(baseClass);

            ctd.Attributes = MemberAttributes.Assembly;

            foreach (var fi in ci.LocalFields)
            {
                CodeTypeReference fieldType;
                if (fi.References != null)
                {
                    fieldType = gen.GetReturnType(PrimitiveRepresentation.SqlType, fi);
                }
                else if (fi.IsNullable)
                {
                    fieldType = gen.GetReturnType(Project.NullableRepresentation, fi);
                }
                else
                {
                    fieldType = gen.GetReturnType(Project.NotNullRepresentation, fi);
                }

                var field = new CodeMemberField(fieldType, fi.Name)
                {
                    Attributes = MemberAttributes.Public
                };
                ctd.Members.Add(field);
            }

            var constructor2 = new CodeConstructor {Attributes = MemberAttributes.Public};
            constructor2.Parameters.Add(new CodeParameterDeclarationExpression(baseClass, "other"));
            constructor2.BaseConstructorArgs.Add(new CodeArgumentReferenceExpression("other"));
            ctd.Members.Add(constructor2);

            var constructor3 = new CodeConstructor {Attributes = MemberAttributes.Public};
            constructor3.Parameters.Add(new CodeParameterDeclarationExpression(typeof (string[]), "fieldNames"));
            constructor3.BaseConstructorArgs.Add(new CodeArgumentReferenceExpression("fieldNames"));
            ctd.Members.Add(constructor3);

            var cloneMethod = new CodeMemberMethod
            {
                Name = "Clone",
                ReturnType = new CodeTypeReference(typeof (SoodaObjectFieldValues)),
                Attributes = MemberAttributes.Public | MemberAttributes.Override
            };
            cloneMethod.Statements.Add(
                new CodeMethodReturnStatement(
                    new CodeObjectCreateExpression(ci.Name + "_Values",
                        new CodeThisReferenceExpression())));
            ctd.Members.Add(cloneMethod);

            nspace.Types.Add(ctd);
        }

        // ReSharper disable once InconsistentNaming
        public void CDILParserTest(CodeTypeDeclaration ctd)
        {
#if SKIPPED
            using (StringWriter sw = new StringWriter())
            {
                CDILPrettyPrinter.PrintType(sw, ctd);
                using (StreamWriter fsw = File.CreateText(ctd.Name + "_1.txt"))
                {
                    fsw.Write(sw.ToString());
                }
                CodeTypeDeclaration ctd2 = CDILParser.ParseClass(sw.ToString(), new CDILContext());
                StringWriter sw2 = new StringWriter();
                CDILPrettyPrinter.PrintType(sw2, ctd2);
                using (StreamWriter fsw = File.CreateText(ctd.Name + "_2.txt"))
                {
                    fsw.Write(sw2.ToString());
                }
                if (sw2.ToString() != sw.ToString())
                {
                    throw new InvalidOperationException("DIFFERENT!");
                }
            }
#endif
        }

        public string MakeCamelCase(string s)
        {
            return Char.ToLower(s[0]) + s.Substring(1);
        }

        public void GenerateClassStub(CodeNamespace nspace, ClassInfo ci, bool miniStub)
        {
            if (!string.IsNullOrEmpty(ci.Schema.AssemblyName))
                return;
            if (!miniStub)
                GenerateClassValues(nspace, ci, false);

            var gen = new CodeDomClassStubGenerator(ci, Project);
            var context = new CDILContext();
            context["ClassName"] = ci.Name;
            context["HasBaseClass"] = ci.InheritsFromClass != null;
            context["MiniStub"] = miniStub;
            context["HasKeyGen"] = gen.KeyGen != "none";

            if (ci.ExtBaseClassName != null && !miniStub)
            {
                context["BaseClassName"] = ci.ExtBaseClassName;
            }
            else if (ci.InheritFrom != null && !miniStub)
            {
                context["BaseClassName"] = ci.InheritFrom;
            }
            else if (Project.BaseClassName != null && !miniStub)
            {
                context["BaseClassName"] = Project.BaseClassName;
            }
            else
            {
                context["BaseClassName"] = "SoodaObject";
            }
            context["ArrayFieldValues"] = ci.GetDataSource().EnableDynamicFields;

            var ctd = CDILParser.ParseClass(CDILTemplate.Get("Stub.cdil"), context);

            ctd.StartDirectives.Add(new CodeRegionDirective(CodeRegionMode.Start, ci.Name + "_Stub"));
            ctd.EndDirectives.Add(new CodeRegionDirective(CodeRegionMode.End, ci.Name + "_Stub"));

            if (ci.Description != null)
            {
                ctd.Comments.Add(new CodeCommentStatement("<summary>", true));
                ctd.Comments.Add(new CodeCommentStatement(ci.Description, true));
                ctd.Comments.Add(new CodeCommentStatement("</summary>", true));
            }
            nspace.Types.Add(ctd);

            if (miniStub) return;

            var ctdLoader = GetLoaderClass(ci);

            if (!Project.LoaderClass)
            {
                foreach (CodeTypeMember m in ctdLoader.Members)
                {
                    ctd.Members.Add(m);
                }
            }

            // class constructor

            if (gen.KeyGen != "none")
            {
                ctd.Members.Add(gen.Field_keyGenerator());
            }

            gen.GenerateFields(ctd, ci);
            gen.GenerateProperties(ctd, ci);

            // literals
            if (ci.Constants != null && ci.GetPrimaryKeyFields().Length == 1)
            {
                foreach (var constInfo in ci.Constants)
                {
                    object value;
                    switch (ci.GetFirstPrimaryKeyField().DataType)
                    {
                        case FieldDataType.Integer:
                            value = int.Parse(constInfo.Key);
                            break;
                        case FieldDataType.String:
                        case FieldDataType.AnsiString:
                        case FieldDataType.Guid:
                            value = constInfo.Key;
                            break;
                        default:
                            throw new NotSupportedException("Primary key type " + ci.GetFirstPrimaryKeyField().DataType +
                                                            " is not supported");
                    }
                    ctd.Members.Add(gen.Prop_GuidValue(constInfo.Name, value));
                }
            }

            foreach (var fi in ci.LocalFields)
            {
                if (fi.IsPrimaryKey)
                    continue;

                if (ci.Triggers || fi.ForceTrigger)
                {
                    ctd.Members.Add(gen.Method_TriggerFieldUpdate(fi, "BeforeFieldUpdate"));
                    ctd.Members.Add(gen.Method_TriggerFieldUpdate(fi, "AfterFieldUpdate"));
                }
            }

#if !DEBUG
            ctd.CustomAttributes.Add(new CodeAttributeDeclaration("System.Diagnostics.DebuggerStepThroughAttribute"));
#endif
        }

        public void GenerateClassFactory(CodeNamespace nspace, ClassInfo ci)
        {
            if (!string.IsNullOrEmpty(ci.Schema.AssemblyName))
                return;
            var fi = ci.GetFirstPrimaryKeyField();
            var fieldHandler = fi.GetNullableFieldHandler();
            var pkClrTypeName = fieldHandler.GetFieldType().FullName;
            var pkFieldHandlerTypeName = fieldHandler.GetType().FullName;

            var context = new CDILContext();
            context["ClassName"] = ci.Name;
            context["OutNamespace"] = Project.OutputNamespace;
            if (ci.GetPrimaryKeyFields().Length == 1)
            {
                context["GetRefArgumentType"] = pkClrTypeName;
                context["MultiColumnPrimaryKey"] = false;
            }
            else
            {
                context["GetRefArgumentType"] = "SoodaTuple";
                context["MultiColumnPrimaryKey"] = true;
            }
            context["PrimaryKeyHandlerType"] = pkFieldHandlerTypeName;
            context["IsAbstract"] = ci.IsAbstractClass();
            if (Project.LoaderClass)
                context["LoaderClass"] = /*Project.OutputNamespace.Replace(".", "") + "." + */ ci.Name + "Loader";
            else
                context["LoaderClass"] = /*Project.OutputNamespace.Replace(".", "") + "Stubs." + */ ci.Name + "_Stub";

            var factoryClass = CDILParser.ParseClass(CDILTemplate.Get("Factory.cdil"), context);
            factoryClass.StartDirectives.Add(new CodeRegionDirective(CodeRegionMode.Start, ci.Name + "_Factory"));
            factoryClass.EndDirectives.Add(new CodeRegionDirective(CodeRegionMode.End, ci.Name + "_Factory"));

            factoryClass.CustomAttributes.Add(new CodeAttributeDeclaration("SoodaObjectFactoryAttribute",
                new CodeAttributeArgument(new CodePrimitiveExpression(ci.Name)),
                new CodeAttributeArgument(new CodeTypeOfExpression(ci.Name))
                ));

            nspace.Types.Add(factoryClass);
        }

        public void GenerateClassSkeleton(CodeNamespace nspace, ClassInfo ci, bool useChainedConstructorCall,
            bool fakeSkeleton, bool usePartial, string partialSuffix, bool addBase)
        {
            if (!string.IsNullOrEmpty(ci.Schema.AssemblyName))
                return;

            var ctd = new CodeTypeDeclaration(ci.Name + (usePartial ? partialSuffix : ""));

            if (ci.Description != null)
            {
                ctd.Comments.Add(new CodeCommentStatement("<summary>", true));
                ctd.Comments.Add(new CodeCommentStatement(ci.Description, true));
                ctd.Comments.Add(new CodeCommentStatement("</summary>", true));
            }

            if (addBase)
            {
                ctd.BaseTypes.Add(Project.OutputNamespace.Replace(".", "") + "Stubs." + ci.Name + "_Stub");
            }

            if (ci.IsAbstractClass())
                ctd.TypeAttributes |= TypeAttributes.Abstract;

            nspace.Types.Add(ctd);

            var gen = new CodeDomClassSkeletonGenerator();

            ctd.Members.Add(gen.Constructor_Raw());
            ctd.Members.Add(gen.Constructor_Inserting(useChainedConstructorCall));
            ctd.Members.Add(gen.Constructor_Inserting1(useChainedConstructorCall)); //wash
            ctd.Members.Add(gen.Constructor_Inserting2(useChainedConstructorCall));

            if (!useChainedConstructorCall)
            {
                ctd.Members.Add(gen.Method_InitObject());
            }

            if (usePartial)
            {
                ctd = new CodeTypeDeclaration(ci.Name);
                if (ci.Description != null)
                {
                    ctd.Comments.Add(new CodeCommentStatement("<summary>", true));
                    ctd.Comments.Add(new CodeCommentStatement(ci.Description, true));
                    ctd.Comments.Add(new CodeCommentStatement("</summary>", true));
                }
                ctd.BaseTypes.Add(ci.Name + partialSuffix);
                if (ci.IsAbstractClass())
                    ctd.TypeAttributes |= TypeAttributes.Abstract;
                ctd.IsPartial = true;
                nspace.Types.Add(ctd);

                gen = new CodeDomClassSkeletonGenerator();

                ctd.Members.Add(gen.Constructor_Raw());
                ctd.Members.Add(gen.Constructor_Inserting(useChainedConstructorCall));
                ctd.Members.Add(gen.Constructor_Inserting2(useChainedConstructorCall));

                if (!useChainedConstructorCall)
                {
                    ctd.Members.Add(gen.Method_InitObject());
                }
            }
        }

        public void GenerateClassPartialSkeleton(CodeNamespace nspace, ClassInfo ci)
        {
            if (!string.IsNullOrEmpty(ci.Schema.AssemblyName))
                return;
            var ctd = new CodeTypeDeclaration(ci.Name) {IsPartial = true};
            nspace.Types.Add(ctd);
        }

        public void GenerateEmptyClassSkeleton(CodeNamespace nspace, ClassInfo ci, bool isPartial)
        {
            if (!string.IsNullOrEmpty(ci.Schema.AssemblyName))
                return;

            var ctd = new CodeTypeDeclaration(ci.Name)
            {
                IsPartial = isPartial
            };

            if (ci.Description != null)
            {
                ctd.Comments.Add(new CodeCommentStatement("<summary>", true));
                ctd.Comments.Add(new CodeCommentStatement(ci.Description, true));
                ctd.Comments.Add(new CodeCommentStatement("</summary>", true));
            }

            ctd.BaseTypes.Add(Project.OutputNamespace.Replace(".", "") + "Stubs." + ci.Name + "_Stub");

            if (ci.IsAbstractClass())
                ctd.TypeAttributes |= TypeAttributes.Abstract;

            nspace.Types.Add(ctd);
        }

        private void OutputFactories(CodeArrayCreateExpression cace, string ns, SchemaInfo schema)
        {
            //foreach (IncludeInfo ii in schema.Includes)
            //{
            //    if (!string.IsNullOrEmpty(ii.SchemaFile))
            //        OutputFactories(cace, ii.Namespace, ii.Schema);
            //}

            foreach (var ci in schema.Classes)
            {
                var nameSpace = string.IsNullOrEmpty(ci.Schema.Namespace) ? ns : ci.Schema.Namespace;
                cace.Initializers.Add(
                    new CodePropertyReferenceExpression(
                        new CodeTypeReferenceExpression(nameSpace + ".Stubs." + ci.Name + "_Factory"), "TheFactory"));
                //cace.Initializers.Add(new CodePropertyReferenceExpression(new CodeTypeReferenceExpression(ns + ".Stubs." + ci.Name + "_Factory"), "TheFactory"));
            }
        }

        public void GenerateDatabaseSchema(CodeNamespace nspace, SchemaInfo schema)
        {
            var context = new CDILContext();
            context["OutNamespace"] = Project.OutputNamespace;

            var databaseSchemaClass = CDILParser.ParseClass(CDILTemplate.Get("DatabaseSchema.cdil"), context);
            databaseSchemaClass.StartDirectives.Add(new CodeRegionDirective(CodeRegionMode.Start, "_DataBaseSchema"));
            databaseSchemaClass.EndDirectives.Add(new CodeRegionDirective(CodeRegionMode.End, "_DataBaseSchema"));

            var cace = new CodeArrayCreateExpression("ISoodaObjectFactory");
            OutputFactories(cace, Project.OutputNamespace, schema);

            var ctc = new CodeTypeConstructor();
            ctc.Statements.Add(
                new CodeAssignStatement(
                    new CodeFieldReferenceExpression(null, "_theSchema"),
                    new CodeMethodInvokeExpression(null, "LoadSchema")));

            databaseSchemaClass.Members.Add(ctc);

            var ctor = new CodeConstructor {Attributes = MemberAttributes.Public};
            ctor.Statements.Add(
                new CodeAssignStatement(
                    new CodeFieldReferenceExpression(new CodeThisReferenceExpression(), "_factories"), cace));

            databaseSchemaClass.Members.Add(ctor);

            nspace.Types.Add(databaseSchemaClass);
        }

        public void GenerateListWrapper(CodeNamespace nspace, ClassInfo ci)
        {
            if (!string.IsNullOrEmpty(ci.Schema.AssemblyName))
                return;
            //Output.Verbose("      * list wrapper {0}.{1}.{2}", ci.Schema.AssemblyName, ci.Schema.Namespace, ci.Name);
            var context = new CDILContext();
            context["ClassName"] = ci.Name;
            context["OptionalNewAttribute"] = (_codeProvider is VBCodeProvider) ? "" : ",New";

            var listWrapperClass = CDILParser.ParseClass(CDILTemplate.Get("ListWrapper.cdil"), context);
            listWrapperClass.StartDirectives.Add(new CodeRegionDirective(CodeRegionMode.Start, ci.Name + "_ListWrapper"));
            listWrapperClass.EndDirectives.Add(new CodeRegionDirective(CodeRegionMode.End, ci.Name + "_ListWrapper"));

            nspace.Types.Add(listWrapperClass);
        }

        public CodeTypeDeclaration GetLoaderClass(ClassInfo ci)
        {
            var context = new CDILContext();
            context["ClassName"] = ci.Name;
            context["HasBaseClass"] = ci.InheritsFromClass != null;

            var formalParameters = "";
            var actualParameters = "";

            foreach (var fi in ci.GetPrimaryKeyFields())
            {
                if (formalParameters != "")
                {
                    formalParameters += ", ";
                    actualParameters += ", ";
                }
                var pkClrTypeName = fi.GetNullableFieldHandler().GetFieldType().FullName;
                formalParameters += pkClrTypeName + " " + MakeCamelCase(fi.Name);
                actualParameters += "arg(" + MakeCamelCase(fi.Name) + ")";
            }

            context["PrimaryKeyFormalParameters"] = formalParameters;
            context["PrimaryKeyActualParameters"] = actualParameters;
            if (ci.GetPrimaryKeyFields().Length == 1)
            {
                context["PrimaryKeyActualParametersTuple"] = actualParameters;
                context["PrimaryKeyIsTuple"] = false;
            }
            else
            {
                context["PrimaryKeyIsTuple"] = true;
                context["PrimaryKeyActualParametersTuple"] = "new SoodaTuple(" + actualParameters + ")";
            }

            context["ClassUnifiedFieldCount"] = ci.UnifiedFields.Count;
            context["PrimaryKeyFieldHandler"] =
                ci.GetFirstPrimaryKeyField().GetNullableFieldHandler().GetType().FullName;
            context["OptionalNewAttribute"] = (ci.InheritsFromClass != null) ? ",New" : "";
            if (_codeProvider is VBCodeProvider)
            {
                context["OptionalNewAttribute"] = "";
            }
            if (Project.LoaderClass)
            {
                context["LoaderClass"] = /*Project.OutputNamespace.Replace(".", "") + "." + */ ci.Name + "Loader";
                context["OptionalNewAttribute"] = "";
            }
            else
                context["LoaderClass"] = /*Project.OutputNamespace.Replace(".", "") + "Stubs." + */ ci.Name + "_Stub";
            context["WithSoql"] = Project.WithSoql;
#if DOTNET35
            context["Linq"] = true;
#else
            context["Linq"] = false;
#endif
            var ctd = CDILParser.ParseClass(CDILTemplate.Get("Loader.cdil"), context);
            foreach (var fi in ci.LocalFields)
            {
                for (var withTransaction = 0; withTransaction <= 1; ++withTransaction)
                {
                    for (var list = 0; list <= 1; list++)
                    {
                        for (var reference = 0; reference <= 1; reference++)
                        {
                            if (reference == 1 && fi.ReferencedClass == null)
                                continue;

                            if (list == 0 && !fi.FindMethod)
                                continue;

                            if (list == 1 && !fi.FindListMethod)
                                continue;

                            var findMethod = new CodeMemberMethod
                            {
                                Name = "Find" + (list == 1 ? "List" : "") + "By" + fi.Name,
                                Attributes = MemberAttributes.Public | MemberAttributes.Static,
                                ReturnType =
                                    new CodeTypeReference(Project.OutputNamespace.Replace(".", "") + "." + ci.Name +
                                                          ((list == 1) ? "List" : ""))
                            };

                            if (withTransaction == 1)
                            {
                                findMethod.Parameters.Add(
                                    new CodeParameterDeclarationExpression(
                                        new CodeTypeReference(typeof (SoodaTransaction)), "transaction")
                                    );
                            }

                            if (reference == 1)
                            {
                                if (fi.ReferencedClass != null)
                                    findMethod.Parameters.Add(
                                        new CodeParameterDeclarationExpression(
                                            fi.ReferencedClass.Name, MakeCamelCase(fi.Name))
                                        );
                            }
                            else
                            {
                                findMethod.Parameters.Add(
                                    new CodeParameterDeclarationExpression(
                                        fi.GetNullableFieldHandler().GetFieldType(), MakeCamelCase(fi.Name))
                                    );
                            }

                            CodeExpression whereClause =
                                new CodeObjectCreateExpression(
                                    new CodeTypeReference(typeof (SoodaWhereClause)),
                                    new CodePrimitiveExpression(fi.Name + " = {0}"),
                                    new CodeArrayCreateExpression(
                                        typeof (object),
                                        new CodeExpression[]
                                        {new CodeArgumentReferenceExpression(MakeCamelCase(fi.Name))})
                                    );

                            CodeExpression transaction = new CodeArgumentReferenceExpression("transaction");
                            if (withTransaction == 0)
                            {
                                transaction = new CodePropertyReferenceExpression(
                                    new CodeTypeReferenceExpression(typeof (SoodaTransaction)),
                                    "ActiveTransaction");
                            }

                            findMethod.Statements.Add(
                                new CodeMethodReturnStatement(
                                    new CodeMethodInvokeExpression(
                                        null, (list == 1 ? "GetList" : "LoadSingleObject"),
                                        transaction,
                                        whereClause)));
                            ctd.Members.Add(findMethod);
                        }
                    }
                }
            }
            return ctd;
        }

        private void GenerateLoaderClass(CodeNamespace nspace, ClassInfo ci)
        {
            if (!string.IsNullOrEmpty(ci.Schema.AssemblyName))
                return;
            var ctd = GetLoaderClass(ci);
            nspace.Types.Add(ctd);
        }

        public void GenerateRelationStub(CodeNamespace nspace, RelationInfo ri)
        {
            var gen = new CodeDomListRelationTableGenerator(ri);

            // public class RELATION_NAME_RelationTable : SoodaRelationTable
            var ctd = new CodeTypeDeclaration(ri.Name + "_RelationTable");
            ctd.BaseTypes.Add("SoodaRelationTable");
            nspace.Types.Add(ctd);

            // public RELATION_NAME_RelationTable() : base("RELATION_TABLE_NAME","LEFT_COLUMN_NAME","RIGHT_COLUMN_NAME") { }
            ctd.Members.Add(gen.Constructor_1());
            ctd.Members.Add(gen.Method_DeserializeTupleLeft());
            ctd.Members.Add(gen.Method_DeserializeTupleRight());

            var field = new CodeMemberField("Sooda.Schema.RelationInfo", "theRelationInfo")
            {
                Attributes = MemberAttributes.Public | MemberAttributes.Static,
                InitExpression = new CodeMethodInvokeExpression(
                    new CodeMethodInvokeExpression(
                        new CodeTypeReferenceExpression(Project.OutputNamespace.Replace(".", "") + "." +
                                                        "_DatabaseSchema"), "GetSchema"), "FindRelationByName",
                    new CodePrimitiveExpression(ri.Name))
            };

            ctd.Members.Add(field);

            //public class RELATION_NAME_L_List : RELATION_NAME_Rel_List, LEFT_COLUMN_REF_TYPEList, ISoodaObjectList

            //OutputRelationHalfTable(nspace, "L", relationName, leftColumnName, leftColumnType, ref1ClassInfo, Project);
            //OutputRelationHalfTable(nspace, "R", relationName, rightColumnName, rightColumnType, ref2ClassInfo, Project);
        }

        private readonly Dictionary<string, string> _generatedMiniBaseClasses = new Dictionary<string, string>();

        private void GenerateMiniBaseClass(CodeCompileUnit ccu, string className)
        {
            if (!_generatedMiniBaseClasses.ContainsKey(className))
            {
                _generatedMiniBaseClasses.Add(className, className);

                var lastPeriod = className.LastIndexOf('.');
                var namespaceName = Project.OutputNamespace;
                if (lastPeriod != -1)
                {
                    namespaceName = className.Substring(0, lastPeriod);
                    className = className.Substring(lastPeriod + 1);
                }

                var ns = new CodeNamespace(namespaceName);
                ns.Imports.Add(new CodeNamespaceImport("Sooda"));
                ccu.Namespaces.Add(ns);

                var ctd = new CodeTypeDeclaration(className);
                ctd.BaseTypes.Add(typeof (SoodaObject));
                ns.Types.Add(ctd);

                var ctor = new CodeConstructor {Attributes = MemberAttributes.Family};

                ctor.Parameters.Add(new CodeParameterDeclarationExpression("SoodaConstructor", "c"));
                ctor.BaseConstructorArgs.Add(new CodeArgumentReferenceExpression("c"));

                ctd.Members.Add(ctor);

                ctor = new CodeConstructor {Attributes = MemberAttributes.Family};

                ctor.Parameters.Add(new CodeParameterDeclarationExpression("SoodaTransaction", "tran"));
                ctor.BaseConstructorArgs.Add(new CodeArgumentReferenceExpression("tran"));
                ctd.Members.Add(ctor);

                Output.Verbose("Generating mini base class {0}", className);
            }
        }

        private void GenerateTypedPublicQueryWrappers(CodeNamespace ns, ClassInfo ci)
        {
            if (!string.IsNullOrEmpty(ci.Schema.AssemblyName))
                return;

            var context = new CDILContext();
            context["ClassName"] = ci.Name;

            var ctd = CDILParser.ParseClass(CDILTemplate.Get("ClassField.cdil"), context);
            ns.Types.Add(ctd);

            foreach (var coll in ci.UnifiedCollections)
            {
                var prop = new CodeMemberProperty
                {
                    Name = coll.Name,
                    Attributes = MemberAttributes.Public | MemberAttributes.Static,
                    Type = new CodeTypeReference(coll.GetItemClass().Name + "CollectionExpression")
                };

                prop.GetStatements.Add(
                    new CodeMethodReturnStatement(
                        new CodeObjectCreateExpression(prop.Type, new CodePrimitiveExpression(null),
                            new CodePrimitiveExpression(coll.Name))
                        ));

                ctd.Members.Add(prop);
            }

            foreach (var fi in ci.UnifiedFields)
            {
                var prop = new CodeMemberProperty
                {
                    Name = fi.Name,
                    Attributes = MemberAttributes.Public | MemberAttributes.Static
                };

                string fullWrapperTypeName;
                var optionalNullable = fi.IsNullable ? "Nullable" : string.Empty;

                if (fi.ReferencedClass == null)
                {
                    fullWrapperTypeName = fi.GetFieldHandler().GetTypedWrapperClass();
                    if (fullWrapperTypeName == null)
                        continue;

                    prop.GetStatements.Add(new CodeMethodReturnStatement(
                        new CodeObjectCreateExpression(fullWrapperTypeName,
                            new CodeObjectCreateExpression("Sooda.QL.SoqlPathExpression",
                                new CodePrimitiveExpression(fi.Name)))));
                }
                else
                {
                    fullWrapperTypeName = fi.ReferencedClass.Name + optionalNullable + "WrapperExpression";
                    prop.GetStatements.Add(new CodeMethodReturnStatement(
                        new CodeObjectCreateExpression(fullWrapperTypeName,
                            new CodePrimitiveExpression(null), new CodePrimitiveExpression(fi.Name))));
                }

                prop.Type = new CodeTypeReference(fullWrapperTypeName);
                ctd.Members.Add(prop);
            }
        }

        private void GenerateTypedInternalQueryWrappers(CodeNamespace ns, ClassInfo ci)
        {
            if (!string.IsNullOrEmpty(ci.Schema.AssemblyName))
                return;
            var context = new CDILContext();
            context["ClassName"] = ci.Name;
            context["PrimaryKeyType"] = ci.GetFirstPrimaryKeyField().GetNullableFieldHandler().GetFieldType().FullName;
            context["CSharp"] = _codeProvider is CSharpCodeProvider;

            var ctd = CDILParser.ParseClass(CDILTemplate.Get("TypedCollectionWrapper.cdil"), context);
            ns.Types.Add(ctd);

            context = new CDILContext();
            context["ClassName"] = ci.Name;
            context["PrimaryKeyType"] = ci.GetFirstPrimaryKeyField().GetNullableFieldHandler().GetFieldType().FullName;
            context["CSharp"] = _codeProvider is CSharpCodeProvider;
            context["ParameterAttributes"] = _codeGenerator.Supports(GeneratorSupport.ParameterAttributes);

            ctd = CDILParser.ParseClass(CDILTemplate.Get("TypedWrapper.cdil"), context);
            ns.Types.Add(ctd);

            foreach (var coll in ci.UnifiedCollections)
            {
                var prop = new CodeMemberProperty
                {
                    Name = coll.Name,
                    Attributes = MemberAttributes.Public,
                    Type = new CodeTypeReference(coll.GetItemClass().Name + "CollectionExpression")
                };

                prop.GetStatements.Add(
                    new CodeMethodReturnStatement(
                        new CodeObjectCreateExpression(prop.Type, new CodeThisReferenceExpression(),
                            new CodePrimitiveExpression(coll.Name))
                        ));

                ctd.Members.Add(prop);
            }

            foreach (var fi in ci.UnifiedFields)
            {
                var prop = new CodeMemberProperty {Name = fi.Name, Attributes = MemberAttributes.Public};

                string fullWrapperTypeName;
                var optionalNullable = fi.IsNullable ? "Nullable" : string.Empty;

                if (fi.ReferencedClass == null)
                {
                    fullWrapperTypeName = fi.GetFieldHandler().GetTypedWrapperClass();
                    if (fullWrapperTypeName == null)
                        continue;

                    prop.GetStatements.Add(new CodeMethodReturnStatement(
                        new CodeObjectCreateExpression(fullWrapperTypeName,
                            new CodeObjectCreateExpression("Sooda.QL.SoqlPathExpression",
                                new CodeThisReferenceExpression(), new CodePrimitiveExpression(fi.Name)))));
                }
                else
                {
                    fullWrapperTypeName = fi.ReferencedClass.Name + optionalNullable + "WrapperExpression";
                    prop.GetStatements.Add(new CodeMethodReturnStatement(
                        new CodeObjectCreateExpression(fullWrapperTypeName,
                            new CodeThisReferenceExpression(), new CodePrimitiveExpression(fi.Name))));
                }

                prop.Type = new CodeTypeReference(fullWrapperTypeName);
                ctd.Members.Add(prop);
            }

            var nullablectd = CDILParser.ParseClass(CDILTemplate.Get("NullableTypedWrapper.cdil"), context);
            ns.Types.Add(nullablectd);
        }

        private string GetEmbeddedSchemaFileName()
        {
            var ext = Project.EmbedSchema == EmbedSchema.Xml ? "xml" : "bin";
            return Project.SeparateStubs
                ? Path.Combine(Project.OutputPath, "Stubs/_DBSchema." + ext)
                : Path.Combine(Project.OutputPath, "_DBSchema." + ext);
        }

        private string GetStubsFile()
        {
            return Project.SeparateStubs
                ? Path.Combine(Project.OutputPath, "Stubs/_Stubs.csx")
                : Path.Combine(Project.OutputPath, "_Stubs." + _fileExtensionWithoutPeriod);
        }

        private void GetInputAndOutputFiles(StringCollection inputFiles, StringCollection rewrittenOutputFiles,
            StringCollection shouldBePresentOutputFiles)
        {
            // input
            inputFiles.Add(Path.GetFullPath(GetType().Assembly.Location)); // Sooda.CodeGen.dll
            inputFiles.Add(Path.GetFullPath(Project.SchemaFile));

            // includes
            foreach (var ii in _schema.Includes)
            {
                if (!string.IsNullOrEmpty(ii.SchemaFile))
                    inputFiles.Add(Path.GetFullPath(ii.SchemaFile));
            }

            // output
            rewrittenOutputFiles.Add(Path.GetFullPath(GetEmbeddedSchemaFileName()));
            rewrittenOutputFiles.Add(Path.GetFullPath(GetStubsFile()));

            if (Project.FilePerNamespace)
            {
                rewrittenOutputFiles.Add(
                    Path.GetFullPath(Path.Combine(Project.OutputPath,
                        "_Stubs." + Project.OutputNamespace + "." + _fileExtensionWithoutPeriod)));
                rewrittenOutputFiles.Add(
                    Path.GetFullPath(Path.Combine(Project.OutputPath,
                        "_Stubs." + Project.OutputNamespace + ".Stubs." + _fileExtensionWithoutPeriod)));
                if (Project.WithTypedQueryWrappers)
                    rewrittenOutputFiles.Add(
                        Path.GetFullPath(Path.Combine(Project.OutputPath,
                            "_Stubs." + Project.OutputNamespace + ".TypedQueries." + _fileExtensionWithoutPeriod)));
            }
            foreach (var ci in _schema.LocalClasses)
            {
                shouldBePresentOutputFiles.Add(
                    Path.GetFullPath(Path.Combine(Project.OutputPath, ci.Name + "." + _fileExtensionWithoutPeriod)));
                //shouldBePresentOutputFiles.Add(Path.GetFullPath(Path.Combine(Project.OutputPath, ci.Name + ".Generated." + _fileExtensionWithoutPeriod)));
            }
        }

        private DateTime MaxDate(StringCollection files)
        {
            var max = DateTime.MinValue;

            foreach (var s in files)
            {
                var dt = File.Exists(s) ? File.GetLastWriteTime(s) : DateTime.MaxValue;

                if (dt > max)
                    max = dt;
            }
            // Console.WriteLine("maxDate: {0}", max);
            return max;
        }

        private DateTime MinDate(StringCollection files)
        {
            var min = DateTime.MaxValue;

            foreach (var s in files)
            {
                DateTime dt;

                if (File.Exists(s))
                    dt = File.GetLastWriteTime(s);
                else
                {
                    Output.Info("{0} not found", s);
                    dt = DateTime.MinValue;
                }

                if (dt < min)
                    min = dt;
            }
            // Console.WriteLine("minDate: {0}", min);
            return min;
        }

        private void SaveExternalProjects()
        {
            foreach (var epi in Project.ExternalProjects)
            {
                Output.Verbose("Saving Project '{0}'...", epi.ActualProjectFile);
                epi.ProjectProvider.SaveTo(epi.ActualProjectFile);
            }
            Output.Verbose("Saved.");
        }

        private void LoadExternalProjects()
        {
            foreach (var epi in Project.ExternalProjects)
            {
                var projectProvider = GetProjectProvider(epi.ProjectType, _codeProvider);

                epi.ActualProjectFile = Path.Combine(Project.OutputPath,
                    epi.ProjectFile ?? projectProvider.GetProjectFileName(Project.OutputNamespace));

                epi.OutputPath = Project.ProjectFilesPath ?? string.Empty;
                epi.ProjectProvider = projectProvider;

                if (!File.Exists(epi.ActualProjectFile) || RewriteProjects)
                {
                    Output.Info("Creating Project file '{0}'.", epi.ActualProjectFile);
                    projectProvider.CreateNew(Project.OutputNamespace, Project.AssemblyName);
                }
                else
                {
                    Output.Verbose("Opening Project file '{0}'...", epi.ActualProjectFile);
                    projectProvider.LoadFrom(epi.ActualProjectFile);
                }
            }
        }

        private void CreateOutputDirectories(StringCollection outputFiles)
        {
            foreach (var s in outputFiles)
            {
                var d = Path.GetDirectoryName(s);
                if (d == null || Directory.Exists(d)) continue;
                Output.Verbose("Creating directory {0}", d);
                Directory.CreateDirectory(d);
            }
        }

        private void WriteMiniStubs()
        {
            var fname = Path.Combine(Project.OutputPath, "Stubs/_MiniStubs.csx");
            Output.Verbose("Generating code for {0}...", fname);

            var ccu = new CodeCompileUnit();

            // stubs namespace
            var nspace = CreateStubsNamespace(_schema);
            ccu.Namespaces.Add(nspace);

            Output.Verbose("    * class stubs");
            foreach (var ci in _schema.LocalClasses)
            {
                GenerateClassStub(nspace, ci, true);
            }
            using (var sw = new StreamWriter(fname))
            {
                _csharpCodeGenerator.GenerateCodeFromCompileUnit(ccu, sw, _codeGeneratorOptions);
            }
        }

        private void WriteMiniSkeleton()
        {
            var fname = Path.Combine(Project.OutputPath, "Stubs/_MiniSkeleton.csx");
            Output.Verbose("Generating code for {0}...", fname);
            // fake skeletons for first compilation only

            var ccu = new CodeCompileUnit();
            var nspace = CreateBaseNamespace(_schema);
            ccu.Namespaces.Add(nspace);

            foreach (var ci in _schema.LocalClasses)
            {
                GenerateClassSkeleton(nspace, ci,
                    _codeGenerator.Supports(GeneratorSupport.ChainedConstructorArguments), true,
                    !ci.IgnorePartial && Project.UsePartial, Project.PartialSuffix, true);
            }

            foreach (var ci in _schema.LocalClasses)
            {
                if (ci.ExtBaseClassName != null)
                {
                    GenerateMiniBaseClass(ccu, ci.ExtBaseClassName);
                }
            }

            if (Project.BaseClassName != null)
            {
                GenerateMiniBaseClass(ccu, Project.BaseClassName);
            }

            using (var sw = new StreamWriter(fname))
            {
                _csharpCodeGenerator.GenerateCodeFromCompileUnit(ccu, sw, _codeGeneratorOptions);
            }
        }

        private void WriteSkeletonClasses()
        {
            foreach (var ci in _schema.LocalClasses)
            {
                var fname = ci.Name + "." + _fileExtensionWithoutPeriod;

                Output.Verbose("    {0}", fname);

                foreach (var epi in Project.ExternalProjects)
                {
                    var filename = Path.Combine(epi.OutputPath, fname);
                    epi.ProjectProvider.AddCompileUnit(filename);
                }

                var usePartial = !ci.IgnorePartial && Project.UsePartial;

                var outFile = Path.Combine(usePartial ? Project.OutputPartialPath : Project.OutputPath, fname);

                if (!File.Exists(outFile) || RewriteSkeletons)
                {
                    using (TextWriter tw = new StreamWriter(outFile))
                    {
                        var nspace = CreateBaseNamespace(_schema);
                        GenerateClassSkeleton(nspace, ci,
                            _codeGenerator.Supports(GeneratorSupport.ChainedConstructorArguments), false, usePartial,
                            Project.PartialSuffix, true);
                        _codeGenerator.GenerateCodeFromNamespace(nspace, tw, _codeGeneratorOptions);
                    }
                }
                if (usePartial)
                {
                    outFile = Path.Combine(Project.OutputPath, fname);
                    if (!File.Exists(outFile) || RewriteSkeletons)
                    {
                        using (TextWriter tw = new StreamWriter(outFile))
                        {
                            var nspace = CreatePartialNamespace();
                            GenerateClassPartialSkeleton(nspace, ci);
                            _codeGenerator.GenerateCodeFromNamespace(nspace, tw, _codeGeneratorOptions);
                        }
                    }
                }
            }
        }


        private void SerializeSchema()
        {
            var embedBaseDir = Project.OutputPath;
            if (Project.SeparateStubs)
                embedBaseDir = Path.Combine(embedBaseDir, "Stubs");

            if (Project.EmbedSchema == EmbedSchema.Xml)
            {
                Output.Verbose("Copying schema to {0}...", Path.Combine(embedBaseDir, "_DBSchema.xml"));
                File.Copy(Project.SchemaFile, Path.Combine(embedBaseDir, "_DBSchema.xml"), true);
                if (!Project.SeparateStubs)
                {
                    foreach (var epi in Project.ExternalProjects)
                    {
                        epi.ProjectProvider.AddResource(Path.Combine(epi.OutputPath, "_DBSchema.xml"));
                    }
                }
            }
            else if (Project.EmbedSchema == EmbedSchema.Binary)
            {
                var binFileName = Path.Combine(embedBaseDir, "_DBSchema.bin");
                Output.Verbose("Serializing schema to {0}...", binFileName);

                var bf = new BinaryFormatter();
                using (var fileStream = File.OpenWrite(binFileName))
                {
                    bf.Serialize(fileStream, _schema);
                }
                if (!Project.SeparateStubs)
                {
                    foreach (var epi in Project.ExternalProjects)
                    {
                        epi.ProjectProvider.AddResource(Path.Combine(epi.OutputPath, "_DBSchema.bin"));
                    }
                }
            }
        }

        public void Run()
        {
            try
            {
                if (Project.SchemaFile == null)
                    throw new SoodaCodeGenException("No schema file specified.");

                if (Project.OutputPath == null)
                    throw new SoodaCodeGenException("No Output path specified.");

                if (Project.OutputNamespace == null)
                    throw new SoodaCodeGenException("No Output namespace specified.");

                var codeProvider = GetCodeProvider(Project.Language);
                _codeProvider = codeProvider;

                var codeGenerator = codeProvider;
                var csharpCodeGenerator = GetCodeProvider("c#");

                _codeGenerator = codeGenerator;
                _csharpCodeGenerator = csharpCodeGenerator;

                _fileExtensionWithoutPeriod = _codeProvider.FileExtension;
                if (_fileExtensionWithoutPeriod.StartsWith("."))
                    _fileExtensionWithoutPeriod = _fileExtensionWithoutPeriod.Substring(1);

                Output.Verbose("Loading schema file {0}...", Project.SchemaFile);
                _schema = SchemaManager.ReadAndValidateSchema(
                    new XmlTextReader(Project.SchemaFile),
                    Path.GetDirectoryName(Project.SchemaFile)
                    );
                if (string.IsNullOrEmpty(_schema.Namespace))
                    _schema.Namespace = Project.OutputNamespace;

                var inputFiles = new StringCollection();
                var rewrittenOutputFiles = new StringCollection();
                var shouldBePresentOutputFiles = new StringCollection();

                GetInputAndOutputFiles(inputFiles, rewrittenOutputFiles, shouldBePresentOutputFiles);

                bool doRebuild = MinDate(shouldBePresentOutputFiles) == DateTime.MaxValue ||
                                 MaxDate(inputFiles) > MinDate(rewrittenOutputFiles) ||
                                 !RebuildIfChanged ||
                                 RewriteProjects;
                if (!doRebuild)
                {
                    Output.Info("Not rebuilding.");
                    return;
                }
                /*
                foreach (string s in inputFiles)
                {
                    Output.Verbose("IN: {0}", s);
                }
                foreach (string s in outputFiles)
                {
                    Output.Verbose("OUT: {0}", s);
                }
                */

                if (Project.AssemblyName == null)
                    Project.AssemblyName = Project.OutputNamespace;

                Output.Verbose("Loaded {0} classes, {1} relations...", _schema.LocalClasses.Count,
                    _schema.Relations.Count);
                LoadExternalProjects();
                CreateOutputDirectories(rewrittenOutputFiles);
                CreateOutputDirectories(shouldBePresentOutputFiles);

                string stubsFileName;

                Output.Verbose("CodeProvider:      {0}", codeProvider.GetType().FullName);
                Output.Verbose("Source extension:  {0}", codeProvider.FileExtension);
                foreach (var epi in Project.ExternalProjects)
                {
                    Output.Verbose("Project:           {0} ({1})", epi.ProjectType, epi.ActualProjectFile);
                }
                Output.Verbose("Output Path:       {0}", Project.OutputPath);
                Output.Verbose("Namespace:         {0}", Project.OutputNamespace);

                // write skeleton files
                _codeGeneratorOptions = new CodeGeneratorOptions
                {
                    BracingStyle = "C",
                    IndentString = "    "
                };

                WriteSkeletonClasses();

                // write stubs
                _codeGeneratorOptions.BracingStyle = "Block";
                _codeGeneratorOptions.IndentString = "  ";
                _codeGeneratorOptions.BlankLinesBetweenMembers = false;

                if (Project.SeparateStubs)
                {
                    WriteMiniSkeleton();
                    WriteMiniStubs();
                }

                SerializeSchema();

                // codeGenerator = csharpCodeGenerator;

                if (Project.SeparateStubs)
                {
                    stubsFileName = Path.Combine(Project.OutputPath, "Stubs/_Stubs.csx");
                }
                else if (Project.FilePerNamespace)
                {
                    var fname = "_Stubs." + _fileExtensionWithoutPeriod;
                    stubsFileName = Path.Combine(Project.OutputPath, fname);

                    foreach (var epi in Project.ExternalProjects)
                    {
                        epi.ProjectProvider.AddCompileUnit(Path.Combine(epi.OutputPath, fname));
                    }

                    fname = "_Stubs." + Project.OutputNamespace + ".TypedQueries." + _fileExtensionWithoutPeriod;
                    foreach (var epi in Project.ExternalProjects)
                    {
                        epi.ProjectProvider.AddCompileUnit(Path.Combine(epi.OutputPath, fname));
                    }

                    fname = "_Stubs." + Project.OutputNamespace + ".Stubs." + _fileExtensionWithoutPeriod;
                    foreach (var epi in Project.ExternalProjects)
                    {
                        epi.ProjectProvider.AddCompileUnit(Path.Combine(epi.OutputPath, fname));
                    }
                }
                else
                {
                    var fname = "_Stubs." + _fileExtensionWithoutPeriod;
                    foreach (var epi in Project.ExternalProjects)
                    {
                        epi.ProjectProvider.AddCompileUnit(Path.Combine(epi.OutputPath, fname));
                    }
                    stubsFileName = Path.Combine(Project.OutputPath, fname);
                }

                var ccu = new CodeCompileUnit();
                var cad = new CodeAttributeDeclaration("Sooda.SoodaObjectsAssembly");
                cad.Arguments.Add(
                    new CodeAttributeArgument(new CodeTypeOfExpression(Project.OutputNamespace + "._DatabaseSchema")));
                ccu.AssemblyCustomAttributes.Add(cad);

                CodeNamespace nspace = null;
                if (Project.WithSoql || Project.LoaderClass)
                {
                    nspace = CreateBaseNamespace(_schema);
                    ccu.Namespaces.Add(nspace);
                }

                if (Project.WithSoql)
                {
                    Output.Verbose("    * list wrappers");
                    foreach (ClassInfo ci in _schema.LocalClasses)
                    {
                        GenerateListWrapper(nspace, ci);
                    }
                }

                if (Project.LoaderClass)
                {
                    Output.Verbose("    * loader class");
                    foreach (var ci in _schema.LocalClasses)
                    {
                        GenerateLoaderClass(nspace, ci);
                    }
                }

                Output.Verbose("    * database schema");

                // stubs namespace
                nspace = CreateStubsNamespace(_schema);
                ccu.Namespaces.Add(nspace);

                Output.Verbose("    * class stubs");
                foreach (var ci in _schema.LocalClasses)
                {
                    GenerateClassStub(nspace, ci, false);
                }
                Output.Verbose("    * class factories");
                foreach (var ci in _schema.LocalClasses)
                {
                    GenerateClassFactory(nspace, ci);
                }
                Output.Verbose("    * N-N relation stubs");
                foreach (var ri in _schema.LocalRelations)
                {
                    GenerateRelationStub(nspace, ri);
                }

                if (Project.WithTypedQueryWrappers)
                {
                    Output.Verbose("    * typed query wrappers (internal)");
                    foreach (var ci in _schema.LocalClasses)
                    {
                        GenerateTypedInternalQueryWrappers(nspace, ci);
                    }

                    nspace = CreateTypedQueriesNamespace(_schema);
                    ccu.Namespaces.Add(nspace);

                    Output.Verbose("    * typed query wrappers");
                    foreach (var ci in _schema.LocalClasses)
                    {
                        GenerateTypedPublicQueryWrappers(nspace, ci);
                    }
                }

                if (Project.FilePerNamespace)
                {
                    foreach (CodeNamespace ns in ccu.Namespaces)
                    {
                        using (var sw = new StringWriter())
                        {
                            Output.Verbose("Writing code...");
                            codeGenerator.GenerateCodeFromNamespace(ns, sw, _codeGeneratorOptions);
                            Output.Verbose("Done.");

                            var resultString = sw.ToString();
                            resultString = resultString.Replace("[System.ParamArrayAttribute()] ", "params ");

                            var fileName = "_Stubs." + ns.Name + "." + _fileExtensionWithoutPeriod;
                            foreach (var epi in Project.ExternalProjects)
                            {
                                epi.ProjectProvider.AddCompileUnit(Path.Combine(epi.OutputPath, fileName));
                            }

                            using (TextWriter tw = new StreamWriter(Path.Combine(Project.OutputPath, fileName)))
                            {
                                tw.Write(resultString);
                            }
                        }
                    }
                    ccu.Namespaces.Clear();
                }

                nspace = CreateBaseNamespace(_schema);
                ccu.Namespaces.Add(nspace);


                //{wash
                if (_schema.Enums.Count > 0)
                {
                    //ccu.StartDirectives.Add(new CodeRegionDirective(CodeRegionMode.Start, "Enums"));

                    Output.Verbose("    * enums");

                    var i = 0;
                    foreach (var ei in _schema.Enums)
                    {
                        var ctd = new CodeTypeDeclaration();

                        if (i == 0)
                            ctd.StartDirectives.Add(new CodeRegionDirective(CodeRegionMode.Start, "Enums"));
                        if (++i == _schema.Enums.Count)
                            ctd.EndDirectives.Add(new CodeRegionDirective(CodeRegionMode.End, "Enums"));

                        ctd.IsEnum = true;
                        ctd.Name = ei.Name;
                        ctd.Attributes = MemberAttributes.Public;

                        ctd.CustomAttributes.Add(
                            new CodeAttributeDeclaration("System.ComponentModel.TypeConverterAttribute",
                                new CodeAttributeArgument(
                                    new CodeTypeOfExpression("Sooda.AttributesEnumConverter"))));
                        ctd.Comments.Add(new CodeCommentStatement(ei.Label));

                        foreach (var value in ei.Values)
                        {
                            var field = new CodeMemberField
                            {
                                Name = value.Name,
                                InitExpression = new
                                    CodePrimitiveExpression(Convert.ToInt32(value.Key))
                            };
                            field.CustomAttributes.Add(
                                new CodeAttributeDeclaration("Sooda.EnumDescriptionAttribute",
                                    new CodeAttributeArgument(
                                        new CodePrimitiveExpression(value.Label))));
                            ctd.Members.Add(field);
                        }

                        nspace.Types.Add(ctd);
                    }
                    //ccu.EndDirectives.Add(new CodeRegionDirective(CodeRegionMode.End, "Enums"));
                }
                //}wash

                GenerateDatabaseSchema(nspace, _schema);

                using (var sw = new StringWriter())
                {
                    Output.Verbose("Writing code...");
                    codeGenerator.GenerateCodeFromCompileUnit(ccu, sw, _codeGeneratorOptions);
                    Output.Verbose("Done.");

                    var resultString = sw.ToString();
                    resultString = resultString.Replace("[System.ParamArrayAttribute()] ", "params ");

                    using (TextWriter tw = new StreamWriter(stubsFileName))
                    {
                        tw.Write(resultString);
                    }
                }

                SaveExternalProjects();
            }
            catch (SoodaCodeGenException)
            {
                throw;
            }
            catch (SoodaSchemaException e)
            {
                throw new SoodaCodeGenException("Schema validation error.", e);
            }
            catch (ApplicationException e)
            {
                throw new SoodaCodeGenException("Error generating code.", e);
            }
            catch (Exception e)
            {
                throw new SoodaCodeGenException("Unexpected error.", e);
            }
        }

        private CodeDomProvider GetCodeProvider(string lang)
        {
            if (lang == null)
                return new CSharpCodeProvider();

            switch (lang.ToLower())
            {
                case "cs":
                case "c#":
                case "csharp":
                    return new CSharpCodeProvider();
#if !NO_VB

                case "vb":
                    return new VBCodeProvider();
#endif

                case "c++/cli":
                    return
                        GetCodeProvider(
                            "Microsoft.VisualC.CppCodeProvider, CppCodeProvider, Version=8.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a, processorArchitecture=MSIL");

                default:
                {
                    var cdp = Activator.CreateInstance(Type.GetType(lang, true)) as CodeDomProvider;
                    if (cdp == null)
                        throw new SoodaCodeGenException("Cannot instantiate type " + lang);
                    return cdp;
                }
            }
        }

        private IProjectFile GetProjectProvider(string projectType, CodeDomProvider codeProvider)
        {
            if (projectType == "vs2005")
            {
                switch (codeProvider.FileExtension)
                {
                    case "cs":
                        return new VS2005csprojProjectFile();

                    case "vb":
                        return new VS2005vbprojProjectFile();

                    default:
                        throw new Exception("Visual Studio 2005 Project not supported for '" +
                                            codeProvider.FileExtension + "' files");
                }
            }
            if (projectType == "null")
            {
                return new NullProjectFile();
            }
            return Activator.CreateInstance(Type.GetType(projectType, true)) as IProjectFile;
        }

        private void AddImportsFromIncludedSchema(CodeNamespace nspace, IEnumerable<IncludeInfo> includes,
            bool stubsSubnamespace)
        {
            if (includes == null)
                return;

            foreach (var ii in includes)
            {
                if (!string.IsNullOrEmpty(ii.SchemaFile))
                {
                    if (!string.IsNullOrEmpty(ii.Namespace))
                        nspace.Imports.Add(new CodeNamespaceImport(ii.Namespace + (stubsSubnamespace ? ".Stubs" : "")));
                    AddImportsFromIncludedSchema(nspace, ii.Schema.Includes, stubsSubnamespace);
                }
                else if (!string.IsNullOrEmpty(ii.Namespace))
                {
                    nspace.Imports.Add(new CodeNamespaceImport(ii.Namespace));
                }
            }
        }

        private void AddTypedQueryImportsFromIncludedSchema(CodeNamespace nspace, IEnumerable<IncludeInfo> includes)
        {
            if (includes == null)
                return;

            foreach (var ii in includes)
            {
                if (!string.IsNullOrEmpty(ii.SchemaFile))
                {
                    if (!string.IsNullOrEmpty(ii.Namespace))
                        nspace.Imports.Add(new CodeNamespaceImport(ii.Namespace + ".TypedQueries"));
                    AddTypedQueryImportsFromIncludedSchema(nspace, ii.Schema.Includes);
                }
            }
        }

        private CodeNamespace CreateTypedQueriesNamespace(SchemaInfo schema)
        {
            var nspace = new CodeNamespace(Project.OutputNamespace + ".TypedQueries");
            nspace.Imports.Add(new CodeNamespaceImport("System"));
            nspace.Imports.Add(new CodeNamespaceImport("System.Collections"));
            nspace.Imports.Add(new CodeNamespaceImport("System.Diagnostics"));
            nspace.Imports.Add(new CodeNamespaceImport("System.Data"));
            nspace.Imports.Add(new CodeNamespaceImport("Sooda"));
            nspace.Imports.Add(new CodeNamespaceImport(Project.OutputNamespace + ".Stubs"));
            AddImportsFromIncludedSchema(nspace, schema.Includes, false);
            AddImportsFromIncludedSchema(nspace, schema.Includes, true);
            AddTypedQueryImportsFromIncludedSchema(nspace, schema.Includes);
            return nspace;
        }

        private CodeNamespace CreateBaseNamespace(SchemaInfo schema)
        {
            var nspace = new CodeNamespace(Project.OutputNamespace);
            nspace.Imports.Add(new CodeNamespaceImport("System"));
            nspace.Imports.Add(new CodeNamespaceImport("System.Collections"));
            nspace.Imports.Add(new CodeNamespaceImport("System.Diagnostics"));
            nspace.Imports.Add(new CodeNamespaceImport("System.Data"));
            nspace.Imports.Add(new CodeNamespaceImport("Sooda"));
            nspace.Imports.Add(
                new CodeNamespaceImport(Project.OutputNamespace.Replace(".", "") + "Stubs = " + Project.OutputNamespace +
                                        ".Stubs"));
            AddImportsFromIncludedSchema(nspace, schema.Includes, false);
            return nspace;
        }

        private CodeNamespace CreatePartialNamespace()
        {
            return new CodeNamespace(Project.OutputNamespace);
        }

        private CodeNamespace CreateStubsNamespace(SchemaInfo schema)
        {
            var nspace = new CodeNamespace(Project.OutputNamespace + ".Stubs");
            nspace.Imports.Add(new CodeNamespaceImport("System"));
            nspace.Imports.Add(new CodeNamespaceImport("System.Collections"));
            nspace.Imports.Add(new CodeNamespaceImport("System.Diagnostics"));
            nspace.Imports.Add(new CodeNamespaceImport("System.Data"));
            nspace.Imports.Add(new CodeNamespaceImport("Sooda"));
            nspace.Imports.Add(new CodeNamespaceImport("Sooda.ObjectMapper"));
            nspace.Imports.Add(
                new CodeNamespaceImport(Project.OutputNamespace.Replace(".", "") + " = " + Project.OutputNamespace));
            AddImportsFromIncludedSchema(nspace, schema.Includes, false);
            AddImportsFromIncludedSchema(nspace, schema.Includes, true);
            return nspace;
        }
    }

    // ReSharper restore BitwiseOperatorOnEnumWithoutFlags
}