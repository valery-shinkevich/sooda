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
    using System.IO;
    using System.Xml;

    public class VS2005ProjectFileBase : IProjectFile
    {
        protected XmlDocument doc = new XmlDocument();
        protected string projectExtension;
        protected string templateName;
        protected bool modified;
        protected XmlNamespaceManager namespaceManager;

        protected VS2005ProjectFileBase(string projectExtension, string templateName)
        {
            this.projectExtension = projectExtension;
            this.templateName = templateName;
        }

        public virtual void CreateNew(string outputNamespace, string assemblyName)
        {
            doc = new XmlDocument();
            namespaceManager = new XmlNamespaceManager(doc.NameTable);
            namespaceManager.AddNamespace("msbuild", "http://schemas.microsoft.com/developer/msbuild/2003");
            using (Stream ins = typeof (CodeGenerator).Assembly.GetManifestResourceStream(templateName))
            {
                doc.Load(ins);
            }
            modified = true;
            XmlElement assemblyNameElement =
                (XmlElement)
                    doc.SelectSingleNode(
                        "msbuild:Project/msbuild:PropertyGroup[msbuild:OutputType]/msbuild:AssemblyName",
                        namespaceManager);
            if (assemblyNameElement != null && assemblyNameElement.InnerText.Trim() == "")
            {
                assemblyNameElement.InnerText = assemblyName;
            }
        }

        void IProjectFile.LoadFrom(string fileName)
        {
            doc = new XmlDocument();
            doc.Load(fileName);
            namespaceManager = new XmlNamespaceManager(doc.NameTable);
            namespaceManager.AddNamespace("msbuild", "http://schemas.microsoft.com/developer/msbuild/2003");
            modified = false;
        }

        void IProjectFile.SaveTo(string fileName)
        {
            XmlElement projectGuid =
                (XmlElement)
                    doc.SelectSingleNode("msbuild:Project/msbuild:PropertyGroup/msbuild:ProjectGuid", namespaceManager);
            if (projectGuid != null && projectGuid.InnerText.Trim() == "")
            {
                Guid g = Guid.NewGuid();
                projectGuid.InnerText = "{" + g.ToString().ToUpper() + "}";
                modified = true;
            }

            if (modified)
            {
                doc.Save(fileName);
            }
        }

        private XmlElement GetItemGroup(string whichHas)
        {
            XmlElement itemGroup =
                (XmlElement)
                    doc.SelectSingleNode("msbuild:Project/msbuild:ItemGroup[msbuild:" + whichHas + "]", namespaceManager);
            if (itemGroup == null)
            {
                itemGroup = doc.CreateElement("", "ItemGroup", "http://schemas.microsoft.com/developer/msbuild/2003");
                if (doc.DocumentElement != null) doc.DocumentElement.AppendChild(itemGroup);
            }
            return itemGroup;
        }


        void IProjectFile.AddCompileUnit(string relativeFileName)
        {
            XmlElement compileItemGroup = GetItemGroup("Compile");

            XmlElement file =
                (XmlElement)
                    compileItemGroup.SelectSingleNode("msbuild:Compile[@Include='" + relativeFileName + "']",
                        namespaceManager);
            if (file == null)
            {
                XmlElement el = doc.CreateElement("", "Compile", "http://schemas.microsoft.com/developer/msbuild/2003");
                el.SetAttribute("Include", relativeFileName);
                compileItemGroup.AppendChild(el);
                modified = true;
            }
        }

        void IProjectFile.AddCompileUnit(string relativeFileName, string dependentFileName)
        {
            XmlElement compileItemGroup = GetItemGroup("Compile");

            var file =
                (XmlElement)
                    compileItemGroup.SelectSingleNode("msbuild:Compile[@Include='" + relativeFileName + "']",
                        namespaceManager);
            if (file == null)
            {
                XmlElement el = doc.CreateElement("", "Compile", "http://schemas.microsoft.com/developer/msbuild/2003");
                el.SetAttribute("Include", relativeFileName);
                if (!string.IsNullOrEmpty(dependentFileName))
                {
                    var el1 = doc.CreateElement("", "AutoGen", "http://schemas.microsoft.com/developer/msbuild/2003");
                    el1.InnerText = "True";

                    var el2 = doc.CreateElement("", "DependentUpon",
                        "http://schemas.microsoft.com/developer/msbuild/2003");
                    el2.InnerText = dependentFileName;

                    el.AppendChild(el1);
                    el.AppendChild(el2);
                }
                compileItemGroup.AppendChild(el);
                modified = true;
            }
        }

        void IProjectFile.AddResource(string relativeFileName)
        {
            XmlElement compileItemGroup = GetItemGroup("Compile");

            XmlElement file =
                (XmlElement)
                    compileItemGroup.SelectSingleNode("msbuild:EmbeddedResource[@Include='" + relativeFileName + "']",
                        namespaceManager);
            if (file == null)
            {
                XmlElement el = doc.CreateElement("", "EmbeddedResource",
                    "http://schemas.microsoft.com/developer/msbuild/2003");
                el.SetAttribute("Include", relativeFileName);
                compileItemGroup.AppendChild(el);
                modified = true;
            }
        }

        string IProjectFile.GetProjectFileName(string outNamespace)
        {
            return outNamespace + projectExtension;
        }
    }
}