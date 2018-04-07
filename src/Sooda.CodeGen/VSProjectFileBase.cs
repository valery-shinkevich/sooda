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

    public class VSProjectFileBase : IProjectFile
    {
        protected XmlDocument doc = new XmlDocument();
        protected string projectRoot;
        protected string projectExtension;
        protected string templateName;
        protected bool modified;

        protected VSProjectFileBase(string projectRoot, string projectExtension, string templateName)
        {
            this.projectRoot = projectRoot;
            this.projectExtension = projectExtension;
            this.templateName = templateName;
        }

        public virtual void CreateNew(string outputNamespace, string assemblyName)
        {
            doc = new XmlDocument();
            using (Stream ins = typeof (CodeGenerator).Assembly.GetManifestResourceStream(templateName))
            {
                doc.Load(ins);
            }
            modified = true;
            XmlElement el = (XmlElement) doc.SelectSingleNode(projectRoot);
            if (!el.HasAttribute("AssemblyName"))
            {
                el.SetAttribute("AssemblyName", assemblyName);
            }
        }

        void IProjectFile.LoadFrom(string fileName)
        {
            doc = new XmlDocument();
            doc.Load(fileName);
            modified = false;
        }

        void IProjectFile.SaveTo(string fileName)
        {
            XmlElement el = (XmlElement) doc.SelectSingleNode(projectRoot);
            if (!el.HasAttribute("ProjectGuid"))
            {
                Guid g = Guid.NewGuid();
                el.SetAttribute("ProjectGuid", "{" + g.ToString().ToUpper() + "}");
                modified = true;
            }

            if (modified)
            {
                doc.Save(fileName);
            }
        }

        void IProjectFile.AddCompileUnit(string relativeFileName)
        {
            XmlNodeList nl = doc.SelectNodes(projectRoot + "/Files/Include/File[@RelPath='" + relativeFileName + "']");

            if (nl.Count == 0)
            {
                XmlElement el = doc.CreateElement("File");
                el.SetAttribute("RelPath", relativeFileName);
                el.SetAttribute("BuildAction", "Compile");
                doc.SelectSingleNode(projectRoot + "/Files/Include").AppendChild(el);
                modified = true;
            }
        }

        public void AddCompileUnit(string relativeFileName, string dependentFileName)
        {
            throw new NotImplementedException();
        }

        void IProjectFile.AddResource(string relativeFileName)
        {
            XmlNodeList nl = doc.SelectNodes(projectRoot + "/Files/Include/File[@RelPath='" + relativeFileName + "']");

            if (nl.Count == 0)
            {
                XmlElement el = doc.CreateElement("File");
                el.SetAttribute("RelPath", relativeFileName);
                el.SetAttribute("BuildAction", "EmbeddedResource");
                doc.SelectSingleNode(projectRoot + "/Files/Include").AppendChild(el);
                modified = true;
            }
        }

        string IProjectFile.GetProjectFileName(string outNamespace)
        {
            return outNamespace + projectExtension;
        }
    }
}