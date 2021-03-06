//
// Copyright (c) 2003-2006 Jaroslaw Kowalski <jaak@jkowalski.net>
// Copyright (c) 2006-2014 Piotr Fusik <piotr@fusik.info>
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

using Sooda.Schema;
using System;
using System.Collections;
using System.IO;
using System.Xml;
using System.Xml.Serialization;

namespace Sooda.ObjectMapper
{
    public class SchemaLoader
    {
        private static readonly Hashtable assembly2SchemaInfo = new Hashtable();

        public static SchemaInfo GetSchemaFromAssembly(System.Reflection.Assembly ass)
        {
            SchemaInfo schemaInfo = (SchemaInfo) assembly2SchemaInfo[ass];

            if (schemaInfo == null)
            {
                lock (typeof(SchemaLoader))
                {
                    schemaInfo = (SchemaInfo) assembly2SchemaInfo[ass];
                    if (schemaInfo == null)
                    {
                        foreach (string name in ass.GetManifestResourceNames())
                        {
                            if (name.EndsWith("_DBSchema.bin"))
                            {
                                System.Runtime.Serialization.Formatters.Binary.BinaryFormatter bf = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                                using (Stream resourceStream = ass.GetManifestResourceStream(name))
                                {
                                    schemaInfo = (SchemaInfo) bf.Deserialize(resourceStream);
                                    schemaInfo.Resolve();
                                }
                                break;
                            }
                            if (name.EndsWith("_DBSchema.xml"))
                            {
                                using (Stream resourceStream = ass.GetManifestResourceStream(name))
                                {
                                    XmlSerializer ser = new XmlSerializer(typeof(SchemaInfo));
                                    XmlTextReader reader = new XmlTextReader(resourceStream);

                                    schemaInfo = (SchemaInfo) ser.Deserialize(reader);
                                    schemaInfo.Resolve();
                                }
                                break;
                            }
                        }
                        if (schemaInfo == null)
                        {
                            throw new InvalidOperationException("_DBSchema.xml not embedded in " + ass.CodeBase);
                        }
                        assembly2SchemaInfo[ass] = schemaInfo;
                    }
                }
            }
            return schemaInfo;
        }
    }
}
