using Sooda.Schema;
using System.IO;
using System.Xml;
using System.Xml.Serialization;

namespace SoodaSchemaTool
{
    using System;

    [Command("genfullschema", "Generate single file for merged schema")]
    public class CommandGenFullSchema : Command
    {
        static int CompareNames(ClassInfo x, ClassInfo y)
        {
            return String.Compare(x.Name, y.Name, StringComparison.Ordinal);
        }

        static int CompareNames(RelationInfo x, RelationInfo y)
        {
            return String.Compare(x.Name, y.Name, StringComparison.Ordinal);
        }

        public override int Run(string[] args)
        {
            var schemaFileName = args[0];
            var outschemaFileName = args[1];

            var xr = new XmlTextReader(schemaFileName);
            var schemaInfo = SchemaManager.ReadAndValidateSchema(xr, Path.GetDirectoryName(schemaFileName));
            schemaInfo.Includes.Clear();

            schemaInfo.Classes.Sort(CompareNames);
            schemaInfo.Relations.Sort(CompareNames); 
            
            var ser = new XmlSerializer(typeof (SchemaInfo));
            var ns = new XmlSerializerNamespaces();
            ns.Add("", SchemaInfo.XmlNamespace);

            using (var fs = File.Create(outschemaFileName))
            {
                try
                {
                    ser.Serialize(fs, schemaInfo, ns);
                }
                finally
                {
                    fs.Flush();
                    fs.Close();
                }
            }

            return 0;
        }
    }
}