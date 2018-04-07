using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Xml;

namespace FixSchemaGuids
{
    class Program
    {
        private static int Usage()
        {
            Console.WriteLine("Usage: FixSchemaGuids filename.xml");
            Console.WriteLine();
            return 1;
        }

        public static int Main(string[] args)
        {
            try
            {
                if(args.Length!=1)
                {                    
                    return Usage();
                }
                FixSchemaFile(args[0]);
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: " + ex.Message);
                if (ex.InnerException != null)
                    Console.WriteLine(ex.InnerException);
                return 1;
            }
        }

        private static void FixSchemaFile(string filename)
        {
            Boolean ischanged = false;
            XmlDocument xdoc = new XmlDocument();
            xdoc.Load(filename);
            XmlElement root = xdoc.DocumentElement;
            foreach (XmlElement xclass in root.GetElementsByTagName("class"))
            {
                foreach (XmlElement xtable in xclass.GetElementsByTagName("table"))
                {
                    if(string.IsNullOrEmpty(xtable.GetAttribute("uid")))
                    {
                        Console.WriteLine("...fix table '{0}'", xtable.Name);
                        xtable.SetAttribute("uid", Guid.NewGuid().ToString());
                        ischanged = true;
                    }
                    foreach (XmlElement xfield in xtable.GetElementsByTagName("field"))
                    {
                        if (string.IsNullOrEmpty(xfield.GetAttribute("uid")))
                        {
                            Console.WriteLine("...fix field '{0}.{1}'", xtable.Name, xfield.Name);
                            xfield.SetAttribute("uid", Guid.NewGuid().ToString());
                            ischanged = true;
                        }
                    }
                }
            }            
            if(ischanged)
                xdoc.Save(filename);
            else 
                Console.WriteLine("tables & fields not changed...");
        }
    }
}
