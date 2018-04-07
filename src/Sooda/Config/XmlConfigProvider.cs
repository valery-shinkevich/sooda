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

namespace Sooda.Config
{
    using System;
    using System.Collections.Specialized;
    using System.Configuration;
    using System.IO;
    using System.Security;
    using System.Security.Cryptography;
    using System.Security.Cryptography.Xml;
    using System.Xml;
    using Logging;

    public class XmlConfigProvider : ISoodaConfigProvider
    {
        private static readonly Logger _logger = LogManager.GetLogger("Sooda.Config");
        private readonly NameValueCollection _dataDictionary = new NameValueCollection();
        private readonly string _fileName;
        private readonly XmlDocument _doc = new XmlDocument();

        public XmlConfigProvider()
            : this("Sooda.config.xml")
        {
        }

        public XmlConfigProvider(string fileName)
        {
            InitSooda.Check();
            _fileName = fileName;
            LoadFromFile(fileName, null);
            FindAndLoadOverride();
        }

        public XmlConfigProvider(string fileName, string xpathExpression)
        {
            InitSooda.Check();
            _fileName = fileName;
            LoadFromFile(fileName, xpathExpression);
            FindAndLoadOverride();
        }


        public void Clear()
        {
            _dataDictionary.Clear();
        }

        public void LoadFromFile(string fileName, string xpathExpression)
        {
            LoadFromFile(fileName, xpathExpression, "");
        }

        private void LoadFromFile(string fileName, string xpathExpression, string prefix)
        {
            //_doc.PreserveWhitespace = true; 
            _doc.Load(fileName);

            var csp = new CspParameters
            {
                //KeyContainerName = "XML_ENC_RSA_KEY",
                Flags = CspProviderFlags.UseDefaultKeyContainer
            };

            RSA rsaKey = new RSACryptoServiceProvider(csp);

            XMLenc.Decrypt(_doc, rsaKey, "rsaKey");

            XmlNode startingNode = _doc.DocumentElement;

            if (xpathExpression != null)
                startingNode = _doc.SelectSingleNode(xpathExpression);

            LoadFromNode(startingNode, prefix, Path.GetDirectoryName(fileName));
        }

        private void LoadFromNode(XmlNode startingNode, string prefix, string baseDir)
        {
            for (var node = startingNode.FirstChild;
                node != null;
                node = node.NextSibling)
                if (node.NodeType == XmlNodeType.Element)
                {
                    if (node.FirstChild != null && node.FirstChild.NodeType == XmlNodeType.Element)
                    {
                        string newPrefix;

                        if (prefix.Length == 0)
                            newPrefix = node.Name + ".";
                        else
                            newPrefix = prefix + node.Name + "."; //wash
                        LoadFromNode(node, newPrefix, baseDir);
                    }
                    else
                    {
                        var keyName = prefix + node.Name;

                        var value = ProcessValue(node.InnerXml, baseDir);
                        _logger.Debug("{0}={1}", keyName, value);
                        _dataDictionary[keyName] = value;
                    }
                }
        }

        public void FindAndLoadOverride()
        {
            var fi = new FileInfo(_fileName);
            var originalExtension = fi.Extension;

            var newExtension = ".site" + originalExtension;
            var newFileName = Path.ChangeExtension(_fileName, newExtension);

            if (File.Exists(newFileName))
            {
                _logger.Debug("Loading config override from file: " + newFileName);
                LoadFromFile(newFileName, null);
            }
            else
            {
                _logger.Debug("Config override file " + newFileName + " not found");
            }

            newExtension = "." + GetMachineName().ToLower(System.Globalization.CultureInfo.CurrentCulture) +
                           originalExtension;
            newFileName = Path.ChangeExtension(_fileName, newExtension);

            if (File.Exists(newFileName))
            {
                _logger.Debug("Loading config override from file: " + newFileName);
                LoadFromFile(newFileName, null);
            }
            else
            {
                _logger.Debug("Config override file " + newFileName + " not found");
            }

            OverrideFromAppConfig();
        }

        public string GetMachineName()
        {
            var overrideMachineName = ConfigurationManager.AppSettings["sooda.hostname"];
            return overrideMachineName ?? Environment.MachineName;
        }

        private static string ProcessValue(string s, string baseDir)
        {
            s = s.Replace("${CONFIGDIR}", baseDir);
            // Console.WriteLine("After replacement: {0}", s);
            return s;
        }

        #region ISoodaConfigProvider Members

        public string GetString(string key)
        {
            return _dataDictionary[key];
        }

        public void SetString(string key, string value)
        {
            _dataDictionary[key] = value;

            XmlNode startingNode = _doc.DocumentElement;

            if (SetNode(startingNode, string.Empty, key, value))
            {
                _doc.Save(_fileName);
            }
            else
                throw new NotImplementedException();
        }

        private static bool SetNode(XmlNode startingNode, string prefix, string key, string value)
        {
            bool result = false;
            for (XmlNode node = startingNode.FirstChild;
                node != null;
                node = node.NextSibling)
            {
                if (node.NodeType != XmlNodeType.Element) continue;

                if (node.FirstChild == null ||
                    node.FirstChild.NodeType != XmlNodeType.Element)
                {
                    var keyName = prefix + node.Name;

                    if (string.Compare(keyName, key, true) == 0)
                    {
                        result = true;
                        _logger.Debug("Setting {0}={1}", keyName, value);
                        node.InnerXml = value;
                    }
                }
                else
                {
                    string newPrefix;

                    if (prefix.Length == 0)
                        newPrefix = node.Name + ".";
                    else
                        newPrefix = prefix + node.Name + "."; //wash
                    result = SetNode(node, newPrefix, key, value);
                }

                if (result) return true;
            }
            return false;
        }

        #endregion

        public static XmlConfigProvider FindConfigFile(string fileName)
        {
            return FindConfigFile(fileName, 10);
        }

        private static XmlConfigProvider TryFindConfigFile(string fileName, int maxParentDirectories)
        {
            FileInfo fi;

            try
            {
                fi = new FileInfo(fileName);
                var depth = maxParentDirectories;
                for (var di = fi.Directory; (di != null) && (depth > 0); di = di.Parent, depth--)
                {
                    var targetFileName = Path.Combine(di.FullName, fi.Name);
                    //_logger.Debug("Checking for " + targetFileName);
                    if (File.Exists(targetFileName))
                        return new XmlConfigProvider(targetFileName);
                }
                return null;
            }
            catch (UnauthorizedAccessException)
            {
                // ignore
                return null;
            }
        }

        public static XmlConfigProvider FindConfigFile(string fileName, int maxParentDirectories)
        {
            //
            // Path.Combine will either take the file name (if it's rooted) or make a relative
            // to the specified base directory
            //

            try
            {
                var retVal = TryFindConfigFile(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, fileName),
                    maxParentDirectories);
                if (retVal != null)
                    return retVal;
            }
            catch (SecurityException e)
            {
                // ignore
                _logger.Debug("SecurityException when scanning AppDomain.BaseDirectory", e);
            }

            try
            {
                var retVal = TryFindConfigFile(Path.Combine(Directory.GetCurrentDirectory(), fileName),
                    maxParentDirectories);
                if (retVal != null)
                    return retVal;
            }
            catch (SecurityException e)
            {
                // ignore
                _logger.Debug("SecurityException when scanning Directory.GetCurrentDirectory()", e);
            }

            throw new SoodaConfigException("Config file not found in " + fileName + " and " + maxParentDirectories +
                                           " parent directories");
        }

        private void OverrideFromAppConfig()
        {
            foreach (string s in ConfigurationManager.AppSettings.Keys)
            {
                _dataDictionary[s] = ConfigurationManager.AppSettings[s];
            }
        }
    }

    public static class XMLenc
    {
        public static void Encrypt(XmlDocument Doc, string ElementToEncrypt, RSA Alg, string KeyName)
        {
            // Check the arguments.  
            if (Doc == null)
                throw new ArgumentNullException("Doc");
            if (ElementToEncrypt == null)
                throw new ArgumentNullException("ElementToEncrypt");
            if (Alg == null)
                throw new ArgumentNullException("Alg");

            ////////////////////////////////////////////////
            // Find the specified element in the XmlDocument
            // object and create a new XmlElemnt object.
            ////////////////////////////////////////////////

            var elementToEncrypt = Doc.GetElementsByTagName(ElementToEncrypt)[0] as XmlElement;

            // Throw an XmlException if the element was not found.
            if (elementToEncrypt == null)
            {
                throw new XmlException("The specified element was not found");
            }

            //////////////////////////////////////////////////
            // Create a new instance of the EncryptedXml class 
            // and use it to encrypt the XmlElement with the 
            // a new random symmetric key.
            //////////////////////////////////////////////////

            // Create a 256 bit Rijndael key.
            var sessionKey = new RijndaelManaged {KeySize = 256};

            var eXml = new EncryptedXml();

            var encryptedElement = eXml.EncryptData(elementToEncrypt, sessionKey, false);

            ////////////////////////////////////////////////
            // Construct an EncryptedData object and populate
            // it with the desired encryption information.
            ////////////////////////////////////////////////


            var edElement = new EncryptedData
            {
                Type = EncryptedXml.XmlEncElementUrl,
                EncryptionMethod = new EncryptionMethod(EncryptedXml.XmlEncAES256Url)
            };

            // Create an EncryptionMethod element so that the 
            // receiver knows which algorithm to use for decryption.

            // Encrypt the session key and add it to an EncryptedKey element.
            var ek = new EncryptedKey();

            var encryptedKey = EncryptedXml.EncryptKey(sessionKey.Key, Alg, false);

            ek.CipherData = new CipherData(encryptedKey);

            ek.EncryptionMethod = new EncryptionMethod(EncryptedXml.XmlEncRSA15Url);

            // Set the KeyInfo element to specify the
            // name of the RSA key.

            // Create a new KeyInfo element.
            edElement.KeyInfo = new KeyInfo();

            // Create a new KeyInfoName element.
            var kin = new KeyInfoName {Value = KeyName};

            // Specify a name for the key.

            // Add the KeyInfoName element to the 
            // EncryptedKey object.
            ek.KeyInfo.AddClause(kin);

            // Add the encrypted key to the 
            // EncryptedData object.

            edElement.KeyInfo.AddClause(new KeyInfoEncryptedKey(ek));

            // Add the encrypted element data to the 
            // EncryptedData object.
            edElement.CipherData.CipherValue = encryptedElement;

            ////////////////////////////////////////////////////
            // Replace the element from the original XmlDocument
            // object with the EncryptedData element.
            ////////////////////////////////////////////////////

            EncryptedXml.ReplaceElement(elementToEncrypt, edElement, false);
        }

        public static void Decrypt(XmlDocument Doc, RSA Alg, string KeyName)
        {
            // Check the arguments.  
            if (Doc == null)
                throw new ArgumentNullException("Doc");
            if (Alg == null)
                throw new ArgumentNullException("Alg");
            if (KeyName == null)
                throw new ArgumentNullException("KeyName");

            // Create a new EncryptedXml object.
            var exml = new EncryptedXml(Doc);

            // Add a key-name mapping.
            // This method can only decrypt documents
            // that present the specified key name.
            exml.AddKeyNameMapping(KeyName, Alg);

            // Decrypt the element.
            exml.DecryptDocument();
        }
    }
}