namespace Sooda.Config
{
    using System.Collections.Generic;
    using Microsoft.Win32;

    public class RegistryConfigProvider : ISoodaConfigProvider
    {
        public enum RegistryConfigMode
        {
            CurrentUser,
            LocalMashine
        }

        private readonly RegistryConfigMode _Mode;

        private readonly Dictionary<string, string> _DataDictionary = new Dictionary<string, string>();

        private readonly string _AppRegistryPath;

        public static string ConnectionName { get; set; }

        public RegistryConfigProvider(RegistryConfigMode mode, string appRegistryPath)
        {
            _Mode = mode;
            _AppRegistryPath = appRegistryPath;
            LoadFromRegistry();
        }

        public void Clear()
        {
            _DataDictionary.Clear();
        }

        private void LoadFromRegistry()
        {
            var rkey = _Mode == RegistryConfigMode.LocalMashine ? Registry.LocalMachine : Registry.CurrentUser;
            var sr = rkey.OpenSubKey(_AppRegistryPath, false);
            if (sr != null)
            {
                foreach (var s in sr.GetValueNames())
                {
                    _DataDictionary.Add(s.ToLower(), sr.GetValue(s).ToString());
                }
                sr.Close();
            }

            rkey.Close();
        }

        #region ISoodaConfigProvider Members

        public string GetString(string key)
        {
            var lkey = key.ToLower();
            return _DataDictionary.ContainsKey(lkey) ? _DataDictionary[lkey] : null;
        }

        public void SetString(string key, string value)
        {
            var lkey = key.ToLower();
            if (_DataDictionary.ContainsKey(lkey))
            {
                _DataDictionary[lkey] = value;
            }
            else
            {
                _DataDictionary.Add(lkey, value);
            }

            var rkey = _Mode == RegistryConfigMode.LocalMashine ? Registry.LocalMachine : Registry.CurrentUser;
            var sk = rkey.OpenSubKey(_AppRegistryPath, true);
            if (sk == null) return;
            sk.SetValue(key, value);
            sk.Close();
        }

        #endregion
    }
}