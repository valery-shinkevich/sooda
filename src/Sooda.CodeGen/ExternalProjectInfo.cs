namespace Sooda.CodeGen
{
    using System.Xml.Serialization;

    public class ExternalProjectInfo
    {
        public ExternalProjectInfo()
        {
        }

        public ExternalProjectInfo(string projectType)
        {
            ProjectType = projectType;
        }

        public ExternalProjectInfo(string projectType, string projectFile)
        {
            ProjectType = projectType;
            ProjectFile = projectFile;
        }

        [XmlIgnore] public IProjectFile ProjectProvider;

        [XmlIgnore] public string ActualProjectFile;

        [XmlAttribute("type")] public string ProjectType;

        [XmlAttribute("file")] public string ProjectFile;

        [XmlIgnore] public string OutputPath;
    }
}