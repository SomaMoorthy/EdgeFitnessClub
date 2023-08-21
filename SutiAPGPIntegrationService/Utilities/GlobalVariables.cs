using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SUTIAPGPIntegrationService.Utilities
{
    class GlobalVariables
    {
        private static string appTitle = "SUTIAPGPIntegration Service";
        public static string AppTitle
        {
            get { return appTitle; }
            set { appTitle = value; }
        }


        private static bool appDebug = false;
        public static bool AppDebug
        {
            get { return appDebug; }
            set { appDebug = value; }
        }

        private static int currentLogLevel = 3;
        public static int CurrentLogLevel
        {
            get { return currentLogLevel; }
            set { currentLogLevel = value; }
        }

        private static string appSettingsFile = "AppConfig.xml";
        public static string AppSettingsFile
        {
            get { return appSettingsFile; }
            set { appSettingsFile = value; }
        }
    }
}
