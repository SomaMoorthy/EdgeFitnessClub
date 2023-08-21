using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace SUTIAPGPIntegrationService.Utilities
{
    public class EventLogger
    {
        public static void WriteToEventLog(string Entry, EventLogEntryType EntryType = EventLogEntryType.Information)
        {

            if (Utilities.GlobalVariables.CurrentLogLevel == 3)
            {
                dynamic logName = "Application";
                EventLog objEventLog = new EventLog();
                try
                {
                    //Register the App as an Event Source
                    if (!EventLog.SourceExists(Utilities.GlobalVariables.AppTitle))
                    {
                        EventLog.CreateEventSource(Utilities.GlobalVariables.AppTitle, logName);
                    }
                    objEventLog.Source = Utilities.GlobalVariables.AppTitle;
                    objEventLog.WriteEntry(Entry, EntryType);
                    Debug.WriteLineIf(Utilities.GlobalVariables.AppDebug, Entry);
                }
                catch (Exception Ex)
                {
                }
            }
        }
    }
}
