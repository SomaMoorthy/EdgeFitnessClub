using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;

namespace SUTIAPGPIntegrationService.Utilities
{
    public class FileLogger
    {
        public static void WriteToFileLog(string Entry,string filePath, EventLogEntryType EntryType = EventLogEntryType.Information)
        {

            using (StreamWriter w = File.AppendText(filePath +"\\"+"log.txt"))
            {
                Log(Entry, w);
                 
            }

            //StringBuilder w = new StringBuilder(filePath+"\\log.txt");
             
            //w.Append("\r\nLog Entry : " + EntryType);
            //w.Append($"{DateTime.Now.ToLongTimeString()} {DateTime.Now.ToLongDateString()}");
            //w.Append("  :");
            //w.Append($"  :{Entry}");
            //w.Append("-------------------------------");

        }

        public static void Log(string logMessage, TextWriter w)
        {
            w.Write("\r\nLog Entry : ");
            w.WriteLine($"{DateTime.Now.ToLongTimeString()} {DateTime.Now.ToLongDateString()}");
            w.WriteLine("  :");
            w.WriteLine($"  :{logMessage}");
            w.WriteLine("-------------------------------");
        }
    }
}
