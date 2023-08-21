using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Mail;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace SUTIAPGPIntegrationService.Utilities
{
    public class NotificationsMail
    {

        public static string sendNotifications(string strSubject, string strBody, string strFromAddress, string strToAddress,string strCCAddress, string strConnString)
        {
            string errMessage = "";
            string strSQL = SQLLibrary.SQLSMTPConfigdetails();

            DataSet dsPaymentSetup = DBLibrary.GetDataSetFromSQLSCript(strSQL, "SMTPConfig", strConnString);

           
            string strHostAddress = dsPaymentSetup.Tables["SMTPConfig"].Rows[0]["SFTPHOSTNAME"].ToString().Trim();
            int iHostPort = Convert.ToInt32(dsPaymentSetup.Tables["SMTPConfig"].Rows[0]["SFTPPORTNUMBER"].ToString().Trim());
            string strSMTPUserID = dsPaymentSetup.Tables["SMTPConfig"].Rows[0]["SFTPUSERNAME"].ToString().Trim();
            string strPassword = dsPaymentSetup.Tables["SMTPConfig"].Rows[0]["SFTPPASSWORD"].ToString().Trim();
            string targetName = dsPaymentSetup.Tables["SMTPConfig"].Rows[0]["TargetName"].ToString().Trim();


            MailMessage mailMessage = new MailMessage();
            mailMessage.From = new MailAddress(strFromAddress); //From Email Id
            mailMessage.Subject = strSubject; //Subject of Email
            mailMessage.Body = strBody; //body or message of Email
            mailMessage.IsBodyHtml = true;


            string[] ToMuliId = strToAddress.Split(',');
            foreach (string ToEMailId in ToMuliId)
            {
                if (ToEMailId != "")
                {
                    mailMessage.To.Add(new MailAddress(ToEMailId)); //adding multiple TO Email Id
                }
            }

            if (strCCAddress != "")
            {
                string[] CCId = strCCAddress.Split(',');

                foreach (string CCEmail in CCId)
                {
                    if (CCEmail != "")
                    {
                        mailMessage.CC.Add(new MailAddress(CCEmail)); //Adding Multiple CC email Id
                    }
                }
            }

            SmtpClient client = new SmtpClient(strHostAddress,iHostPort);

            if (targetName == "")
            {
                client.Credentials = new NetworkCredential(strSMTPUserID, strPassword);
            }
            else
            {
                {
                    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls
                                                      | SecurityProtocolType.Tls11
                                                      | SecurityProtocolType.Tls12;
                }
 
                client.UseDefaultCredentials = false;
                client.DeliveryMethod = SmtpDeliveryMethod.Network;
                client.Credentials = new NetworkCredential(strSMTPUserID, strPassword);
                client.TargetName = targetName;
                client.EnableSsl = true;
            }

            try
            {
                client.Send(mailMessage);
                return "Email Sent Successfully.";
            }
            catch (SmtpException ex)
            {
                EventLogger.WriteToEventLog((ex.Message.ToString()), EventLogEntryType.Warning);

                //return ex.ToString();
                errMessage = ex.ToString();
                return errMessage;
            }
            finally
            {

            }

        }

    }
}
