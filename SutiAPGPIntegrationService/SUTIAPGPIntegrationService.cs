using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
//using SUTIAPGPIntegrationService.Models;
using SUTIAPGPIntegrationService.Utilities;
using System.Xml;
//using Chilkat;
using System.IO;
using System.Reflection;
using System.Configuration;

using System.Data.SqlClient;
using SUTIAPGPIntegrationService.Processors;
using System.Net;
using System.Net.Mail;
using System.Text.RegularExpressions;

namespace SutiAPGPIntegrationService
{
    public partial class SUTIAPGPIntegrationService : ServiceBase
    {
        //private TimerConfigurationModel timerConfig;
        private Timer appTimer = new Timer();
        private bool isRunningTasks = false;
        private bool isMailing = false;
        private static int timerCount = 0;
        private string strConnString = "";
        private string errorMsg = "";
        private string emailErrMsg = "";
        private static int iFrequency = Convert.ToInt16(ConfigurationManager.AppSettings["TimerFrequency"]);

        public SUTIAPGPIntegrationService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            
            
            
            if (!GlobalVariables.AppDebug)
            {
                //TestConnection();
                System.Threading.Thread.Sleep(60000);



                   LoadValues();

                   appTimer.Interval = Convert.ToDouble(60000);

              
                EventLogger.WriteToEventLog(GlobalVariables.AppTitle + ":Starting SUTIAPGPIntegration Service ", EventLogEntryType.Information);

                appTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);


                appTimer.Enabled = true;

                isMailing = true;

               

                

                isMailing = false;

                isRunningTasks = true;

                //RunTasks();

                isRunningTasks = false;

            }

        }

        public void DebugStart()
        {
            LoadValues();

            appTimer.Interval = Convert.ToDouble(60000);
            



            EventLogger.WriteToEventLog("Starting SUTI AP GP Integration Service Debugging ", 0);
            appTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);

            appTimer.Enabled = true;


            //Commented for Debugging
            isMailing = true;
            //ProcessApprovalNotifications();
            //ProcessTransmitNotification();
            //ProcessVendorAuditNotifications();
            isMailing = false;

            //isRunningTasks = true;
            //RunTasks();
            //isRunningTasks = false;
        }

       private void LoadValues()
        {


            strConnString = Convert.ToString(ConfigurationManager.AppSettings["SUTIAPGPServiceCon"]);
            iFrequency = Convert.ToInt16(ConfigurationManager.AppSettings["TimerFrequency"]);
           
        }
        private void OnTimedEvent(object source, ElapsedEventArgs e)
        {

            
            timerCount = timerCount + 1;
            if ((!isRunningTasks) & (timerCount >= iFrequency))
            {
                isRunningTasks = true;
                //RunTasks();
                ProcessFiles();


                

                timerCount = 0;
                isRunningTasks = false;
                
            }
            if (!isMailing)
            {
                isMailing = true;
                //ProcessApprovalNotifications();
                //ProcessTransmitNotification();
                //ProcessVendorAuditNotifications();
                isMailing = false;
            }

         
           // EventLogger.WriteToEventLog("Running SUTI AP GP Integration Service: ", 0);
        }


        private void ProcessFiles()
        {
            string strSQL = SQLLibrary.SQLSUTIAPGPIntegrationSetup();
            if (strSQL != "")
            {
                DataSet dsIntegrationSetup = DBLibrary.GetDataSetFromSQLSCript(strSQL, "IntegrationSetup", strConnString);
                try
                {




                    if (dsIntegrationSetup.Tables["IntegrationSetup"].Rows.Count > 0)
                    {

                        foreach (DataRow drIntegrationSetup in dsIntegrationSetup.Tables["IntegrationSetup"].Rows)
                        {


                            if (ValidateSetupRecord(drIntegrationSetup))
                            {

                                if (ValidateSetupRecordForInvoice(drIntegrationSetup))
                                {
                                    ProcessInvoiceFiles(drIntegrationSetup);
                                }
                                else
                                {
                                    if (errorMsg != "")
                                    {
                                        EventLogger.WriteToEventLog((errorMsg), EventLogEntryType.Warning);
                                        if (Convert.ToBoolean(drIntegrationSetup["FAILUREINVOICEMAILENABLED"].ToString().Trim()) == true)
                                        {
                                            emailErrMsg = "";
                                            emailErrMsg = NotificationsMail.sendNotifications("INVOICE: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() , ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Invoice Integration ") + (" Error: " + errorMsg), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                            if (emailErrMsg != "")
                                            {
                                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                {
                                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                    FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                }
                                            }
                                        }
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(errorMsg.ToString(), filePath, EventLogEntryType.Warning);
                                        }
                                        errorMsg = "";
                                    }
                                }

                                if (ValidateSetupRecordForPayment(drIntegrationSetup))
                                {
                                    ProcessPaymentFiles(drIntegrationSetup);

                                }
                                else
                                {
                                    if (errorMsg != "")
                                    {
                                        EventLogger.WriteToEventLog((errorMsg), EventLogEntryType.Warning);

                                        if (Convert.ToBoolean(drIntegrationSetup["FAILUREPAYMENTMAILENABLED"].ToString().Trim()) == true)
                                        {
                                            emailErrMsg = "";
                                            emailErrMsg =  NotificationsMail.sendNotifications("PAYMENT: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() , ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Payment Integration ") + " Error: " + errorMsg.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                            if (emailErrMsg != "")
                                            {
                                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                {
                                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                    FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                }
                                            }
                                        }
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(errorMsg.ToString(), filePath, EventLogEntryType.Warning);
                                        }
                                        errorMsg = "";
                                    }
                                }

                                if (ValidateSetupRecordForPurchaseOrder(drIntegrationSetup))
                                {
                                    ProcessPOPFiles(drIntegrationSetup);

                                }
                                else
                                {
                                    if (errorMsg != "")
                                    {
                                        EventLogger.WriteToEventLog((errorMsg), EventLogEntryType.Warning);

                                        if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPMAILENABLED"].ToString().Trim()) == true)
                                        {
                                            emailErrMsg = "";
                                            emailErrMsg =  NotificationsMail.sendNotifications("Purchase Order: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim(), ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Purchase Order Integration ") + " Error: " + errorMsg.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                            if (emailErrMsg != "")
                                            {
                                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                {
                                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                    FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                }
                                            }
                                        }
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(errorMsg.ToString(), filePath, EventLogEntryType.Error);
                                        }
                                        errorMsg = "";
                                    }
                                }

                                if (ValidateSetupRecordForReceivingTransaction(drIntegrationSetup))
                                {
                                    ProcessRECVTransactionFiles(drIntegrationSetup);

                                }
                                else
                                {
                                    if (errorMsg != "")
                                    {
                                        EventLogger.WriteToEventLog((errorMsg), EventLogEntryType.Warning);

                                        if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPRECVMAILENABLED"].ToString().Trim()) == true)
                                        {
                                            emailErrMsg = "";
                                            emailErrMsg =  NotificationsMail.sendNotifications("Receiving Transaction: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim(), ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Receiving Transaction Integration ") + " Error: " + errorMsg.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                            if (emailErrMsg != "")
                                            {
                                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                {
                                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                    FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                }
                                            }
                                        }
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(errorMsg.ToString(), filePath, EventLogEntryType.Warning);
                                        }
                                        errorMsg = "";
                                    }
                                }

                                if (ValidateSetupRecordForEnterMatchInvoice(drIntegrationSetup))
                                {
                                    ProcessEnterMatchFiles(drIntegrationSetup);

                                }
                                else
                                {
                                    if (errorMsg != "")
                                    {
                                        EventLogger.WriteToEventLog((errorMsg), EventLogEntryType.Warning);

                                        if (Convert.ToBoolean(drIntegrationSetup["FAILUREENTMATCHINVMAILENABLED"].ToString().Trim()) == true)
                                        {
                                            emailErrMsg = "";
                                            emailErrMsg = NotificationsMail.sendNotifications("Enter Match Invoice: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim(), ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Enter Match Integration ") + " Error: " + errorMsg.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                            if (emailErrMsg != "")
                                            {
                                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                {
                                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                    FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                }
                                            }
                                        }
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(errorMsg.ToString(), filePath, EventLogEntryType.Warning);
                                        }
                                        errorMsg = "";
                                    }
                                }
                                if (ValidateSetupRecordForPOExport(drIntegrationSetup))
                                {
                                    ProcessPurchaseOrderExport(drIntegrationSetup);
                                }
                                else
                                {
                                    if (errorMsg != "")
                                    {
                                        EventLogger.WriteToEventLog((errorMsg), EventLogEntryType.Warning);

                                        if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOEXPORTMAILENABLED"].ToString().Trim()) == true)
                                        {
                                            emailErrMsg = "";
                                            emailErrMsg = NotificationsMail.sendNotifications("Purchase Order Export: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim(), ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Purchase Order Export ") + " Error: " + errorMsg.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                            if (emailErrMsg != "")
                                            {
                                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                {
                                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                    FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                }
                                            }
                                        }
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(errorMsg.ToString(), filePath, EventLogEntryType.Warning);
                                        }
                                        errorMsg = "";
                                    }
                                }
                                if (ValidateSetupRecordForShipmentExport(drIntegrationSetup))
                                {
                                    ProcessShipmentExport(drIntegrationSetup);
                                }
                                else
                                {
                                    if (errorMsg != "")
                                    {
                                        EventLogger.WriteToEventLog((errorMsg), EventLogEntryType.Warning);

                                        if (Convert.ToBoolean(drIntegrationSetup["FAILURESHIPEXPORTMAILENABLED"].ToString().Trim()) == true)
                                        {
                                            emailErrMsg = "";
                                            emailErrMsg = NotificationsMail.sendNotifications("Shipment Data Export: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim(), ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Shipment Data Export ") + " Error: " + errorMsg.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                            if (emailErrMsg != "")
                                            {
                                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                {
                                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                    FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                }
                                            }
                                        }
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(errorMsg.ToString(), filePath, EventLogEntryType.Warning);
                                        }
                                        errorMsg = "";
                                    }
                                }

                            }
                            else
                            {
                                if (errorMsg != "")
                                {
                                    EventLogger.WriteToEventLog((errorMsg), EventLogEntryType.Warning);

                                    emailErrMsg = "";
                                    emailErrMsg =  NotificationsMail.sendNotifications("Setup: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim(), ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", " Integration ") + " Error: " + errorMsg.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                    if (emailErrMsg != "")
                                    {
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                        }
                                    }
                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                    {
                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                        FileLogger.WriteToFileLog(errorMsg.ToString(), filePath, EventLogEntryType.Warning);
                                    }
                                    errorMsg = "";
                                }
                            }

                        }






                    }
                    else
                    {
                        EventLogger.WriteToEventLog(("Warning: Integration Setup is missing. Please check setup table"), EventLogEntryType.Warning);

                    }




                }
                catch (Exception ex)
                {
                    EventLogger.WriteToEventLog(("!>>> Exception: " + ex.Message), EventLogEntryType.Error);

                }

                finally
                {
                    dsIntegrationSetup.Dispose();
                }
            }
        }


        private Boolean ValidateSetupRecord(DataRow drIntegrationSetup)
        {
            Boolean bValidate = true;
            if (!Directory.Exists(drIntegrationSetup["INBOUNDPATH"].ToString()))
            {

                if (errorMsg == string.Empty)
                {
                    errorMsg = "Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " is not valid for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " is not valid for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                bValidate = false;
            }
            if (Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString()).Count() == 0)
            {
                //if (errorMsg == string.Empty)
                //{
                //    errorMsg = "File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                //}
                //else
                //{
                //    errorMsg = errorMsg + "\n File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                //}
                string a  = drIntegrationSetup["EXPORTSHIPMENT"].ToString().Trim();

                if (drIntegrationSetup["EXPORTSHIPMENT"].ToString().Trim() == "True" || drIntegrationSetup["EXPORTPURCHORDER"].ToString().Trim() == "True")
                {
                    bValidate = true;
                }
                else
                {
                    bValidate = false;
                }
                    
            }
            if (drIntegrationSetup["INBOUNDPATH"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "INBOUNDPATH is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n INBOUNDPATH is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                bValidate = false;
            }
            if (drIntegrationSetup["GPSERVER"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "GPSERVER is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n GPSERVER is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                bValidate = false;
            }
            if (drIntegrationSetup["GPINTERID"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "GPINTERID is missing in Setup Table";
                }
                else
                {
                    errorMsg = errorMsg + "\n GPINTERID is missing in Setup Table";
                }
                bValidate = false;
            }

            return bValidate;
        }

        private Boolean ValidateSetupRecordForInvoice(DataRow drIntegrationSetup)
        {

            Boolean bValidate = true;


           
            if (drIntegrationSetup["INVOICEFILEPREFIX"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "INVOICEFILEPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n INVOICEFILEPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }

                bValidate = false;
            }
            if (drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "INVOICEFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n INVOICEFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                bValidate = false;
            }

            if (bValidate)
            {
                if (Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString(), drIntegrationSetup["INVOICEFILEPREFIX"].ToString().Trim() + "*." + drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim()).Count() == 0)
                {
                    //if (errorMsg == string.Empty)
                    //{
                    //    errorMsg = drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim()  + " File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    //}
                    //else
                    //{
                    //    errorMsg = errorMsg + "\n " + drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim() + " File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    //}
                    bValidate = false;
                }

            }

            return bValidate;
        }
               
        private Boolean ValidateSetupRecordForPayment(DataRow drIntegrationSetup)
        {
            Boolean bValidate = true;

            if (drIntegrationSetup["PAYMENTFILEPREFIX"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "PAYMENTFILEPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n PAYMENTFILEPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                bValidate = false;
            }
            if (drIntegrationSetup["PAYMENTFILETYPE"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "PAYMENTFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n PAYMENTFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                bValidate = false;
            }


            if (bValidate)
            {
                if (Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString(), drIntegrationSetup["PAYMENTFILEPREFIX"].ToString().Trim() + "*." + drIntegrationSetup["PAYMENTFILETYPE"].ToString().Trim()).Count() == 0)
                {
                    //if (errorMsg == string.Empty)
                    //{
                    //    errorMsg = drIntegrationSetup["PAYMENTFILETYPE"].ToString().Trim() + " File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    //}
                    //else
                    //{
                    //    errorMsg = errorMsg + "\n " + drIntegrationSetup["PAYMENTFILETYPE"].ToString().Trim()  + " File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    //}
                    bValidate = false;
                }

            }

            return bValidate;
        }

        private Boolean ValidateSetupRecordForPurchaseOrder(DataRow drIntegrationSetup)
        {

            Boolean bValidate = true;



            if (drIntegrationSetup["POPFILEPREFIX"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "POPFILEPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n POPFILEPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }

                bValidate = false;
            }
            if (drIntegrationSetup["POPFILETYPE"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "POPFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n POPFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                bValidate = false;
            }

            if (bValidate)
            {
                if (Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString(), drIntegrationSetup["POPFILEPREFIX"].ToString().Trim() + "*." + drIntegrationSetup["POPFILETYPE"].ToString().Trim()).Count() == 0)
                {
                    //if (errorMsg == string.Empty)
                    //{
                    //    errorMsg = drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim()  + " File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    //}
                    //else
                    //{
                    //    errorMsg = errorMsg + "\n " + drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim() + " File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    //}
                    bValidate = false;
                }

            }

            return bValidate;
        }

        private Boolean ValidateSetupRecordForReceivingTransaction(DataRow drIntegrationSetup)
        {

            Boolean bValidate = true;



            if (drIntegrationSetup["RCVINVOICEFILEPREFIX"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "RCVINVOICEFILEPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n RCVINVOICEFILEPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }

                bValidate = false;
            }
            if (drIntegrationSetup["RCVINVOICEFILETYPE"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "RCVINVOICEFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n RCVINVOICEFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                bValidate = false;
            }

            if (bValidate)
            {
                if (Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString(), drIntegrationSetup["RCVINVOICEFILEPREFIX"].ToString().Trim() + "*." + drIntegrationSetup["RCVINVOICEFILETYPE"].ToString().Trim()).Count() == 0)
                {
                    //if (errorMsg == string.Empty)
                    //{
                    //    errorMsg = drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim()  + " File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    //}
                    //else
                    //{
                    //    errorMsg = errorMsg + "\n " + drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim() + " File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    //}
                    bValidate = false;
                }

            }

            return bValidate;
        }

        private Boolean ValidateSetupRecordForEnterMatchInvoice(DataRow drIntegrationSetup)
        {

            Boolean bValidate = true;



            if (drIntegrationSetup["ENTMATCHINVFILEPREFIX"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "ENTMATCHINVFILEPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n ENTMATCHINVFILEPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }

                bValidate = false;
            }
            if (drIntegrationSetup["ENTMATCHINVFILETYPE"].ToString().Trim() == "")
            {
                if (errorMsg == string.Empty)
                {
                    errorMsg = "ENTMATCHINVFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                else
                {
                    errorMsg = errorMsg + "\n ENTMATCHINVFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                }
                bValidate = false;
            }

            if (bValidate)
            {
                if (Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString(), drIntegrationSetup["ENTMATCHINVFILEPREFIX"].ToString().Trim() + "*." + drIntegrationSetup["ENTMATCHINVFILETYPE"].ToString().Trim()).Count() == 0)
                {
                    //if (errorMsg == string.Empty)
                    //{
                    //    errorMsg = drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim()  + " File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    //}
                    //else
                    //{
                    //    errorMsg = errorMsg + "\n " + drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim() + " File is not existing in Directory " + drIntegrationSetup["INBOUNDPATH"].ToString() + " for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    //}
                    bValidate = false;
                }

            }

            return bValidate;
        }

        private Boolean ValidateSetupRecordForPOExport(DataRow drIntegrationSetup)
        {

            Boolean bValidate = true;

            if (drIntegrationSetup["EXPORTPURCHORDER"].ToString().Trim() != "" || drIntegrationSetup["EXPORTPURCHORDER"].ToString().Trim() != "0")
            {
                if (drIntegrationSetup["EXPORTPOPREFIX"].ToString().Trim() == "")
                {
                    if (errorMsg == string.Empty)
                    {
                        errorMsg = "EXPORTPOPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }
                    else
                    {
                        errorMsg = errorMsg + "\n EXPORTPOPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }

                    bValidate = false;
                }
                if (drIntegrationSetup["EXPORTPOFILETYPE"].ToString().Trim() == "")
                {
                    if (errorMsg == string.Empty)
                    {
                        errorMsg = "EXPORTPOFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }
                    else
                    {
                        errorMsg = errorMsg + "\n EXPORTPOFILETYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }
                    bValidate = false;
                }
                if (!Directory.Exists(drIntegrationSetup["EXPORTPOPATH"].ToString()))
                {
                    if (errorMsg == string.Empty)
                    {
                        errorMsg = "EXPORTPOPATH is not found for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }
                    else
                    {
                        errorMsg = errorMsg + "\n EXPORTPOPATH is not found for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }
                    bValidate = false;

                }
            }
            else
            {
                
                bValidate = false;
            }
            
            return bValidate;
        }

        private Boolean ValidateSetupRecordForShipmentExport(DataRow drIntegrationSetup)
        {

            Boolean bValidate = true;

            if (drIntegrationSetup["EXPORTSHIPMENT"].ToString().Trim() != "" || drIntegrationSetup["EXPORTSHIPMENT"].ToString().Trim() != "0")
            {
                if (drIntegrationSetup["EXPORTSHIPMENTPREFIX"].ToString().Trim() == "")
                {
                    if (errorMsg == string.Empty)
                    {
                        errorMsg = "EXPORTSHIPMENTPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }
                    else
                    {
                        errorMsg = errorMsg + "\n EXPORTSHIPMENTPREFIX is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }

                    bValidate = false;
                }
                if (drIntegrationSetup["EXPORTSHIPMENTTYPE"].ToString().Trim() == "")
                {
                    if (errorMsg == string.Empty)
                    {
                        errorMsg = "EXPORTSHIPMENTTYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }
                    else
                    {
                        errorMsg = errorMsg + "\n EXPORTSHIPMENTTYPE is missing in Setup Table for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }
                    bValidate = false;
                }
                if (!Directory.Exists(drIntegrationSetup["EXPORTSHIPMENTPATH"].ToString()))
                {
                    if (errorMsg == string.Empty)
                    {
                        errorMsg = "EXPORTSHIPMENTPATH is not found for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }
                    else
                    {
                        errorMsg = errorMsg + "\n EXPORTSHIPMENTPATH is not found for Company " + drIntegrationSetup["GPINTERID"].ToString();
                    }
                    bValidate = false;

                }
            }
            else
            {

                bValidate = false;
            }

            return bValidate;
        }

        private void ProcessInvoiceFiles(DataRow drIntegrationSetup)
        {
            try
            {

                //string[] filePaths = Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString(), "*." + drIntegrationSetup["INVOICEFILETYPE"].ToString());
                switch (drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim())
                {

                    case "CSV":
                        foreach (string fileName in Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString().Trim(), drIntegrationSetup["INVOICEFILEPREFIX"].ToString().Trim() + "*." + drIntegrationSetup["INVOICEFILETYPE"].ToString().Trim()))
                        {

                            DataTable dtFileData = new DataTable();

                            EventLogger.WriteToEventLog("File Processing started :" + fileName , EventLogEntryType.Warning);
                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                            {
                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                FileLogger.WriteToFileLog("File Processing Started", filePath, EventLogEntryType.Warning);
                            }
                            try
                            {
                                dtFileData = ConvertCSVtoDataTable(fileName);


                                if (dtFileData.Rows.Count > 0)
                                {



                                    string strSQL = SQLLibrary.SQLInvoicPaymenteHeaderMapping(drIntegrationSetup["INVOICEMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLDist = SQLLibrary.SQLInvoicPaymenteDistributionMapping(drIntegrationSetup["INVOICEMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLTax = SQLLibrary.SQLInvoicPaymenteTaxMapping(drIntegrationSetup["INVOICEMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLMappingColumnNames = SQLLibrary.SQLInvoicPaymenteHeaderMappingColumns(drIntegrationSetup["INVOICEMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    if (strSQL != "")
                                    {
                                        DataSet dsIntegrationSetup = DBLibrary.GetDataSetFromSQLSCript(strSQL, "InvoiceHeader", strConnString);
                                        DataSet dsIntegrationDistSetup = DBLibrary.GetDataSetFromSQLSCript(strSQLDist, "InvoiceDistribution", strConnString);
                                        DataSet dsIntegrationTaxSetup = DBLibrary.GetDataSetFromSQLSCript(strSQLTax, "InvoiceTax", strConnString);
                                        DataTable dtMappingColumnName = DBLibrary.GetDataTableFromSQLSCript(strSQLMappingColumnNames, strConnString);
                                       
                                        string[] SourceColumns = new string[dtMappingColumnName.Rows.Count];
                                        SourceColumns = dtMappingColumnName.Rows.OfType<DataRow>().Select(k => k[0].ToString()).ToArray();
                                        string gpCmpDBConnStr = "data source=" + drIntegrationSetup["GPSERVER"].ToString().Trim() + ";initial catalog=" + drIntegrationSetup["GPINTERID"].ToString().Trim() + ";integrated security=SSPI;persist security info=False;packet size=4096";

                                        try
                                        {

                                            EventLogger.WriteToEventLog("File Processing started1 :" + fileName, EventLogEntryType.Warning);
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog("File Processing Started 1", filePath, EventLogEntryType.Error);
                                            }
                                            if (InvoiceFileDataPush.InvoiceFileDataProcessing(dtFileData, dsIntegrationSetup, dsIntegrationDistSetup, dsIntegrationTaxSetup, drIntegrationSetup, SourceColumns, gpCmpDBConnStr, drIntegrationSetup["GPINTERID"].ToString().Trim(), drIntegrationSetup["INVOICEMAPID"].ToString().Trim(), Convert.ToBoolean(drIntegrationSetup["INVFORMATACCOUNTSTRING"])))
                                            {
                                                File.Move(fileName, drIntegrationSetup["PROCESSEDPATH"].ToString().Trim() + @"\" + Path.GetFileNameWithoutExtension(fileName) + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + Path.GetExtension(fileName));

                                                if (Convert.ToBoolean(drIntegrationSetup["SUCCESSINVOICEMAILENABLED"].ToString().Trim()) == true)
                                                {
                                                    emailErrMsg = "";
                                                    emailErrMsg = NotificationsMail.sendNotifications("INVOICE: " + drIntegrationSetup["SUCCESSEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["SuccessNotification"].ToString().Replace("@IntegrationObject", "Invoice Integration ") + " Error : " + InvoiceFileDataPush.strExceptionMessage, drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSTOEMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSCCEMAILID"].ToString().Trim(), strConnString);
                                                    if (emailErrMsg != "")
                                                    {
                                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                        {
                                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                        }
                                                    }
                                                }
                                                

                                            }
                                            else
                                            {

                                                File.Move(fileName, drIntegrationSetup["FAILEDPATH"].ToString().Trim() + @"\" + Path.GetFileNameWithoutExtension(fileName) + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + Path.GetExtension(fileName));

                                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREINVOICEMAILENABLED"].ToString().Trim()) == true)
                                                {
                                                    emailErrMsg = "";
                                                    emailErrMsg =  NotificationsMail.sendNotifications("INVOICE: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Invoice Integration ") + " Error : " + InvoiceFileDataPush.strExceptionMessage, drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                                    if (emailErrMsg != "")
                                                    {
                                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                        {
                                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                        }
                                                    }
                                                }
                                                
                                                
                                            }

                                        }
                                        catch (Exception ex)
                                        {
                                            EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);

                                            if (Convert.ToBoolean(drIntegrationSetup["FAILUREINVOICEMAILENABLED"].ToString().Trim()) == true)
                                            {
                                                emailErrMsg = "";
                                                emailErrMsg =  NotificationsMail.sendNotifications("INVOICE: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Invoice Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                                if (emailErrMsg != "")
                                                {
                                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                    {
                                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                        FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                    }
                                                }
                                            }
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                                            }
                                        }
                                        finally
                                        {
                                            dsIntegrationSetup.Dispose();
                                            dsIntegrationDistSetup.Dispose();
                                            dsIntegrationTaxSetup.Dispose();
                                            dtMappingColumnName.Dispose();
                                        }


                                    }
                                }
                                else
                                {
                                    EventLogger.WriteToEventLog(("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString()+@"\"+fileName), EventLogEntryType.Warning);

                                    if (Convert.ToBoolean(drIntegrationSetup["FAILUREINVOICEMAILENABLED"].ToString().Trim()) == true)
                                    {
                                        emailErrMsg = "";
                                        emailErrMsg =  NotificationsMail.sendNotifications("INVOICE: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Invoice Integration ") + ("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                        if (emailErrMsg != "")
                                        {
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                            }
                                        }
                                    }
                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                    {
                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                        FileLogger.WriteToFileLog(("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), filePath, EventLogEntryType.Error);
                                    }
                                }



                            }
                            catch (Exception ex)
                            {
                                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREINVOICEMAILENABLED"].ToString().Trim()) == true)
                                {
                                    emailErrMsg = "";
                                    emailErrMsg =  NotificationsMail.sendNotifications("INVOICE: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Invoice Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                    if (emailErrMsg != "")
                                    {
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                        }
                                    }

                                }
                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                {
                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                                }
                            }
                            finally
                            {
                                dtFileData.Dispose();
                            }



                        }
                        break;

                }

            }
            catch (Exception ex)
            {
                EventLogger.WriteToEventLog(ex.Message.ToString(),EventLogEntryType.Error);
                if (Convert.ToBoolean(drIntegrationSetup["FAILUREINVOICEMAILENABLED"].ToString().Trim()) == true)
                {
                    emailErrMsg = "";
                    emailErrMsg =  NotificationsMail.sendNotifications("INVOICE: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() , ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Invoice Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                    if (emailErrMsg != "")
                    {
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                        }
                    }
                }
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                }
            }
           

        }

        private void ProcessPaymentFiles(DataRow drIntegrationSetup)
        {
            try
            {
                switch (drIntegrationSetup["PAYMENTFILETYPE"].ToString().Trim())
                {

                    case "CSV":


                        foreach (string fileName in Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString().Trim(), drIntegrationSetup["PAYMENTFILEPREFIX"].ToString().Trim() + "*." + drIntegrationSetup["PAYMENTFILETYPE"].ToString().Trim()))
                        {
                            DataTable dtFileData = new DataTable();
                            

                            try
                            {
                                dtFileData = ConvertCSVtoDataTable(fileName);
                                if (dtFileData.Rows.Count > 0)
                                {
                                    string strSQL = SQLLibrary.SQLInvoicPaymenteHeaderMapping(drIntegrationSetup["PAYMENTMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLDist = SQLLibrary.SQLInvoicPaymenteDistributionMapping(drIntegrationSetup["PAYMENTMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLMappingColumnNames = SQLLibrary.SQLInvoicPaymenteHeaderMappingColumns(drIntegrationSetup["PAYMENTMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());

                                   
                                    
                                    if (strSQL != "")
                                    {
                                        DataSet dsPaymentSetup = DBLibrary.GetDataSetFromSQLSCript(strSQL, "PaymentHeader", strConnString);
                                        DataSet dsPaymentDistSetup = DBLibrary.GetDataSetFromSQLSCript(strSQLDist, "PaymentDistribution", strConnString);
                                        DataTable dtPaymentMappingColumnName = DBLibrary.GetDataTableFromSQLSCript(strSQLMappingColumnNames, strConnString);

                                        string[] SourceColumns = new string[dtPaymentMappingColumnName.Rows.Count];
                                        SourceColumns = dtPaymentMappingColumnName.Rows.OfType<DataRow>().Select(k => k[0].ToString()).ToArray();
                                        string gpCmpDBConnStr = "data source=" + drIntegrationSetup["GPSERVER"].ToString().Trim() + ";initial catalog=" + drIntegrationSetup["GPINTERID"].ToString().Trim() + ";integrated security=SSPI;persist security info=False;packet size=4096";




                                        try
                                        {

                                            if (PaymentFileDataPush.PaymentFileDataProcessing(dtFileData, dsPaymentSetup, dsPaymentDistSetup, drIntegrationSetup, SourceColumns, gpCmpDBConnStr, drIntegrationSetup["GPINTERID"].ToString().Trim(), drIntegrationSetup["PAYMENTMAPID"].ToString().Trim(),Convert.ToBoolean(drIntegrationSetup["PYMFORMATACCOUNTSTRING"])))
                                            {
                                                File.Move(fileName, drIntegrationSetup["PROCESSEDPATH"].ToString().Trim() + @"\" + Path.GetFileNameWithoutExtension(fileName) + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + Path.GetExtension(fileName));

                                                if (Convert.ToBoolean(drIntegrationSetup["SUCCESSPAYMENTMAILENABLED"].ToString().Trim()) == true)
                                                {
                                                    emailErrMsg = "";
                                                    emailErrMsg = NotificationsMail.sendNotifications("PAYMENT: " + drIntegrationSetup["SUCCESSEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["SuccessNotification"].ToString().Replace("@IntegrationObject", "Payment Integration "), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSTOEMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSCCEMAILID"].ToString().Trim(), strConnString);
                                                    if (emailErrMsg != "")
                                                    {
                                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                        {
                                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                        }
                                                    }
                                                }
                                            
                                            }
                                            else
                                            {
                                                File.Move(fileName, drIntegrationSetup["FAILEDPATH"].ToString().Trim() + @"\" + Path.GetFileNameWithoutExtension(fileName) + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + Path.GetExtension(fileName));

                                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPAYMENTMAILENABLED"].ToString().Trim()) == true)
                                                {
                                                    emailErrMsg = "";
                                                    emailErrMsg =  NotificationsMail.sendNotifications("PAYMENT: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Payment Integration ") + "Error: " + PaymentFileDataPush.strExceptionMessage, drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                                    if (emailErrMsg != "")
                                                    {
                                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                        {
                                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                        }
                                                    }
                                                }
                                            
                                            
                                            }

                                        }
                                        catch (Exception ex)
                                        {
                                            EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                                            if (Convert.ToBoolean(drIntegrationSetup["FAILUREPAYMENTMAILENABLED"].ToString().Trim()) == true)
                                            {
                                                emailErrMsg = "";
                                                emailErrMsg =  NotificationsMail.sendNotifications("PAYMENT: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Payment Integration ") + "Error: " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                                if (emailErrMsg != "")
                                                {
                                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                    {
                                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                        FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                    }
                                                }
                                            }
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                                            }
                                        }
                                        finally
                                        {
                                            dsPaymentSetup.Dispose();
                                            dsPaymentDistSetup.Dispose();
                                            dtPaymentMappingColumnName.Dispose();
                                        }



                                    }
                                }
                                else
                                {
                                    EventLogger.WriteToEventLog(("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), EventLogEntryType.Warning);

                                    if (Convert.ToBoolean(drIntegrationSetup["FAILUREPAYMENTMAILENABLED"].ToString().Trim()) == true)
                                    {
                                        emailErrMsg = "";
                                        emailErrMsg =  NotificationsMail.sendNotifications("PAYMENT: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Payment Integration ") + (" Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                        if (emailErrMsg != "")
                                        {
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                            }
                                        }
                                    }
                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                    {
                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                        FileLogger.WriteToFileLog("Warning: No Data found in the file. ", filePath, EventLogEntryType.Error);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPAYMENTMAILENABLED"].ToString().Trim()) == true)
                                {
                                    emailErrMsg = "";
                                    emailErrMsg =  NotificationsMail.sendNotifications("PAYMENT: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Payment Integration ") + " Error: " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                    if (emailErrMsg != "")
                                    {
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                        }
                                    }
                                }
                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                {
                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                                }
                            }
                            finally
                            {
                                dtFileData.Dispose();
                            }


                        }
                        break;
                }
            }
            catch (Exception ex)
            {
                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);

                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPAYMENTMAILENABLED"].ToString().Trim()) == true)
                {
                    emailErrMsg = "";
                    emailErrMsg =  NotificationsMail.sendNotifications("PAYMENT: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() , ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Payment Integration ") + " Error: " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                    if (emailErrMsg != "")
                    {
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                        }
                    }
                }
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                }
            }

        }

        private void ProcessPOPFiles(DataRow drIntegrationSetup)
        {
            try
            {

                //string[] filePaths = Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString(), "*." + drIntegrationSetup["INVOICEFILETYPE"].ToString());
                switch (drIntegrationSetup["POPFILETYPE"].ToString().Trim())
                {

                    case "CSV":
                        foreach (string fileName in Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString().Trim(), drIntegrationSetup["POPFILEPREFIX"].ToString().Trim() + "*." + drIntegrationSetup["POPFILETYPE"].ToString().Trim()))
                        {

                            DataTable dtFileData = new DataTable();

                            EventLogger.WriteToEventLog("POP File Processing started :" + fileName, EventLogEntryType.Warning);
                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                            {
                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                FileLogger.WriteToFileLog("POP File Processing started :" + fileName, filePath, EventLogEntryType.Warning);
                            }
                            try
                            {
                                dtFileData = ConvertCSVtoDataTable(fileName);


                                if (dtFileData.Rows.Count > 0)
                                {



                                    string strSQL = SQLLibrary.SQLPOPHeaderMapping(drIntegrationSetup["POPMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLDist = SQLLibrary.SQLPOPLineMapping(drIntegrationSetup["POPMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLTax = SQLLibrary.SQLPOPTaxMapping(drIntegrationSetup["POPMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLMappingColumnNames = SQLLibrary.SQLPOPHeaderMappingColumns(drIntegrationSetup["POPMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    if (strSQL != "")
                                    {
                                        DataSet dsIntegrationPOPHdrSetup = DBLibrary.GetDataSetFromSQLSCript(strSQL, "POPHeader", strConnString);
                                        DataSet dsIntegrationPOPLineSetup = DBLibrary.GetDataSetFromSQLSCript(strSQLDist, "POPLine", strConnString);
                                        DataSet dsIntegrationPOPTaxSetup = DBLibrary.GetDataSetFromSQLSCript(strSQLTax, "POPLineTax", strConnString);
                                        DataTable dtMappingColumnName = DBLibrary.GetDataTableFromSQLSCript(strSQLMappingColumnNames, strConnString);

                                        string[] SourceColumns = new string[dtMappingColumnName.Rows.Count];
                                        SourceColumns = dtMappingColumnName.Rows.OfType<DataRow>().Select(k => k[0].ToString()).ToArray();
                                        string gpCmpDBConnStr = "data source=" + drIntegrationSetup["GPSERVER"].ToString().Trim() + ";initial catalog=" + drIntegrationSetup["GPINTERID"].ToString().Trim() + ";integrated security=SSPI;persist security info=False;packet size=4096";

                                        try
                                        {

                                            EventLogger.WriteToEventLog("POP Integration Processing started :" + fileName, EventLogEntryType.Warning);
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog("POP Integration Processing started :" + fileName, filePath, EventLogEntryType.Warning);
                                            }
                                            if (PurchaseOrderFileDataPush.PurchaseOrderFileProcessing(dtFileData, dsIntegrationPOPHdrSetup, dsIntegrationPOPLineSetup, dsIntegrationPOPTaxSetup, drIntegrationSetup, SourceColumns, gpCmpDBConnStr, drIntegrationSetup["GPINTERID"].ToString().Trim(), drIntegrationSetup["POPMAPID"].ToString().Trim(), Convert.ToBoolean(drIntegrationSetup["POPRECVFORMATACCOUNTSTRING"])))
                                            {
                                                File.Move(fileName, drIntegrationSetup["PROCESSEDPATH"].ToString().Trim() + @"\" + Path.GetFileNameWithoutExtension(fileName) + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + Path.GetExtension(fileName));

                                                if (Convert.ToBoolean(drIntegrationSetup["SUCCESSPOPMAILENABLED"].ToString().Trim()) == true)
                                                {
                                                    emailErrMsg = "";
                                                    emailErrMsg = NotificationsMail.sendNotifications("Purchase Order: " + drIntegrationSetup["SUCCESSEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["SuccessNotification"].ToString().Replace("@IntegrationObject", "Purchase Order Integration ")  , drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSTOEMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSCCEMAILID"].ToString().Trim(), strConnString);
                                                    if (emailErrMsg != "")
                                                    {
                                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                        {
                                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                        }
                                                    }
                                                }


                                            }
                                            else
                                            {

                                                File.Move(fileName, drIntegrationSetup["FAILEDPATH"].ToString().Trim() + @"\" + Path.GetFileNameWithoutExtension(fileName) + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + Path.GetExtension(fileName));

                                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPMAILENABLED"].ToString().Trim()) == true)
                                                {
                                                    emailErrMsg = "";
                                                    emailErrMsg = NotificationsMail.sendNotifications("Purchase Order: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Purchase Order Integration ") + " Error : " + PurchaseOrderFileDataPush.strExceptionMessage, drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                                    if (emailErrMsg != "")
                                                    {
                                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                        {
                                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                        }
                                                    }
                                                }


                                            }

                                        }
                                        catch (Exception ex)
                                        {
                                            EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);

                                            if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPMAILENABLED"].ToString().Trim()) == true)
                                            {
                                                emailErrMsg = "";
                                                emailErrMsg =  NotificationsMail.sendNotifications("Purchase Order: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Purchase Order Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                                if (emailErrMsg != "")
                                                {
                                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                    {
                                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                        FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                    }
                                                }
                                            }
                                            
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog(ex.Message.ToString(),  filePath, EventLogEntryType.Error);
                                            }

                                            

                                        }
                                        finally
                                        {
                                            dsIntegrationPOPHdrSetup.Dispose();
                                            dsIntegrationPOPLineSetup.Dispose();
                                            dsIntegrationPOPTaxSetup.Dispose();
                                            dtMappingColumnName.Dispose();
                                        }


                                    }
                                }
                                else
                                {
                                    EventLogger.WriteToEventLog(("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), EventLogEntryType.Warning);

                                    if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPMAILENABLED"].ToString().Trim()) == true)
                                    {
                                        emailErrMsg = "";
                                        emailErrMsg =  NotificationsMail.sendNotifications("PurchaseOrder: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Invoice Integration ") + ("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                        if (emailErrMsg != "")
                                        {
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                            }
                                        }
                                    }
                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                    {
                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                        FileLogger.WriteToFileLog(("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), filePath, EventLogEntryType.Error);
                                    }

                                }



                            }
                            catch (Exception ex)
                            {
                                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPMAILENABLED"].ToString().Trim()) == true)
                                {
                                    emailErrMsg = "";
                                    emailErrMsg =  NotificationsMail.sendNotifications("PurchaseOrder: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Invoice Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                    if (emailErrMsg != "")
                                    {
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                        }
                                    }

                                }
                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                {
                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                                }
                            }
                            finally
                            {
                                dtFileData.Dispose();
                            }



                        }
                        break;

                }

            }
            catch (Exception ex)
            {
                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPMAILENABLED"].ToString().Trim()) == true)
                {
                    emailErrMsg = "";
                    emailErrMsg = NotificationsMail.sendNotifications("PurchaseOrder: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim(), ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Invoice Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                    if (emailErrMsg != "")
                    {
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                        }
                    }
                }
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                }
            }


        }

        private void ProcessRECVTransactionFiles(DataRow drIntegrationSetup)
        {
            try
            {

                //string[] filePaths = Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString(), "*." + drIntegrationSetup["INVOICEFILETYPE"].ToString());
                switch (drIntegrationSetup["RCVINVOICEFILETYPE"].ToString().Trim())
                {

                    case "CSV":
                        foreach (string fileName in Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString().Trim(), drIntegrationSetup["RCVINVOICEFILEPREFIX"].ToString().Trim() + "*." + drIntegrationSetup["RCVINVOICEFILETYPE"].ToString().Trim()))
                        {

                            DataTable dtFileData = new DataTable();

                            EventLogger.WriteToEventLog("Receving Transaction File Processing started :" + fileName, EventLogEntryType.Warning);
                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                            {
                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                FileLogger.WriteToFileLog("Receving Transaction File Processing started :" + fileName, filePath, EventLogEntryType.Warning);
                            }
                            try
                            {
                                dtFileData = ConvertCSVtoDataTable(fileName);


                                if (dtFileData.Rows.Count > 0)
                                {



                                    string strSQL = SQLLibrary.SQLRECVHeaderMapping(drIntegrationSetup["RCVINVMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLLine = SQLLibrary.SQLRECVLineMapping(drIntegrationSetup["RCVINVMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLTax = SQLLibrary.SQLRECVTaxMapping(drIntegrationSetup["RCVINVMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLDistr = SQLLibrary.SQLRECVDistMapping(drIntegrationSetup["RCVINVMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLMappingColumnNames = SQLLibrary.SQLRECVHeaderMappingColumns(drIntegrationSetup["RCVINVMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    if (strSQL != "")
                                    {
                                        DataSet dsIntegrationRECVHdrSetup = DBLibrary.GetDataSetFromSQLSCript(strSQL, "RecvTrxHeader", strConnString);
                                        DataSet dsIntegrationRECVLineSetup = DBLibrary.GetDataSetFromSQLSCript(strSQLLine, "RecvTrxLine", strConnString);
                                        DataSet dsIntegrationRECVTaxSetup = DBLibrary.GetDataSetFromSQLSCript(strSQLTax, "RecvTrxLineTax", strConnString);
                                        DataSet dsIntegrationRECVDistSetup = DBLibrary.GetDataSetFromSQLSCript(strSQLDistr, "RecvTrxDist", strConnString);
                                        DataTable dtMappingColumnName = DBLibrary.GetDataTableFromSQLSCript(strSQLMappingColumnNames, strConnString);

                                        string[] SourceColumns = new string[dtMappingColumnName.Rows.Count];
                                        SourceColumns = dtMappingColumnName.Rows.OfType<DataRow>().Select(k => k[0].ToString()).ToArray();
                                        string gpCmpDBConnStr = "data source=" + drIntegrationSetup["GPSERVER"].ToString().Trim() + ";initial catalog=" + drIntegrationSetup["GPINTERID"].ToString().Trim() + ";integrated security=SSPI;persist security info=False;packet size=4096";

                                        try
                                        {

                                            EventLogger.WriteToEventLog("Receving Integration Processing started :" + fileName, EventLogEntryType.Warning);
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog("Receving Integration Processing started", filePath, EventLogEntryType.Error);
                                            }
                                            if (RecevingTransactionFileDataPush.RecevingTransactionFileProcessing(dtFileData, dsIntegrationRECVHdrSetup, dsIntegrationRECVLineSetup, dsIntegrationRECVTaxSetup, dsIntegrationRECVDistSetup, drIntegrationSetup,SourceColumns, gpCmpDBConnStr, drIntegrationSetup["GPINTERID"].ToString().Trim(), drIntegrationSetup["RCVINVMAPID"].ToString().Trim(), Convert.ToBoolean(drIntegrationSetup["POPRECVFORMATACCOUNTSTRING"])))
                                            {
                                                File.Move(fileName, drIntegrationSetup["PROCESSEDPATH"].ToString().Trim() + @"\" + Path.GetFileNameWithoutExtension(fileName) + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + Path.GetExtension(fileName));

                                                if (Convert.ToBoolean(drIntegrationSetup["SUCCESSPOPRECVMAILENABLED"].ToString().Trim()) == true)
                                                {
                                                    emailErrMsg = "";
                                                    emailErrMsg =  NotificationsMail.sendNotifications("Receving Transaction: " + drIntegrationSetup["SUCCESSEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["SuccessNotification"].ToString().Replace("@IntegrationObject", "Receving Transaction Integration ") , drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSTOEMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSCCEMAILID"].ToString().Trim(), strConnString);
                                                    if (emailErrMsg != "")
                                                    {
                                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                        {
                                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                        }
                                                    }
                                                }
                                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                {
                                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                    FileLogger.WriteToFileLog("Receving Transaction Integrated Successfully.", filePath, EventLogEntryType.Error);
                                                }

                                            }
                                            else
                                            {

                                                File.Move(fileName, drIntegrationSetup["FAILEDPATH"].ToString().Trim() + @"\" + Path.GetFileNameWithoutExtension(fileName) + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + Path.GetExtension(fileName));

                                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPRECVMAILENABLED"].ToString().Trim()) == true)
                                                {
                                                    emailErrMsg = "";
                                                    emailErrMsg = NotificationsMail.sendNotifications("Receving Transaction: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Receiving Transaction Integration ") + " Error : " + RecevingTransactionFileDataPush.strExceptionMessage, drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                                    if (emailErrMsg != "")
                                                    {
                                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                        {
                                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                        }
                                                    }
                                                }
                                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                {
                                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                    FileLogger.WriteToFileLog("Receving Transaction Integration Failed.", filePath, EventLogEntryType.Error);
                                                }

                                            }

                                        }
                                        catch (Exception ex)
                                        {
                                            EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);

                                            if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPRECVMAILENABLED"].ToString().Trim()) == true)
                                            {
                                                emailErrMsg = "";
                                                emailErrMsg = NotificationsMail.sendNotifications("Receving Transaction: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Receving Transaction Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                                if (emailErrMsg != "")
                                                {
                                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                    {
                                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                        FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                    }
                                                }
                                            }
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                                            }
                                        }
                                        finally
                                        {
                                            dsIntegrationRECVHdrSetup.Dispose();
                                            dsIntegrationRECVLineSetup.Dispose();
                                            dsIntegrationRECVTaxSetup.Dispose();
                                            dsIntegrationRECVDistSetup.Dispose();
                                            dtMappingColumnName.Dispose();
                                        }


                                    }
                                }
                                else
                                {
                                    EventLogger.WriteToEventLog(("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), EventLogEntryType.Warning);

                                    if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPRECVMAILENABLED"].ToString().Trim()) == true)
                                    {
                                        emailErrMsg = "";
                                        emailErrMsg = NotificationsMail.sendNotifications("Receving Transaction: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Receving Transaction Integration ") + ("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                        if (emailErrMsg != "")
                                        {
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                            }
                                        }
                                    }
                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                    {
                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                        FileLogger.WriteToFileLog(("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), filePath, EventLogEntryType.Warning);
                                    }
                                }



                            }
                            catch (Exception ex)
                            {
                                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPRECVMAILENABLED"].ToString().Trim()) == true)
                                {
                                    emailErrMsg = "";
                                    emailErrMsg = NotificationsMail.sendNotifications("Receving Transaction: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Receving Transaction Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                    if (emailErrMsg != "")
                                    {
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                        }
                                    }

                                }
                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                {
                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                                }
                            }
                            finally
                            {
                                dtFileData.Dispose();
                            }



                        }
                        break;

                }

            }
            catch (Exception ex)
            {
                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOPRECVMAILENABLED"].ToString().Trim()) == true)
                {
                    emailErrMsg = "";
                    emailErrMsg =  NotificationsMail.sendNotifications("Receving Transaction: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim(), ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Invoice Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                    if (emailErrMsg != "")
                    {
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                        }
                    }
                }
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                }
            }


        }

        private void ProcessEnterMatchFiles(DataRow drIntegrationSetup)
        {
            try
            {

                switch (drIntegrationSetup["ENTMATCHINVFILETYPE"].ToString().Trim())
                {
                    case "CSV":
                        foreach (string fileName in Directory.GetFiles(drIntegrationSetup["INBOUNDPATH"].ToString().Trim(), drIntegrationSetup["ENTMATCHINVFILEPREFIX"].ToString().Trim() + "*." + drIntegrationSetup["ENTMATCHINVFILETYPE"].ToString().Trim()))
                        {
                            DataTable dtFileData = new DataTable();
                            EventLogger.WriteToEventLog("Enter Match Invoice File Processing started :" + fileName, EventLogEntryType.Warning);
                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                            {
                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                FileLogger.WriteToFileLog("Enter Match Invoice File Processing started :" + fileName, filePath, EventLogEntryType.Warning);
                            }
                            try
                            {
                                dtFileData = ConvertCSVtoDataTable(fileName);
                                if (dtFileData.Rows.Count > 0)
                                {
                                    string strSQL = SQLLibrary.SQLENTMATCHINVHeaderMapping(drIntegrationSetup["ENTMATCHINVMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLLine = SQLLibrary.SQLENTMATCHINVLineMapping(drIntegrationSetup["ENTMATCHINVMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLTax = SQLLibrary.SQLENTMATCHINVTaxMapping(drIntegrationSetup["ENTMATCHINVMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLDistr = SQLLibrary.SQLENTMATCHINVDistMapping(drIntegrationSetup["ENTMATCHINVMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    string strSQLMappingColumnNames = SQLLibrary.SQLENTMATCHINVHeaderMappingColumns(drIntegrationSetup["ENTMATCHINVMAPID"].ToString().Trim(), drIntegrationSetup["GPINTERID"].ToString().Trim());
                                    if (strSQL != "")
                                    {
                                        DataSet dsIntegrationENTMATCHINVHdrSetup = DBLibrary.GetDataSetFromSQLSCript(strSQL, "EnterMatchTrxHeader", strConnString);
                                        DataSet dsIntegrationENTMATCHINVLineSetup = DBLibrary.GetDataSetFromSQLSCript(strSQLLine, "EnterMatchTrxLine", strConnString);
                                        DataSet dsIntegrationENTMATCHINVTaxSetup = DBLibrary.GetDataSetFromSQLSCript(strSQLTax, "EnterMatchTrxLineTax", strConnString);
                                        DataSet dsIntegrationENTMATCHINVDistSetup = DBLibrary.GetDataSetFromSQLSCript(strSQLDistr, "EnterMatchTrxDist", strConnString);
                                        DataTable dtMappingColumnName = DBLibrary.GetDataTableFromSQLSCript(strSQLMappingColumnNames, strConnString);

                                        string[] SourceColumns = new string[dtMappingColumnName.Rows.Count];
                                        SourceColumns = dtMappingColumnName.Rows.OfType<DataRow>().Select(k => k[0].ToString()).ToArray();
                                        string gpCmpDBConnStr = "data source=" + drIntegrationSetup["GPSERVER"].ToString().Trim() + ";initial catalog=" + drIntegrationSetup["GPINTERID"].ToString().Trim() + ";integrated security=SSPI;persist security info=False;packet size=4096";

                                        try
                                        {

                                            EventLogger.WriteToEventLog("Enter Match Invoice Processing started :" + fileName, EventLogEntryType.Warning);
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog("Enter Match Invoice Processing started", filePath, EventLogEntryType.Error);
                                            }
                                            if (EnterMatchInvoiceFileDataPush.EnterMatchInvoiceFileProcessing(dtFileData, dsIntegrationENTMATCHINVHdrSetup, dsIntegrationENTMATCHINVLineSetup, dsIntegrationENTMATCHINVTaxSetup, dsIntegrationENTMATCHINVDistSetup, drIntegrationSetup, SourceColumns, gpCmpDBConnStr, drIntegrationSetup["GPINTERID"].ToString().Trim(), drIntegrationSetup["ENTMATCHINVMAPID"].ToString().Trim(), Convert.ToBoolean(drIntegrationSetup["ENTMATCHINVFORMATACCOUNTSTRING"])))
                                            {
                                                File.Move(fileName, drIntegrationSetup["PROCESSEDPATH"].ToString().Trim() + @"\" + Path.GetFileNameWithoutExtension(fileName) + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + Path.GetExtension(fileName));

                                                if (Convert.ToBoolean(drIntegrationSetup["SUCCESSENTMATCHINVMAILENABLED"].ToString().Trim()) == true)
                                                {
                                                    emailErrMsg = "";
                                                    emailErrMsg = NotificationsMail.sendNotifications("Enter Match Invoice: " + drIntegrationSetup["SUCCESSEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["SuccessNotification"].ToString().Replace("@IntegrationObject", "Enter Match Invoice Integration "), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSTOEMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSCCEMAILID"].ToString().Trim(), strConnString);
                                                    if (emailErrMsg != "")
                                                    {
                                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                        {
                                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                        }
                                                    }
                                                }
                                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                {
                                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                    FileLogger.WriteToFileLog("Enter Match Invoice Integrated Successfully.", filePath, EventLogEntryType.Error);
                                                }

                                            }
                                            else
                                            {

                                                File.Move(fileName, drIntegrationSetup["FAILEDPATH"].ToString().Trim() + @"\" + Path.GetFileNameWithoutExtension(fileName) + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + Path.GetExtension(fileName));

                                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREENTMATCHINVMAILENABLED"].ToString().Trim()) == true)
                                                {
                                                    emailErrMsg = "";
                                                    emailErrMsg = NotificationsMail.sendNotifications("Enter Match Invoice: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Enter Match Invoice Integration ") + " Error : " + RecevingTransactionFileDataPush.strExceptionMessage, drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                                    if (emailErrMsg != "")
                                                    {
                                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                        {
                                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                        }
                                                    }
                                                }
                                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                {
                                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                    FileLogger.WriteToFileLog("Enter Match Invoice Integration Failed.", filePath, EventLogEntryType.Error);
                                                }

                                            }

                                        }
                                        catch (Exception ex)
                                        {
                                            EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);

                                            if (Convert.ToBoolean(drIntegrationSetup["FAILUREENTMATCHINVMAILENABLED"].ToString().Trim()) == true)
                                            {
                                                emailErrMsg = "";
                                                emailErrMsg = NotificationsMail.sendNotifications("Enter Match Invoice: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Enter Match Invoice Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                                if (emailErrMsg != "")
                                                {
                                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                                    {
                                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                        FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                                    }
                                                }
                                            }
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                                            }
                                        }
                                        finally
                                        {
                                            dsIntegrationENTMATCHINVHdrSetup.Dispose();
                                            dsIntegrationENTMATCHINVLineSetup.Dispose();
                                            dsIntegrationENTMATCHINVTaxSetup.Dispose();
                                            dsIntegrationENTMATCHINVDistSetup.Dispose();
                                            dtMappingColumnName.Dispose();
                                        }


                                    }
                                }
                                else
                                {
                                    EventLogger.WriteToEventLog(("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), EventLogEntryType.Warning);

                                    if (Convert.ToBoolean(drIntegrationSetup["FAILUREENTMATCHINVMAILENABLED"].ToString().Trim()) == true)
                                    {
                                        emailErrMsg = "";
                                        emailErrMsg = NotificationsMail.sendNotifications("Enter Match Invoice: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Enter Match Invoice Integration ") + ("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                        if (emailErrMsg != "")
                                        {
                                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                            {
                                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                                FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                            }
                                        }
                                    }
                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                    {
                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                        FileLogger.WriteToFileLog(("Warning: No Data found in the file " + drIntegrationSetup["INBOUNDPATH"].ToString() + @"\" + fileName), filePath, EventLogEntryType.Warning);
                                    }
                                }



                            }
                            catch (Exception ex)
                            {
                                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREENTMATCHINVMAILENABLED"].ToString().Trim()) == true)
                                {
                                    emailErrMsg = "";
                                    emailErrMsg = NotificationsMail.sendNotifications("Enter Match Invoice: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Enter Match Invoice Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                    if (emailErrMsg != "")
                                    {
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                        }
                                    }

                                }
                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                {
                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                                }
                            }
                            finally
                            {
                                dtFileData.Dispose();
                            }



                        }
                        break;

                }

            }
            catch (Exception ex)
            {
                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                if (Convert.ToBoolean(drIntegrationSetup["FAILUREENTMATCHINVMAILENABLED"].ToString().Trim()) == true)
                {
                    emailErrMsg = "";
                    emailErrMsg = NotificationsMail.sendNotifications("Enter Match Invoice: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim(), ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Enter Match Invoice Integration ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                    if (emailErrMsg != "")
                    {
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                        }
                    }
                }
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                }
            }


        }

        private void ProcessPurchaseOrderExport(DataRow drIntegrationSetup)
        {
            try
            {

                switch (drIntegrationSetup["EXPORTPOFILETYPE"].ToString().Trim())
                {
                    case "CSV":

                        string fileName = drIntegrationSetup["EXPORTPOPATH"].ToString().Trim();

                       
                        DataTable dtShipmentDetails = new DataTable();


                            EventLogger.WriteToEventLog("Purchase Order Data Export Processing started :" + fileName, EventLogEntryType.Warning);
                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                            {
                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                FileLogger.WriteToFileLog("Purchase Order Data Export Processing started :" + fileName, filePath, EventLogEntryType.Warning);
                            }
                            try
                            {
                                string strSQLShipmentTableNames = SQLLibrary.SQLPURCHASEORDERTABLENAMES();
                                string gpCmpDBConnectionStr = "data source=" + drIntegrationSetup["GPSERVER"].ToString().Trim() + ";initial catalog=" + drIntegrationSetup["GPINTERID"].ToString().Trim() + ";integrated security=SSPI;persist security info=False;packet size=4096";
                                dtShipmentDetails = DBLibrary.GetDataTableFromSQLSCript(strSQLShipmentTableNames, gpCmpDBConnectionStr);

                                if (dtShipmentDetails.Rows.Count > 0)
                                {

                                    ExportPurchaseOrderDataToCSV(dtShipmentDetails, drIntegrationSetup, fileName, gpCmpDBConnectionStr);
       
                                }
                                else
                                {
                                    EventLogger.WriteToEventLog("No Data found in the Purchase Order Export Table :" + fileName, EventLogEntryType.Warning);
                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                    {
                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                        FileLogger.WriteToFileLog("No Data found in the Purchase Order Export Table :" + fileName, filePath, EventLogEntryType.Warning);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOEXPORTMAILENABLED"].ToString().Trim()) == true)
                                {
                                    emailErrMsg = "";
                                    emailErrMsg = NotificationsMail.sendNotifications("Purchase Order Data Export: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Purchase Order Export Data ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                    if (emailErrMsg != "")
                                    {
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                        }
                                    }

                                }
                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                {
                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                                }
                            }
                            finally
                            {
                            dtShipmentDetails.Dispose();
                            }
 
                        break;

                }

            }
            catch (Exception ex)
            {
                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOEXPORTMAILENABLED"].ToString().Trim()) == true)
                {
                    emailErrMsg = "";
                    emailErrMsg = NotificationsMail.sendNotifications("Purchase Order Export data: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim(), ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Purchase Order Export Data ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                    if (emailErrMsg != "")
                    {
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                        }
                    }
                }
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                }
            }


        }

        private void ProcessShipmentExport(DataRow drIntegrationSetup)
        {
            try
            {

                switch (drIntegrationSetup["EXPORTSHIPMENTTYPE"].ToString().Trim())
                {
                    case "CSV":

                        string fileName = drIntegrationSetup["EXPORTSHIPMENTPATH"].ToString().Trim();

                        //fileName = fileName + @"\ShipmentTrx_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + '.' + drIntegrationSetup["EXPORTSHIPMENTTYPE"].ToString().Trim();

                        DataTable dtShipmentDetails = new DataTable();


                        EventLogger.WriteToEventLog("Shipment Data Export Processing started :" + fileName, EventLogEntryType.Warning);
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog("Shipment Data Export Processing started :" + fileName, filePath, EventLogEntryType.Warning);
                        }
                        try
                        {
                            string strSQLShipmentTableNames = SQLLibrary.SQLSHIPMENTTABLENAMES();
                            string gpCmpDBConnectionStr = "data source=" + drIntegrationSetup["GPSERVER"].ToString().Trim() + ";initial catalog=" + drIntegrationSetup["GPINTERID"].ToString().Trim() + ";integrated security=SSPI;persist security info=False;packet size=4096";
                            dtShipmentDetails = DBLibrary.GetDataTableFromSQLSCript(strSQLShipmentTableNames, gpCmpDBConnectionStr);

                            if (dtShipmentDetails.Rows.Count > 0)
                            {

                                ExportShipmentDataToCSV(dtShipmentDetails, drIntegrationSetup, fileName, gpCmpDBConnectionStr);

                                //if (ExportDataTableToCSV(dtShipmentDetails, drIntegrationSetup, fileName))
                                //{
                                //    DBLibrary.SQLUpdateExportedRecords("sp_Update_GPDataExport_Records", gpCmpDBConnectionStr, 2);
                                //}
                                //else
                                //{
                                //    EventLogger.WriteToEventLog("No Data Exported in the Shipment Export Table :" + fileName, EventLogEntryType.Warning);
                                //    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                //    {
                                //        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                //        FileLogger.WriteToFileLog("No Data Exported in the Shipment Export Table :" + fileName, filePath, EventLogEntryType.Warning);
                                //    }
                                //}
                            }
                            else
                            {
                                EventLogger.WriteToEventLog("No Data found in the Shipment Export Table :" + fileName, EventLogEntryType.Warning);
                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                {
                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                    FileLogger.WriteToFileLog("No Data found in the Shipment Export Table :" + fileName, filePath, EventLogEntryType.Warning);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                            if (Convert.ToBoolean(drIntegrationSetup["FAILURESHIPEXPORTMAILENABLED"].ToString().Trim()) == true)
                            {
                                emailErrMsg = "";
                                emailErrMsg = NotificationsMail.sendNotifications("Shipment Data Export: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Shipment Data Export ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                if (emailErrMsg != "")
                                {
                                    if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                    {
                                        string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                        FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                    }
                                }

                            }
                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                            {
                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                            }
                        }
                        finally
                        {
                            dtShipmentDetails.Dispose();
                        }

                        break;

                }

            }
            catch (Exception ex)
            {
                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                if (Convert.ToBoolean(drIntegrationSetup["FAILURESHIPEXPORTMAILENABLED"].ToString().Trim()) == true)
                {
                    emailErrMsg = "";
                    emailErrMsg = NotificationsMail.sendNotifications("Shipment Data Export: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim(), ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", " Shipment Data Export ") + " Error : " + ex.Message.ToString(), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                    if (emailErrMsg != "")
                    {
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                        }
                    }
                }
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Error);
                }
            }


        }

        public static DataTable ConvertCSVtoDataTable(string strFilePath)
        {
            //DataTable dt = new DataTable();
            //using (StreamReader sr = new StreamReader(strFilePath))
            //{
            //    string[] headers = sr.ReadLine().Split(',');
            //    foreach (string header in headers)
            //    {
            //        dt.Columns.Add(header);
            //    }
            //    while (!sr.EndOfStream)
            //    {
            //        string[] rows = sr.ReadLine().Split(',');
            //        DataRow dr = dt.NewRow();
            //        for (int i = 0; i < headers.Length; i++)
            //        {
            //            dr[i] = rows[i];
            //        }
            //        dt.Rows.Add(dr);
            //    }

            //}


            //return dt;

            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                // Read the first line to create the columns
                string line = sr.ReadLine();
                string[] headers = Regex.Split(line, ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                foreach (string header in headers)
                {
                    dt.Columns.Add(header);
                }

                // Read the rest of the lines and add them to the DataTable
                while ((line = sr.ReadLine()) != null)
                {
                    string[] fields = Regex.Split(line, ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                    DataRow dr = dt.NewRow();
                    dr.ItemArray = fields;
                    dt.Rows.Add(dr);
                }
            }

            return dt;
        }

        public void ExportPurchaseOrderDataToCSV(DataTable dataTable,DataRow drIntegrationSetup, string fileName, string gpCmpDBConnectionStr)
        {

            Boolean vExported = false;
            string strFolderPath;

            try
            {
                string strSQLExportMappingColumnNames = SQLLibrary.SQLPOEXPORTHeaderMappingColumns();
                DataTable dtExportColumnName = DBLibrary.GetDataTableFromSQLSCript(strSQLExportMappingColumnNames, gpCmpDBConnectionStr);

                string[] exportDisntColumns = new string[dtExportColumnName.Rows.Count];
                exportDisntColumns = dtExportColumnName.Rows.OfType<DataRow>().Select(k => k[0].ToString().Trim()).ToArray();

                DataView dvExportDistinctValues = new DataView(dataTable);
                DataTable dtExportSourceFileData = new DataTable();
                dtExportSourceFileData = dvExportDistinctValues.ToTable(true, exportDisntColumns);

                for (int iSourceDataCount = 0; iSourceDataCount < dtExportSourceFileData.Rows.Count; iSourceDataCount++)
                {
                    try
                    {
                        DataTable dtSourceFileDataExportPO = dataTable.Select("PONUMBER = '" + Convert.ToString(dtExportSourceFileData.Rows[iSourceDataCount]["PONUMBER"]) + "'", "PONUMBER").CopyToDataTable();

                        if (dtSourceFileDataExportPO.Rows.Count > 0)
                        {
                            string poNumber = dtSourceFileDataExportPO.Rows[0]["PONUMBER"].ToString().Trim().Replace("\"", "");

                            strFolderPath = fileName + @"\" + drIntegrationSetup["EXPORTPOPREFIX"].ToString().Trim() + "_" + poNumber + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + '.' + drIntegrationSetup["EXPORTPOFILETYPE"].ToString().Trim();
                            using (StreamWriter writer = new StreamWriter(strFolderPath))
                            {
                                // Write header
                                for (int i = 0; i < dtSourceFileDataExportPO.Columns.Count; i++)
                                {
                                    writer.Write(dtSourceFileDataExportPO.Columns[i].ToString().Trim());
                                    if (i < dtSourceFileDataExportPO.Columns.Count - 1)
                                        writer.Write(",");
                                }
                                writer.WriteLine();

                                // Write rows
                                foreach (DataRow row in dtSourceFileDataExportPO.Rows)
                                {
                                    for (int i = 0; i < dtSourceFileDataExportPO.Columns.Count; i++)
                                    {
                                        if (row[i].ToString().Trim().Contains(","))
                                        {
                                            writer.Write($"\"{row[i].ToString().Trim().Replace("\"", "\"\"")}\"");
                                        }
                                        else
                                        {
                                            writer.Write(row[i].ToString().Trim());
                                        }

                                        //writer.Write(row[i].ToString().Trim());
                                        //writer.Write($"\"{row[i].ToString().Replace("\"", "\"\"")}\"");
                                        if (i < dtSourceFileDataExportPO.Columns.Count - 1)
                                            writer.Write(",");
                                    }
                                    writer.WriteLine();
                                }
                            }

                            DataTable dtFileData = ConvertCSVtoDataTable(strFolderPath);

                            if (dtFileData.Rows.Count > 0)
                            {
                                DBLibrary.SQLUpdateExportedRecords("sp_Update_GPDataExport_Records", gpCmpDBConnectionStr, true, 1,1, poNumber);
                                //vExported = true;

                                if (Convert.ToBoolean(drIntegrationSetup["SUCCESSPOEXPORTMAILENABLED"].ToString().Trim()) == true)
                                {
                                    emailErrMsg = "";
                                    emailErrMsg = NotificationsMail.sendNotifications("Purchase Order Data Exported Successfully: " + drIntegrationSetup["SUCCESSEMAILSUBJECT"].ToString().Trim() + " " + strFolderPath, ConfigurationManager.AppSettings["SuccessNotification"].ToString().Replace("@IntegrationObject", "Purchase Order Export Data "), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSTOEMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSCCEMAILID"].ToString().Trim(), strConnString);
                                    if (emailErrMsg != "")
                                    {
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                        }
                                    }
                                }
                                EventLogger.WriteToEventLog("Purchase Order Data Exported Successfully :" + strFolderPath, EventLogEntryType.Warning);
                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                {
                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                    FileLogger.WriteToFileLog("Purchase Order Data Exported Successfully.", filePath, EventLogEntryType.Error);
                                }

                            }
                            else
                            {
                                EventLogger.WriteToEventLog("No Data Exported in the Purchase Order Export Table :" + strFolderPath, EventLogEntryType.Warning);

                                if (Convert.ToBoolean(drIntegrationSetup["FAILUREPOEXPORTMAILENABLED"].ToString().Trim()) == true)
                                {
                                    emailErrMsg = "";
                                    emailErrMsg = NotificationsMail.sendNotifications("Purchase Order Data Export: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Purchase Order Data Export ") + " Error : " + RecevingTransactionFileDataPush.strExceptionMessage, drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                    if (emailErrMsg != "")
                                    {
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                        }
                                    }
                                }

                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                {
                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                    FileLogger.WriteToFileLog("No Data Exported in the Purchase Order Export Table :" + strFolderPath, filePath, EventLogEntryType.Warning);
                                }
                            }
                        }
                        else
                        {
                            EventLogger.WriteToEventLog("No Data row available in the Purchase Order Export Table :" + fileName, EventLogEntryType.Warning);
                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                            {
                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                FileLogger.WriteToFileLog("No Data row available in the Purchase Order Export Table :" + fileName, filePath, EventLogEntryType.Warning);
                            }
                        }

                    }
                    catch (Exception ex)
                    {
                        EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Warning);
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Warning);
                }
            }
            finally
            {
                
            }

            //return vExported;
        }

        public void ExportShipmentDataToCSV(DataTable dataTable, DataRow drIntegrationSetup, string fileName, string gpCmpDBConnectionStr)
        {

            Boolean vExported = false;
            string strFolderPath;

            try
            {
                string strSQLExportMappingColumnNames = SQLLibrary.SQLSHIPEXPORTHeaderMappingColumns();
                DataTable dtExportColumnName = DBLibrary.GetDataTableFromSQLSCript(strSQLExportMappingColumnNames, gpCmpDBConnectionStr);

                string[] exportDisntColumns = new string[dtExportColumnName.Rows.Count];
                exportDisntColumns = dtExportColumnName.Rows.OfType<DataRow>().Select(k => k[0].ToString().Trim()).ToArray();

                DataView dvExportDistinctValues = new DataView(dataTable);
                DataTable dtExportSourceFileData = new DataTable();
                dtExportSourceFileData = dvExportDistinctValues.ToTable(true, exportDisntColumns);

                for (int iSourceDataCount = 0; iSourceDataCount < dtExportSourceFileData.Rows.Count; iSourceDataCount++)
                {
                    try
                    {
                        DataTable dtSourceFileDataExportShipment = dataTable.Select("POPRCTNM = '" + Convert.ToString(dtExportSourceFileData.Rows[iSourceDataCount]["POPRCTNM"]) + "'", "POPRCTNM").CopyToDataTable();

                        if (dtSourceFileDataExportShipment.Rows.Count > 0)
                        {
                            string receptNumber = dtSourceFileDataExportShipment.Rows[0]["POPRCTNM"].ToString().Trim().Replace("\"", "");

                            strFolderPath = fileName + @"\" + drIntegrationSetup["EXPORTSHIPMENTPREFIX"].ToString().Trim() + "_" + receptNumber + "_" + DateTime.Today.ToString("yyyyMMdd") + "_" + DateTime.Now.ToString("HHmmss") + '.' + drIntegrationSetup["EXPORTSHIPMENTTYPE"].ToString().Trim();
                            using (StreamWriter writer = new StreamWriter(strFolderPath))
                            {
                                // Write header
                                for (int i = 0; i < dtSourceFileDataExportShipment.Columns.Count; i++)
                                {
                                    writer.Write(dtSourceFileDataExportShipment.Columns[i].ToString().Trim());
                                    if (i < dtSourceFileDataExportShipment.Columns.Count - 1)
                                        writer.Write(",");
                                }
                                writer.WriteLine();

                                // Write rows
                                foreach (DataRow row in dtSourceFileDataExportShipment.Rows)
                                {
                                    for (int i = 0; i < dtSourceFileDataExportShipment.Columns.Count; i++)
                                    {
                                        if (row[i].ToString().Trim().Contains(","))
                                        {
                                            writer.Write($"\"{row[i].ToString().Trim().Replace("\"", "\"\"")}\"");
                                        }
                                        else
                                        {
                                            writer.Write(row[i].ToString().Trim());
                                        }

                                        //writer.Write(row[i].ToString().Trim());
                                        //writer.Write($"\"{row[i].ToString().Replace("\"", "\"\"")}\"");
                                        if (i < dtSourceFileDataExportShipment.Columns.Count - 1)
                                            writer.Write(",");
                                    }
                                    writer.WriteLine();
                                }
                            }

                            DataTable dtFileData = ConvertCSVtoDataTable(strFolderPath);

                            if (dtFileData.Rows.Count > 0)
                            {
                                DBLibrary.SQLUpdateExportedRecords("sp_Update_GPDataExport_Records", gpCmpDBConnectionStr, true, 2, 1, receptNumber);
                                //vExported = true;

                                if (Convert.ToBoolean(drIntegrationSetup["SUCCESSSHIPEXPORTMAILENABLED"].ToString().Trim()) == true)
                                {
                                    emailErrMsg = "";
                                    emailErrMsg = NotificationsMail.sendNotifications("Shipment Data Exported Successfully: " + drIntegrationSetup["SUCCESSEMAILSUBJECT"].ToString().Trim() + " " + strFolderPath, ConfigurationManager.AppSettings["SuccessNotification"].ToString().Replace("@IntegrationObject", "Shipment Export Data "), drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSTOEMAILID"].ToString().Trim(), drIntegrationSetup["SUCCESSCCEMAILID"].ToString().Trim(), strConnString);
                                    if (emailErrMsg != "")
                                    {
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                        }
                                    }
                                }
                                EventLogger.WriteToEventLog("Shipment Data Exported Successfully :" + strFolderPath, EventLogEntryType.Warning);
                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                {
                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                    FileLogger.WriteToFileLog("Shipment Data Exported Successfully.", filePath, EventLogEntryType.Error);
                                }

                            }
                            else
                            {
                                EventLogger.WriteToEventLog("No Data Exported in the Shipment Export Table :" + strFolderPath, EventLogEntryType.Warning);

                                if (Convert.ToBoolean(drIntegrationSetup["FAILURESHIPEXPORTMAILENABLED"].ToString().Trim()) == true)
                                {
                                    emailErrMsg = "";
                                    emailErrMsg = NotificationsMail.sendNotifications("Shipment Data Export: " + drIntegrationSetup["FAILUREEMAILSUBJECT"].ToString().Trim() + " " + fileName, ConfigurationManager.AppSettings["FailureNotification"].ToString().Replace("@IntegrationObject", "Shipment Data Export ") + " Error : " + RecevingTransactionFileDataPush.strExceptionMessage, drIntegrationSetup["FROMMAILID"].ToString().Trim(), drIntegrationSetup["FAILURETOEMAILID"].ToString().Trim(), drIntegrationSetup["FAILURECCEMAILID"].ToString().Trim(), strConnString);
                                    if (emailErrMsg != "")
                                    {
                                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                        {
                                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                            FileLogger.WriteToFileLog(emailErrMsg, filePath, EventLogEntryType.Error);
                                        }
                                    }
                                }

                                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                                {
                                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                    FileLogger.WriteToFileLog("No Data Exported in the Shipment Export Table :" + strFolderPath, filePath, EventLogEntryType.Warning);
                                }
                            }
                        }
                        else
                        {
                            EventLogger.WriteToEventLog("No Data row available in the Shipment Export Table :" + fileName, EventLogEntryType.Warning);
                            if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                            {
                                string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                                FileLogger.WriteToFileLog("No Data row available in the Shipment Export Table :" + fileName, filePath, EventLogEntryType.Warning);
                            }
                        }

                    }
                    catch (Exception ex)
                    {
                        EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Warning);
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                EventLogger.WriteToEventLog(ex.Message.ToString(), EventLogEntryType.Error);
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog(ex.Message.ToString(), filePath, EventLogEntryType.Warning);
                }
            }
            finally
            {

            }

            //return vExported;
        }

        public Boolean ExportDataTableToCSVBulk(DataTable dataTable, string strFolderPath)
        {

            Boolean vExported = false;

            

            using (StreamWriter writer = new StreamWriter(strFolderPath))
            {
                // Write header
                for (int i = 0; i < dataTable.Columns.Count; i++)
                {
                    writer.Write(dataTable.Columns[i].ToString().Trim());
                    if (i < dataTable.Columns.Count - 1)
                        writer.Write(",");
                }
                writer.WriteLine();

                // Write rows
                foreach (DataRow row in dataTable.Rows)
                {
                    for (int i = 0; i < dataTable.Columns.Count; i++)
                    {
                        if (row[i].ToString().Trim().Contains(","))
                        {
                            writer.Write($"\"{row[i].ToString().Trim().Replace("\"", "\"\"")}\"");
                        }
                        else
                        {
                            writer.Write(row[i].ToString().Trim());
                        }
                        if (i < dataTable.Columns.Count - 1)
                            writer.Write(",");
                    }
                    writer.WriteLine();
                }


            }
            DataTable dtFileData = ConvertCSVtoDataTable(strFolderPath);

            if (dtFileData.Rows.Count > 0)
            {
                vExported = true;
            }

            return vExported;
        }

        protected override void OnStop()
        {
            appTimer.Enabled = false;
            appTimer.Dispose();
            appTimer = null;
            EventLogger.WriteToEventLog("Shutting down SUTI AP GP Integration Service Version:", 0);

        }
    }
}
