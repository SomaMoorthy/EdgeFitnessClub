using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Dynamics.GP.eConnect;
using Microsoft.Dynamics.GP.eConnect.Serialization;
using SUTIAPGPIntegrationService.Utilities;
using System.Diagnostics;
using System.Configuration;

using System.IO;
using System.Xml;
using System.Xml.Serialization;
using System.Globalization;


namespace SUTIAPGPIntegrationService.Processors
{
    class PaymentFileDataPush
    {
        public static Boolean bAutoGenerateNum = false;
        public static string strNextNumber = "";
        public static string strExceptionMessage = "";

        public static Boolean PaymentFileDataProcessing(DataTable dtSourceFileDataSource, DataSet dsMappingSetup, DataSet dsIntegrationDistSetup,DataRow drIntegrationSetup, string[] strSourceColumnNames, string gpCmpDBConnStr, string strCompanyName, string strPaymentMapID, Boolean PYMFORMATACCOUNTSTRING)
        {

           Boolean bReturnValue = true;

           strExceptionMessage = "";
            try
            {

                DateTimeFormatInfo dateFormat = new CultureInfo("en-US").DateTimeFormat;
                eConnectMethods eConCall = new eConnectMethods();
                eConnectType eConnect = new eConnectType();

                DataView dvDistinctValues = new DataView(dtSourceFileDataSource);

                DataTable dtSourceFileData = new DataTable();





                dtSourceFileData = dvDistinctValues.ToTable(true, strSourceColumnNames);




                int iSourceDataCount, iMappingSetup;
                for (iSourceDataCount = 0; iSourceDataCount < dtSourceFileData.Rows.Count; iSourceDataCount++)
                {
                    try
                    {

                        PMManualCheckType pyTransactiontype = new PMManualCheckType();
                        taPMManualCheck pyTransaction = new taPMManualCheck();
                        for (iMappingSetup = 0; iMappingSetup < dsMappingSetup.Tables["PaymentHeader"].Rows.Count; iMappingSetup++)
                        {



                            switch (dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup]["eConnect Field"].ToString())
                            {
                                case "BACHNUMB":

                                    pyTransaction.BACHNUMB = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PMNTNMBR":

                                    pyTransaction.PMNTNMBR = strAutoValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount],gpCmpDBConnStr);
                                    break;

                                case "VENDORID":
                                    pyTransaction.VENDORID = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DOCNUMBR":
                                    pyTransaction.DOCNUMBR = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DOCAMNT":
                                    pyTransaction.DOCAMNT = dcValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DOCDATE":
                                    pyTransaction.DOCDATE = dtValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "PSTGDATE":
                                    pyTransaction.PSTGDATE = dtValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "PYENTTYP":
                                    pyTransaction.PYENTTYP = shValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CARDNAME":
                                    pyTransaction.CARDNAME = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CURNCYID":
                                    pyTransaction.CURNCYID = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CHEKBKID":
                                    pyTransaction.CHEKBKID = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TRXDSCRN":
                                    pyTransaction.TRXDSCRN = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "XCHGRATE":
                                    pyTransaction.XCHGRATE = dcValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RATETPID":
                                    pyTransaction.RATETPID = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "EXPNDATE":
                                    pyTransaction.EXPNDATE = dtValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "EXCHDATE":
                                    pyTransaction.EXCHDATE = dtValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");

                                    break;

                                case "EXGTBDSC":
                                    pyTransaction.EXGTBDSC = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "EXTBLSRC":
                                    pyTransaction.EXTBLSRC = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RATEEXPR":
                                    pyTransaction.RATEEXPR = shValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DYSTINCR":
                                    pyTransaction.DYSTINCR = shValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RATEVARC":
                                    pyTransaction.RATEVARC = dcValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TRXDTDEF":
                                    pyTransaction.TRXDTDEF = shValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RTCLCMTD":
                                    pyTransaction.RTCLCMTD = shValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PRVDSLMT":
                                    pyTransaction.PRVDSLMT = shValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DATELMTS":
                                    pyTransaction.DATELMTS = shValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TIME1":
                                    pyTransaction.TIME1 = dtValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd HH:mm:ss");
                                    break;

                                case "MDFUSRID":
                                    pyTransaction.MDFUSRID = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PTDUSRID":
                                    pyTransaction.PTDUSRID = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "BatchCHEKBKID":
                                    pyTransaction.BatchCHEKBKID = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CREATEDIST":
                                    pyTransaction.CREATEDIST = shValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RequesterTrx":
                                    pyTransaction.RequesterTrx = shValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND1":
                                    pyTransaction.USRDEFND1 = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND2":
                                    pyTransaction.USRDEFND2 = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND3":
                                    pyTransaction.USRDEFND3 = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND4":
                                    pyTransaction.USRDEFND4 = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND5":
                                    pyTransaction.USRDEFND5 = strValue(dsMappingSetup.Tables["PaymentHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;




                            }



                        }




                        DataTable dtSourceFileDataDist = dtSourceFileDataSource.Select("[Document Number] = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["Document Number"]) + "'", "Document Number").CopyToDataTable();

                        taPMDistribution_ItemsTaPMDistribution[] PMDistributionvaluesArr = new taPMDistribution_ItemsTaPMDistribution[dtSourceFileDataDist.Rows.Count];


                        for (int iSourceDataDistCount = 0; iSourceDataDistCount < dtSourceFileDataDist.Rows.Count; iSourceDataDistCount++)
                        {
                            taPMDistribution_ItemsTaPMDistribution PMDistributionvalues = new taPMDistribution_ItemsTaPMDistribution();


                            for (iMappingSetup = 0; iMappingSetup < dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows.Count; iMappingSetup++)
                            {

                                switch (dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup]["eConnect Field"].ToString())
                                {
                                    case "VCHRNMBR":

                                        PMDistributionvalues.VCHRNMBR = pyTransaction.PMNTNMBR;
                                        break;

                                    case "VENDORID":
                                        PMDistributionvalues.VENDORID = strValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "DOCTYPE":
                                        PMDistributionvalues.DOCTYPE = shValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "DSTSQNUM":
                                        PMDistributionvalues.DSTSQNUM = shValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "DISTTYPE":
                                        PMDistributionvalues.DISTTYPE = shValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "DistRef":
                                        PMDistributionvalues.DistRef = strValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "ACTINDX":
                                        PMDistributionvalues.ACTINDX = shValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "ACTNUMST":

                                        if (PYMFORMATACCOUNTSTRING == true)
                                        {

                                            PMDistributionvalues.ACTNUMST = strValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        }
                                        else
                                        {
                                            PMDistributionvalues.ACTNUMST = strAccountNumber(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount],gpCmpDBConnStr);
                                        }
                                        break;

                                    case "DEBITAMT":

                                        decimal dcTempDRValue = dcValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);

                                        if (dsIntegrationDistSetup.Tables["PaymentDistribution"].Select("[eConnect Field] = 'SEPERATECREDIT' and [ValueorColumn] = 0").Count() == 1)
                                        {
                                            if (dcTempDRValue > 0)
                                            {
                                                PMDistributionvalues.DEBITAMT = dcTempDRValue;
                                            }
                                            else
                                            {
                                                PMDistributionvalues.DEBITAMT = 0;
                                            }
                                        }
                                        else
                                        {
                                            PMDistributionvalues.DEBITAMT = dcTempDRValue;
                                        }


                                        break;

                                    case "CRDTAMNT":


                                        decimal dcTempCRValue = dcValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);

                                        if (dsIntegrationDistSetup.Tables["PaymentDistribution"].Select("[eConnect Field] = 'SEPERATECREDIT' and [ValueorColumn] = 0").Count() == 1)
                                        {
                                            if (dcTempCRValue < 0)
                                            {
                                                PMDistributionvalues.CRDTAMNT = -dcTempCRValue;
                                            }
                                            else
                                            {
                                                PMDistributionvalues.CRDTAMNT = 0;
                                            }
                                        }
                                        else
                                        {
                                            if (dcTempCRValue < 0)
                                            {
                                                PMDistributionvalues.CRDTAMNT = -dcTempCRValue;
                                            }
                                            else
                                            {
                                                PMDistributionvalues.CRDTAMNT = dcTempCRValue;
                                            }
                                        }
                                        break;

                                    case "RequesterTrx":
                                        PMDistributionvalues.RequesterTrx = shValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "USRDEFND1":

                                        PMDistributionvalues.USRDEFND1 = strValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "USRDEFND2":
                                        PMDistributionvalues.USRDEFND2 = strValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "USRDEFND3":
                                        PMDistributionvalues.USRDEFND3 = strValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "USRDEFND4":
                                        PMDistributionvalues.USRDEFND4 = strValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "USRDEFND5":
                                        PMDistributionvalues.USRDEFND5 = strValue(dsIntegrationDistSetup.Tables["PaymentDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;
                                }
                            }



                            //if (PMDistributionvalues.USRDEFND1 != "")
                            //{
                            //    if (PMDistributionvalues.USRDEFND1 != strCompanyName)
                            //    {
                            //        PMDistributionvalues.ACTNUMST = DBLibrary.GetScalarValue(SQLLibrary.SQLIntercompanyTempAccount(strInvoiceMapID, strCompanyName), Convert.ToString(ConfigurationManager.AppSettings["SUTIAPGPServiceCon"])).Trim();
                            //        if (PMDistributionvalues.ACTINDX != 0)
                            //        {
                            //            PMDistributionvalues.ACTINDX = Convert.ToInt16(DBLibrary.GetScalarValue(PMDistributionvalues.ACTNUMST, gpCmpDBConnStr).Trim());

                            //        }


                            //    }
                            //    else
                            //    {
                            //        PMDistributionvalues.USRDEFND1 = "";
                            //        PMDistributionvalues.USRDEFND2 = "";
                            //    }
                            //}
                            //else
                            //{
                            //    PMDistributionvalues.USRDEFND1 = "";
                            //    PMDistributionvalues.USRDEFND2 = "";
                            //}

                            PMDistributionvaluesArr[iSourceDataDistCount] = PMDistributionvalues;

                        }


                        pyTransactiontype.taPMManualCheck = pyTransaction;

                        pyTransactiontype.taPMDistribution_Items = PMDistributionvaluesArr;



                        PMDistributionvaluesArr = null;

                        PMManualCheckType[] myPMtrx = { pyTransactiontype };
                        eConnect.PMManualCheckType = myPMtrx;

                        FileStream fs = new FileStream("PaymentTransaction.xml", FileMode.Create);


                        XmlTextWriter writer = new XmlTextWriter(fs, new UTF8Encoding());
                        XmlSerializer serializer = new XmlSerializer(eConnect.GetType());
                        serializer.Serialize(writer, eConnect);

                        writer.Close();


                        XmlDocument xmldoc = new XmlDocument();
                        xmldoc.Load("PaymentTransaction.xml");
                        string PaymentTransaction = xmldoc.OuterXml;

                        eConCall.CreateTransactionEntity(gpCmpDBConnStr, PaymentTransaction);
                        pyTransactiontype = null;

                    }
                    catch (Exception ex)
                    {

                        EventLogger.WriteToEventLog(("Error: " + ex.Message.ToString()), EventLogEntryType.Error);
                        bReturnValue = false;

                        if (strExceptionMessage != "")
                        {
                            strExceptionMessage = strExceptionMessage + ex.Message.ToString();
                        }
                        else
                        {
                            strExceptionMessage = ex.Message.ToString();
                        }
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog(strExceptionMessage, filePath, EventLogEntryType.Error);
                        }
                        //if (bAutoGenerateNum)
                        //{
                        //    if (strNextNumber != "")
                        //    {
                        //        using (GetNextDocNumbers getDocNumbers = new GetNextDocNumbers())
                        //        {
                        //            using (DocumentRollback docRollBack = new DocumentRollback())
                        //            {

                        //                RollBackDocument RollbackDoc = new RollBackDocument(Microsoft.Dynamics.GP.eConnect.TransactionType.PM, null, strNextNumber);
                        //                getDocNumbers.RollBackDocumentNumber(RollbackDoc, gpCmpDBConnStr);
                        //                strNextNumber = "";
                        //            }
                        //        }
                        //    }
                        //    bAutoGenerateNum = false;
                        //}

                    }
                    finally
                    {
                    }
                }



            }
            catch (Exception ex)
            {

                EventLogger.WriteToEventLog(("Error: " + ex.Message.ToString()), EventLogEntryType.Error);
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog(("Error: " + ex.Message.ToString()), filePath, EventLogEntryType.Error);
                }
                bReturnValue = false;

                
                

            }
            finally
            {

            }
            return bReturnValue;

        }





        private static string strValue(DataRow drMappingSetup, DataRow drSourceFileData)
        {
            string sValue = "";
            switch (drMappingSetup["Value Type"].ToString())
            {

                case "Source":
                    sValue = drSourceFileData[drMappingSetup["ValueorColumn"].ToString().Trim()].ToString().Trim().Replace("\"", "");
                    break;
                case "Constant":
                    sValue = drMappingSetup["ValueorColumn"].ToString().Trim().Replace("\"", "");
                    break;
            }
            return sValue;
        }

        private static Decimal dcValue(DataRow drMappingSetup, DataRow drSourceFileData)
        {

            Decimal dValue = 0;
            string sValue = "";
            switch (drMappingSetup["Value Type"].ToString())
            {

                case "Source":
                    sValue = drSourceFileData[drMappingSetup["ValueorColumn"].ToString().Trim()].ToString().Trim().Replace("\"", "");
                    if (sValue != "")
                    {
                        dValue = Convert.ToDecimal(sValue);
                    }

                    break;
                case "Constant":
                    dValue = Convert.ToDecimal(drMappingSetup["ValueorColumn"].ToString().Trim().Replace("\"", ""));
                    break;
            }
            return dValue;

        }


        private static DateTime dtValue(DataRow drMappingSetup, DataRow drSourceFileData)
        {
            DateTime dtValue = Convert.ToDateTime("1900-01-01");
            string sValue = "";
            switch (drMappingSetup["Value Type"].ToString())
            {

                case "Source":
                    sValue = drSourceFileData[drMappingSetup["ValueorColumn"].ToString().Trim()].ToString().Trim().Replace("\"", "");
                    if (sValue != "")
                    {
                        dtValue = Convert.ToDateTime(sValue);
                    }

                    break;
                case "Constant":
                    dtValue = Convert.ToDateTime(drMappingSetup["ValueorColumn"].ToString().Trim().Replace("\"", ""));
                    break;
            }
            return dtValue;
        }

        private static short shValue(DataRow drMappingSetup, DataRow drSourceFileData)
        {
            short shValue = 0;

            string sValue = "";
            switch (drMappingSetup["Value Type"].ToString())
            {

                case "Source":
                    sValue = drSourceFileData[drMappingSetup["ValueorColumn"].ToString().Trim()].ToString().Trim().Replace("\"", "");
                    if (sValue != "")
                    {
                        shValue = Convert.ToInt16(sValue);
                    }

                    break;
                case "Constant":
                    shValue = Convert.ToInt16(drMappingSetup["ValueorColumn"].ToString().Trim().Replace("\"", ""));
                    break;
            }
            return shValue;
        }

        private static Boolean bValue(DataRow drMappingSetup, DataRow drSourceFileData)
        {
            Boolean bValue = false;
            string sValue = "";
            switch (drMappingSetup["Value Type"].ToString())
            {

                case "Source":
                    sValue = drSourceFileData[drMappingSetup["ValueorColumn"].ToString().Trim()].ToString().Trim().Replace("\"", "");
                    if (sValue != "")
                    {
                        bValue = Convert.ToBoolean(sValue);
                    }
                    break;
                case "Constant":
                    bValue = Convert.ToBoolean(drMappingSetup["ValueorColumn"].ToString().Trim().Replace("\"", ""));
                    break;
            }
            return bValue;
        }


        private static string strAccountNumber(DataRow drMappingSetup, DataRow drSourceFileData, string gpCompDBConnString)
        {
            string sValue = "";



            switch (drMappingSetup["Value Type"].ToString())
            {

                case "Source":
                    sValue = drSourceFileData[drMappingSetup["ValueorColumn"].ToString().Trim()].ToString().Trim().Replace("\"", "");
                    break;
                case "Constant":
                    sValue = drMappingSetup["ValueorColumn"].ToString().Trim().Replace("\"", "");
                    break;



            }


            sValue = DBLibrary.SQLGetFormattedAccountString("sp_Account_Format", gpCompDBConnString, sValue);

            return sValue;
        }

         private static string strAutoValue(DataRow drMappingSetup, DataRow drSourceFileData,string gpCompDBConnString)
        {
             string sValue = "";
            switch (drMappingSetup["Value Type"].ToString())
            {
                    
                case "Source":
                    sValue = drSourceFileData[drMappingSetup["ValueorColumn"].ToString().Trim()].ToString().Trim().Replace("\"", "");
                    break;
                case "Constant":
                    sValue = drMappingSetup["ValueorColumn"].ToString().Trim().Replace("\"", "");
                    break;
                case "GPAutoNumber":
                   // Microsoft.Dynamics.GP.eConnect.GetNextDocNumbers getNextNumber = new Microsoft.Dynamics.GP.eConnect.GetNextDocNumbers();

                    sValue = DBLibrary.SQLGetNextPaymentNumber("taGetPMNextPaymentNumber", gpCompDBConnString);
                   //sValue = getNextNumber.GetPMNextVoucherNumber(Microsoft.Dynamics.GP.eConnect.IncrementDecrement.Increment, gpCompDBConnString);
                    
                    strNextNumber = sValue;
                    bAutoGenerateNum = true;
                    break;
            }
            return sValue ;
        }
    }
}
