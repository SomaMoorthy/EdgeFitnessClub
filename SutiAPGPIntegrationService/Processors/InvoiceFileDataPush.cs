using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Xml;
using System.Xml.Serialization;
using System.Globalization;
using Microsoft.Dynamics.GP.eConnect;
using Microsoft.Dynamics.GP.eConnect.Serialization;
using SUTIAPGPIntegrationService.Utilities;
using System.Diagnostics;
using System.Configuration;



namespace SUTIAPGPIntegrationService.Processors
{
    class InvoiceFileDataPush
    {
        public static Boolean bAutoGenerateNum = false;
        public static string strNextNumber = "";
        public static string strExceptionMessage = "";
        public static Boolean InvoiceFileDataProcessing(DataTable dtSourceFileDataSource, DataSet dsMappingSetup, DataSet dsIntegrationDistSetup, DataSet dsIntegrationTaxSetup,DataRow drIntegrationSetup, string[] strSourceColumnNames, string gpCmpDBConnStr, string strCompanyName, string strInvoiceMapID, Boolean INVFORMATACCOUNTSTRING)
        {
            strExceptionMessage = "";
            Boolean bReturnValue = true;

            try
            {
                
                DateTimeFormatInfo dateFormat = new CultureInfo("en-US").DateTimeFormat;
                eConnectMethods eConCall = new eConnectMethods();
                eConnectType eConnect = new eConnectType();

                DataView dvDistinctValues = new DataView(dtSourceFileDataSource);

                DataTable dtSourceFileData = new DataTable();

                dtSourceFileData = dvDistinctValues.ToTable(true, strSourceColumnNames);

            
                EventLogger.WriteToEventLog("Warning: Invoice file processing has been processed - Start.");
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog("Warning: Invoice file processing has been processed - Start.", filePath, EventLogEntryType.Error);
                }
                int iSourceDataCount, iMappingSetup;
                for (iSourceDataCount = 0; iSourceDataCount < dtSourceFileData.Rows.Count; iSourceDataCount++)
                {
                    try
                    {
                        Boolean bTaxRequired = false;
                        PMTransactionType pmTransactiontype = new PMTransactionType();
                        taPMTransactionInsert pmTransaction = new taPMTransactionInsert();
                        for (iMappingSetup = 0; iMappingSetup < dsMappingSetup.Tables["InvoiceHeader"].Rows.Count; iMappingSetup++)
                        {



                            switch (dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup]["eConnect Field"].ToString())
                            {
                                case "BACHNUMB":

                                    pmTransaction.BACHNUMB = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "VCHNUMWK":

                                    pmTransaction.VCHNUMWK = strAutoValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount],gpCmpDBConnStr);
                                    break;

                                case "VENDORID":
                                    pmTransaction.VENDORID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DOCNUMBR":
                                    pmTransaction.DOCNUMBR = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DOCTYPE":
                                    pmTransaction.DOCTYPE = shValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DOCAMNT":
                                    pmTransaction.DOCAMNT = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DOCDATE":
                                    pmTransaction.DOCDATE = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "PSTGDATE":
                                    pmTransaction.PSTGDATE = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "VADCDTRO":
                                    pmTransaction.VADCDTRO = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "VADDCDPR":
                                    pmTransaction.VADDCDPR = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PYMTRMID":
                                    pmTransaction.PYMTRMID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TAXSCHID":
                                    pmTransaction.TAXSCHID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DUEDATE":
                                    pmTransaction.DUEDATE = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "DSCDLRAM":
                                    pmTransaction.DSCDLRAM = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    pmTransaction.DSCDLRAMSpecified = true;
                                    break;

                                case "DISCDATE":
                                    pmTransaction.DISCDATE = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "PRCHAMNT":
                                    pmTransaction.PRCHAMNT = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CHRGAMNT":
                                    pmTransaction.CHRGAMNT = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);

                                    break;

                                case "CASHAMNT":
                                    pmTransaction.CASHAMNT = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CAMCBKID":
                                    pmTransaction.CAMCBKID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CDOCNMBR":
                                    pmTransaction.CDOCNMBR = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CAMTDATE":
                                    pmTransaction.CAMTDATE = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "CAMPMTNM":
                                    pmTransaction.CAMPMTNM = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CHEKAMNT":
                                    pmTransaction.CHEKAMNT = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CHAMCBID":
                                    pmTransaction.CHAMCBID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CHEKDATE":
                                    pmTransaction.CHEKDATE = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "CAMPYNBR":
                                    pmTransaction.CAMPYNBR = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CRCRDAMT":
                                    pmTransaction.CRCRDAMT = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CCAMPYNM":
                                    pmTransaction.CCAMPYNM = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CHEKNMBR":
                                    pmTransaction.CHEKNMBR = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CARDNAME":
                                    pmTransaction.CARDNAME = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CCRCTNUM":
                                    pmTransaction.CCRCTNUM = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CRCARDDT":
                                    pmTransaction.CRCARDDT = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "CHEKBKID":
                                    pmTransaction.CHEKBKID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TRXDSCRN":
                                    pmTransaction.TRXDSCRN = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TRDISAMT":
                                    pmTransaction.TRDISAMT = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TAXAMNT":
                                    pmTransaction.TAXAMNT = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (pmTransaction.TAXAMNT > 0)
                                    {
                                        bTaxRequired = true;
                                    }
                                    break;

                                case "FRTAMNT":
                                    pmTransaction.FRTAMNT = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TEN99AMNT":
                                    pmTransaction.TEN99AMNT = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "MSCCHAMT":
                                    pmTransaction.MSCCHAMT = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PORDNMBR":
                                    pmTransaction.PORDNMBR = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "SHIPMTHD":
                                    pmTransaction.SHIPMTHD = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DISAMTAV":
                                    pmTransaction.DISAMTAV = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    pmTransaction.DISAMTAVSpecified = true;

                                    break;

                                case "DISTKNAM":
                                    pmTransaction.DISTKNAM = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "APDSTKAM":
                                    pmTransaction.APDSTKAM = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "MDFUSRID":
                                    pmTransaction.MDFUSRID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "POSTEDDT":
                                    pmTransaction.POSTEDDT = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "PTDUSRID":
                                    pmTransaction.PTDUSRID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PCHSCHID":
                                    pmTransaction.PCHSCHID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "FRTSCHID":
                                    pmTransaction.FRTSCHID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "MSCSCHID":
                                    pmTransaction.MSCSCHID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PRCTDISC":
                                    pmTransaction.PRCTDISC = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    pmTransaction.PRCTDISCSpecified = true;
                                    break;

                                case "Tax_Date":
                                    pmTransaction.Tax_Date = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "CURNCYID":
                                    pmTransaction.CURNCYID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "XCHGRATE":
                                    pmTransaction.XCHGRATE = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RATETPID":
                                    pmTransaction.RATETPID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "EXPNDATE":
                                    pmTransaction.EXPNDATE = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "EXCHDATE":
                                    pmTransaction.EXCHDATE = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "EXGTBDSC":
                                    pmTransaction.EXGTBDSC = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "EXTBLSRC":
                                    pmTransaction.EXTBLSRC = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RATEEXPR":
                                    pmTransaction.RATEEXPR = shValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DYSTINCR":
                                    pmTransaction.DYSTINCR = shValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RATEVARC":
                                    pmTransaction.RATEVARC = dcValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TRXDTDEF":
                                    pmTransaction.TRXDTDEF = shValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RTCLCMTD":
                                    pmTransaction.RTCLCMTD = shValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PRVDSLMT":
                                    pmTransaction.PRVDSLMT = shValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DATELMTS":
                                    pmTransaction.DATELMTS = shValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TIME1":
                                    pmTransaction.TIME1 = dtValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd HH:mm:ss");
                                    break;

                                case "BatchCHEKBKID":
                                    pmTransaction.BatchCHEKBKID = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CREATEDIST":
                                    pmTransaction.CREATEDIST = shValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RequesterTrx":
                                    pmTransaction.RequesterTrx = shValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND1":
                                    pmTransaction.USRDEFND1 = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND2":
                                    pmTransaction.USRDEFND2 = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND3":
                                    pmTransaction.USRDEFND3 = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND4":
                                    pmTransaction.USRDEFND4 = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND5":
                                    pmTransaction.USRDEFND5 = strValue(dsMappingSetup.Tables["InvoiceHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;




                            }



                        }


                        EventLogger.WriteToEventLog("Warning: Invoice file processing has been processed - Distribution.");
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog("Warning: Invoice file processing has been processed - Distribution.", filePath, EventLogEntryType.Error);
                        }

                        DataTable dtSourceFileDataDist = dtSourceFileDataSource.Select("INV_NUM = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["INV_NUM"]) + "' AND VENDOR = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VENDOR"]) + "'", "INV_NUM").CopyToDataTable();
                        
                        taPMDistribution_ItemsTaPMDistribution[] PMDistributionvaluesArr = new taPMDistribution_ItemsTaPMDistribution[dtSourceFileDataDist.Rows.Count];


                        for (int iSourceDataDistCount = 0; iSourceDataDistCount < dtSourceFileDataDist.Rows.Count; iSourceDataDistCount++)
                        {
                            taPMDistribution_ItemsTaPMDistribution PMDistributionvalues = new taPMDistribution_ItemsTaPMDistribution();

                            //string strInvoiceMapID =  dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[0]["INVOICEMAPID"].ToString().Trim();
                            //string strCompanyName = dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[0]["COMPANYID"].ToString().Trim();

                            for (iMappingSetup = 0; iMappingSetup < dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows.Count; iMappingSetup++)
                            {

                                switch (dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup]["eConnect Field"].ToString())
                                {
                                    case "VCHRNMBR":

                                        //PMDistributionvalues.VCHRNMBR = strValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        PMDistributionvalues.VCHRNMBR = pmTransaction.VCHNUMWK;
                                        break;

                                    case "VENDORID":
                                        PMDistributionvalues.VENDORID = strValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "DOCTYPE":
                                        PMDistributionvalues.DOCTYPE = shValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "DSTSQNUM":
                                        PMDistributionvalues.DSTSQNUM = shValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "DISTTYPE":
                                        PMDistributionvalues.DISTTYPE = shValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "DistRef":
                                        PMDistributionvalues.DistRef = strValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "ACTINDX":
                                        PMDistributionvalues.ACTINDX = shValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "ACTNUMST":

                                        if (INVFORMATACCOUNTSTRING == true)
                                        {
                                            PMDistributionvalues.ACTNUMST = strValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        }
                                        else
                                        {
                                            PMDistributionvalues.ACTNUMST = strAccountNumber(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount],gpCmpDBConnStr);
                                        }
                                        break;

                                    case "DEBITAMT":

                                        decimal dcTempDRValue = dcValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);

                                        if (dsIntegrationDistSetup.Tables["InvoiceDistribution"].Select("[eConnect Field] = 'SEPERATECREDIT' and [ValueorColumn] = 0").Count() == 1)
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


                                        decimal dcTempCRValue = dcValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);

                                        if (dsIntegrationDistSetup.Tables["InvoiceDistribution"].Select("[eConnect Field] = 'SEPERATECREDIT' and [ValueorColumn] = 0").Count() == 1)
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
                                        PMDistributionvalues.RequesterTrx = shValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "USRDEFND1":

                                        PMDistributionvalues.USRDEFND1 = strValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "USRDEFND2":
                                        PMDistributionvalues.USRDEFND2 = strValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "USRDEFND3":
                                        PMDistributionvalues.USRDEFND3 = strValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "USRDEFND4":
                                        PMDistributionvalues.USRDEFND4 = strValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;

                                    case "USRDEFND5":
                                        PMDistributionvalues.USRDEFND5 = strValue(dsIntegrationDistSetup.Tables["InvoiceDistribution"].Rows[iMappingSetup], dtSourceFileDataDist.Rows[iSourceDataDistCount]);
                                        break;
                                }
                            }



                            if (PMDistributionvalues.USRDEFND1 != "")
                            {
                                if (PMDistributionvalues.USRDEFND1 != strCompanyName)
                                {
                                    PMDistributionvalues.ACTNUMST = DBLibrary.GetScalarValue(SQLLibrary.SQLIntercompanyTempAccount(strInvoiceMapID, strCompanyName), Convert.ToString(ConfigurationManager.AppSettings["SUTIAPGPServiceCon"])).Trim();
                                    if (PMDistributionvalues.ACTINDX != 0)
                                    {
                                        PMDistributionvalues.ACTINDX = Convert.ToInt16(DBLibrary.GetScalarValue(PMDistributionvalues.ACTNUMST, gpCmpDBConnStr).Trim());

                                    }


                                }
                                else
                                {
                                    PMDistributionvalues.USRDEFND1 = "";
                                    PMDistributionvalues.USRDEFND2 = "";
                                }
                            }
                            else
                            {
                                PMDistributionvalues.USRDEFND1 = "";
                                PMDistributionvalues.USRDEFND2 = "";
                            }

                            PMDistributionvaluesArr[iSourceDataDistCount] = PMDistributionvalues;

                        }

                        if (bTaxRequired)
                        {
                            DataTable dtSourceFileDataTax = dtSourceFileDataSource.Select("INV_NUM = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["INV_NUM"]) + "' AND VENDOR = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VENDOR"]) + "'", "INV_NUM").CopyToDataTable();

                            taPMTransactionTaxInsert_ItemsTaPMTransactionTaxInsert PMTaxvlaues = new taPMTransactionTaxInsert_ItemsTaPMTransactionTaxInsert();
                            taPMTransactionTaxInsert_ItemsTaPMTransactionTaxInsert[] PMTaxvlauesArr = new taPMTransactionTaxInsert_ItemsTaPMTransactionTaxInsert[dtSourceFileDataTax.Rows.Count];

                            for (int iSourceDataTaxCount = 0; iSourceDataTaxCount < dtSourceFileDataTax.Rows.Count; iSourceDataTaxCount++)
                            {
                                for (iMappingSetup = 0; iMappingSetup < dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows.Count; iMappingSetup++)
                                {
                                    switch (dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup]["eConnect Field"].ToString())
                                    {


                                        case "VCHRNMBR":

                                            //PMTaxvlaues.VCHRNMBR = strValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                           PMTaxvlaues.VCHRNMBR = pmTransaction.VCHNUMWK;
                                            break;

                                        case "VENDORID":
                                            PMTaxvlaues.VENDORID = strValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "DOCTYPE":
                                            PMTaxvlaues.DOCTYPE = shValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "BACHNUMB":
                                            PMTaxvlaues.BACHNUMB = strValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "TAXDTLID":
                                            PMTaxvlaues.TAXDTLID = strValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "TAXAMNT":
                                            PMTaxvlaues.TAXAMNT = dcValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;


                                        case "PCTAXAMT":
                                            PMTaxvlaues.TAXAMNT = dcValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "FRTTXAMT":
                                            PMTaxvlaues.FRTTXAMT = dcValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "MSCTXAMT":
                                            PMTaxvlaues.MSCTXAMT = dcValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "TDTTXPUR":
                                            PMTaxvlaues.TDTTXPUR = dcValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "TXDTTPUR":
                                            PMTaxvlaues.TXDTTPUR = dcValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "ACTINDX":
                                            PMTaxvlaues.ACTINDX = shValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "ACTNUMST":
                                            PMTaxvlaues.ACTNUMST = strValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "RequesterTrx":
                                            PMTaxvlaues.RequesterTrx = shValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "USRDEFND1":
                                            PMTaxvlaues.USRDEFND1 = strValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "USRDEFND2":
                                            PMTaxvlaues.USRDEFND2 = strValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "USRDEFND3":
                                            PMTaxvlaues.USRDEFND3 = strValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "USRDEFND4":
                                            PMTaxvlaues.USRDEFND4 = strValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "USRDEFND5":
                                            PMTaxvlaues.USRDEFND5 = strValue(dsIntegrationTaxSetup.Tables["InvoiceTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                    }
                                }

                                PMTaxvlauesArr[iSourceDataTaxCount] = PMTaxvlaues;

                            }


                            pmTransactiontype.taPMTransactionTaxInsert_Items = PMTaxvlauesArr;
                            PMTaxvlauesArr = null;


                        }
                        //Array.Resize<taRMDistribution_ItemsTaRMDistribution>(ref PMDistributionvaluesArr, rowCount);
                        pmTransactiontype.taPMTransactionInsert = pmTransaction;

                        pmTransactiontype.taPMDistribution_Items = PMDistributionvaluesArr;



                        PMDistributionvaluesArr = null;

                        PMTransactionType[] myPMtrx = { pmTransactiontype };
                        eConnect.PMTransactionType = myPMtrx;

                        FileStream fs = new FileStream("PayableTransaction.xml", FileMode.Create);


                        XmlTextWriter writer = new XmlTextWriter(fs, new UTF8Encoding());
                        XmlSerializer serializer = new XmlSerializer(eConnect.GetType());
                        serializer.Serialize(writer, eConnect);

                        writer.Close();


                        XmlDocument xmldoc = new XmlDocument();
                        xmldoc.Load("PayableTransaction.xml");
                        string PayableDocument = xmldoc.OuterXml;


                        EventLogger.WriteToEventLog("Warning: Invoice file processing has been processed - Final.");
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog("Warning: Invoice file processing has been processed - Final.", filePath, EventLogEntryType.Error);
                        }

                        eConCall.CreateTransactionEntity(gpCmpDBConnStr, PayableDocument);
                        pmTransactiontype = null;

                    }
                    catch (Exception ex)
                    {

                        EventLogger.WriteToEventLog(("Error:  " + ex.Message.ToString()), EventLogEntryType.Error);
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
                        if (bAutoGenerateNum)
                        {
                            if (strNextNumber != "")
                            {
                                using (GetNextDocNumbers getDocNumbers = new GetNextDocNumbers())
                                {
                                    using (DocumentRollback docRollBack = new DocumentRollback())
                                    {

                                        RollBackDocument RollbackDoc = new RollBackDocument(Microsoft.Dynamics.GP.eConnect.TransactionType.PM, null, strNextNumber);
                                        getDocNumbers.RollBackDocumentNumber(RollbackDoc, gpCmpDBConnStr);
                                        strNextNumber = "";
                                    }
                                }
                            }
                            bAutoGenerateNum = false;
                        }


                    }
                    finally
                    {
                    }
                }
            
                
        
            }
            catch (Exception ex)
            {

                EventLogger.WriteToEventLog(("Warning: No Data found in the file " + ex.Message.ToString()), EventLogEntryType.Error);
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog(("Warning: No Data found in the file " + ex.Message.ToString()), filePath, EventLogEntryType.Error);
                }
                bReturnValue = false;
            }
            finally
            {
                
            }
            return bReturnValue;

        }


        private static string strAccountNumber(DataRow drMappingSetup, DataRow drSourceFileData,string gpCompDBConnString)
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
                    Microsoft.Dynamics.GP.eConnect.GetNextDocNumbers getNextNumber = new Microsoft.Dynamics.GP.eConnect.GetNextDocNumbers();
                    sValue = getNextNumber.GetPMNextVoucherNumber(Microsoft.Dynamics.GP.eConnect.IncrementDecrement.Increment, gpCompDBConnString);
                    strNextNumber = sValue;
                    bAutoGenerateNum = true;
                    break;
            }
            return sValue ;
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
            return sValue ;
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



    }
}
