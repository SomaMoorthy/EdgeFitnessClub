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
    class RecevingTransactionFileDataPush
    {
        public static Boolean bAutoGenerateNum = false;
        public static string strNextNumber = "";
        public static string strExceptionMessage = "";
        public static Boolean RecevingTransactionFileProcessing(DataTable dtSourceFileDataSource, DataSet dsIntegrationRECVHdrSetup, DataSet dsIntegrationRECVLineSetup, DataSet dsIntegrationRECVTaxSetup, DataSet dsIntegrationRECVDistSetup, DataRow drIntegrationSetup, string[] strSourceColumnNames, string gpCmpDBConnStr, string strCompanyName, string strInvoiceMapID, Boolean POPFORMATACCOUNTSTRING)
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


                EventLogger.WriteToEventLog("Warning: Receving Transaction Header file processing has been processed - Start.");
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog("Warning: Receving Transaction Header file processing has been processed - Start.", filePath, EventLogEntryType.Error);
                }

                int iSourceDataCount, iMappingSetup;
                

                for (iSourceDataCount = 0; iSourceDataCount < dtSourceFileData.Rows.Count; iSourceDataCount++)
                {
                    
                    try 
                    {
                        string MiscAccount = "";
                        string TradeAccount = "";
                        string APAccount = "";
                        string FreightAccount = "";
                        string MiscAccountIndx = "";
                        string TradeAccountIndx = "";
                        string APAccountIndx = "";
                        string FreightAccountIndx = "";

                        Boolean bTaxRequired = false;
                        Boolean bDistRequired = false;
                        Boolean bUpdateDistRequired = false;
                        POPReceivingsType recvTransactiontype = new POPReceivingsType();
                        taPopRcptHdrInsert recvHdrTrans = new taPopRcptHdrInsert();
                        for (iMappingSetup = 0; iMappingSetup < dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows.Count; iMappingSetup++)
                        {

                            switch (dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup]["eConnect Field"].ToString())
                            {
                                case "POPRCTNM":

                                    recvHdrTrans.POPRCTNM = strAutoValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount], gpCmpDBConnStr);
                                    break;
                                case "POPTYPE":

                                    recvHdrTrans.POPTYPE = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "VNDDOCNM":

                                    recvHdrTrans.VNDDOCNM = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "receiptdate":

                                    recvHdrTrans.receiptdate = dtValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "ACTLSHIP":

                                    recvHdrTrans.ACTLSHIP = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "BACHNUMB":

                                    recvHdrTrans.BACHNUMB = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "VENDORID":

                                    recvHdrTrans.VENDORID = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "VENDNAME":

                                    recvHdrTrans.VENDNAME = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "SUBTOTAL":

                                    recvHdrTrans.SUBTOTAL = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (recvHdrTrans.SUBTOTAL > 0)
                                    {
                                        recvHdrTrans.SUBTOTALSpecified = true;
                                    }
                                    break;
                                case "TRDISAMT":

                                    recvHdrTrans.TRDISAMT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (recvHdrTrans.TRDISAMT > 0)
                                    {
                                        recvHdrTrans.TRDISAMTSpecified = true;
                                    }
                                    break;
                                case "FRTAMNT":

                                    recvHdrTrans.FRTAMNT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "MISCAMNT":

                                    recvHdrTrans.MISCAMNT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "TAXAMNT":

                                    recvHdrTrans.TAXAMNT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "TEN99AMNT":

                                    recvHdrTrans.TEN99AMNT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "PYMTRMID":

                                    recvHdrTrans.PYMTRMID = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "DSCPCTAM":

                                    recvHdrTrans.DSCPCTAM = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (recvHdrTrans.DSCPCTAM > 0)
                                    {
                                        recvHdrTrans.DSCPCTAMSpecified = true;
                                    }
                                    break;
                                case "DSCDLRAM":

                                    recvHdrTrans.DSCDLRAM = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (recvHdrTrans.DSCDLRAM > 0)
                                    {
                                        recvHdrTrans.DSCDLRAMSpecified = true;
                                    }
                                    break;
                                case "DISAVAMT":

                                    recvHdrTrans.DISAVAMT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (recvHdrTrans.DISAVAMT > 0)
                                    {
                                        recvHdrTrans.DISAVAMTSpecified = true;
                                    }
                                    break;
                                case "REFRENCE":

                                    recvHdrTrans.REFRENCE = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USER2ENT":

                                    recvHdrTrans.USER2ENT = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "VCHRNMBR":

                                    recvHdrTrans.VCHRNMBR = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "Tax_Date":

                                    recvHdrTrans.Tax_Date = dtValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "TIME1":

                                    recvHdrTrans.TIME1 = dtValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "WITHHAMT":

                                    recvHdrTrans.WITHHAMT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "TXRGNNUM":

                                    recvHdrTrans.TXRGNNUM = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "AUTOCOST":

                                    recvHdrTrans.AUTOCOST = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "TAXSCHID":

                                    recvHdrTrans.TAXSCHID = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "Purchase_Freight_Taxable":

                                    recvHdrTrans.Purchase_Freight_Taxable = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "Purchase_Misc_Taxable":

                                    recvHdrTrans.Purchase_Misc_Taxable = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "FRTSCHID":

                                    recvHdrTrans.FRTSCHID = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "MSCSCHID":

                                    recvHdrTrans.MSCSCHID = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "FRTTXAMT":

                                    recvHdrTrans.FRTTXAMT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "MSCTXAMT":

                                    recvHdrTrans.MSCTXAMT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "BCKTXAMT":

                                    recvHdrTrans.BCKTXAMT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "BackoutTradeDiscTax":

                                    recvHdrTrans.BackoutTradeDiscTax = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "SHIPMTHD":

                                    recvHdrTrans.SHIPMTHD = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USINGHEADERLEVELTAXES":

                                    recvHdrTrans.USINGHEADERLEVELTAXES = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CREATEDIST":
                                    //Do not create distribution(Always set constant CREATEDIST = 1 in database)  use the Auto Create distribution and pass the Inventory Account to each line items. And update AP account after distribution Created if AP account is passed from source file
                                    recvHdrTrans.CREATEDIST = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (recvHdrTrans.CREATEDIST == 0)
                                    {
                                        bDistRequired = true;
                                    }
                                    

                                    break;
                                case "CURNCYID":

                                    recvHdrTrans.CURNCYID = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "XCHGRATE":

                                    recvHdrTrans.XCHGRATE = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "RATETPID":

                                    recvHdrTrans.RATETPID = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "EXPNDATE":

                                    recvHdrTrans.EXPNDATE = dtValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "EXCHDATE":

                                    recvHdrTrans.EXCHDATE = dtValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "EXGTBDSC":

                                    recvHdrTrans.EXGTBDSC = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "EXTBLSRC":

                                    recvHdrTrans.EXTBLSRC = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "RATEEXPR":

                                    recvHdrTrans.RATEEXPR = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "DYSTINCR":

                                    recvHdrTrans.DYSTINCR = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "RATEVARC":

                                    recvHdrTrans.RATEVARC = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "TRXDTDEF":

                                    recvHdrTrans.TRXDTDEF = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "RTCLCMTD":

                                    recvHdrTrans.RTCLCMTD = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "PRVDSLMT":

                                    recvHdrTrans.PRVDSLMT = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "DATELMTS":

                                    recvHdrTrans.DATELMTS = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "DUEDATE":

                                    recvHdrTrans.DUEDATE = dtValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "DISCDATE":

                                    recvHdrTrans.DISCDATE = dtValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "NOTETEXT":

                                    recvHdrTrans.NOTETEXT = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "VADCDTRO":

                                    recvHdrTrans.VADCDTRO = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CASHAMNT":

                                    recvHdrTrans.CASHAMNT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CAMCBKID":

                                    recvHdrTrans.CAMCBKID = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CDOCNMBR":

                                    recvHdrTrans.CDOCNMBR = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CAMTDATE":

                                    recvHdrTrans.CAMTDATE = dtValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "CAMPMTNM":

                                    recvHdrTrans.CAMPMTNM = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CHEKAMNT":

                                    recvHdrTrans.CHEKAMNT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CHAMCBID":

                                    recvHdrTrans.CHAMCBID = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CHEKNMBR":

                                    recvHdrTrans.CHEKNMBR = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CHEKDATE":

                                    recvHdrTrans.CHEKDATE = dtValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "CAMPYNBR":

                                    recvHdrTrans.CAMPYNBR = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CRCRDAMT":

                                    recvHdrTrans.CRCRDAMT = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CARDNAME":

                                    recvHdrTrans.CARDNAME = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CCRCTNUM":

                                    recvHdrTrans.CCRCTNUM = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CRCARDDT":

                                    recvHdrTrans.CRCARDDT = dtValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "CCAMPYNM":

                                    recvHdrTrans.CCAMPYNM = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "DISTKNAM":

                                    recvHdrTrans.DISTKNAM = dcValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "RequesterTrx":

                                    recvHdrTrans.RequesterTrx = shValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USRDEFND1":

                                    recvHdrTrans.USRDEFND1 = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USRDEFND2":

                                    recvHdrTrans.USRDEFND2 = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USRDEFND3":

                                    recvHdrTrans.USRDEFND3 = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USRDEFND4":

                                    recvHdrTrans.USRDEFND4 = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USRDEFND5":

                                    recvHdrTrans.USRDEFND5 = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "APAccount":
                                    
                                    if (POPFORMATACCOUNTSTRING == true)
                                    {
                                        APAccount = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    }
                                    else
                                    {
                                        APAccount = strAccountNumber(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount], gpCmpDBConnStr);
                                    }
                                    if (APAccount != "")
                                    {
                                        APAccountIndx = DBLibrary.SQLValidateAccountString("sp_Validate_Account_Number", gpCmpDBConnStr, APAccount);
                                        bUpdateDistRequired = true;
                                    }

                                    break;
                                case "MiscAccount":

                                    if (POPFORMATACCOUNTSTRING == true)
                                    {
                                        MiscAccount = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    }
                                    else
                                    {
                                        MiscAccount = strAccountNumber(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount], gpCmpDBConnStr);
                                    }
                                    if (MiscAccount != "")
                                    {
                                        MiscAccountIndx = DBLibrary.SQLValidateAccountString("sp_Validate_Account_Number", gpCmpDBConnStr, MiscAccount);
                                        bUpdateDistRequired = true;
                                    }

                                    break;
                                case "FreigtAccount":

                                    if (POPFORMATACCOUNTSTRING == true)
                                    {
                                        FreightAccount = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    }
                                    else
                                    {
                                        FreightAccount = strAccountNumber(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount], gpCmpDBConnStr);
                                    }
                                    if (FreightAccount != "")
                                    {
                                        FreightAccountIndx = DBLibrary.SQLValidateAccountString("sp_Validate_Account_Number", gpCmpDBConnStr, FreightAccount);
                                        bUpdateDistRequired = true;
                                    }

                                    break;
                                case "TradeDiscAccount":

                                    if (POPFORMATACCOUNTSTRING == true)
                                    {
                                        TradeAccount = strValue(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    }
                                    else
                                    {
                                        TradeAccount = strAccountNumber(dsIntegrationRECVHdrSetup.Tables["RecvTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount], gpCmpDBConnStr);
                                    }
                                    if (TradeAccount != "")
                                    {
                                        TradeAccountIndx = DBLibrary.SQLValidateAccountString("sp_Validate_Account_Number", gpCmpDBConnStr, TradeAccount);
                                        bUpdateDistRequired = true;
                                    }

                                    break;

                            }



                        }

                        //if (bUpdateDistRequired)
                        //{
                        //    if (recvHdrTrans.MISCAMNT > 0 && MiscAccountIndx == 0 && MiscAccount != "")
                        //    {
                        //        throw new Exception ("Misc Acccount Number " + MiscAccount + " does not exists.");
                        //    }
                        //    if (recvHdrTrans.FRTAMNT > 0 && FreightAccountIndx == 0 && FreightAccount != "")
                        //    {
                        //        throw new Exception("Freight Acccount Number " + FreightAccount + " does not exists.");
                        //    }
                        //    if (recvHdrTrans.TRDISAMT > 0 && TradeAccountIndx == 0 && TradeAccount != "")
                        //    {
                        //        throw new Exception("Trade Discount Acccount Number " + TradeAccount + " does not exists.");
                        //    }
                        //    if (APAccount != "" && APAccountIndx == 0)
                        //    {
                        //        throw new Exception("AP Acccount Number " + APAccount + " does not exists.");
                        //    }
                        //}


                        EventLogger.WriteToEventLog("Warning: Receving Transaction Header processing has been processed - POP Line.");

                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog("Warning: Receving Transaction Header processing has been processed - POP Line.", filePath, EventLogEntryType.Warning);
                        }

                        DataTable dtSourceFileDataRECVLine = dtSourceFileDataSource.Select("VNDDOCNM = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VNDDOCNM"]) + "' AND VENDORID = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VENDORID"]) + "'", "VNDDOCNM").CopyToDataTable();
                        
                       
                        taPopRcptLineInsert_ItemsTaPopRcptLineInsert[] RECVLineItemsArr = new taPopRcptLineInsert_ItemsTaPopRcptLineInsert[dtSourceFileDataRECVLine.Rows.Count];

                        for (int iSourceDataRECVLineCount = 0; iSourceDataRECVLineCount < dtSourceFileDataRECVLine.Rows.Count; iSourceDataRECVLineCount++)
                        {
                            //taPMDistribution_ItemsTaPMDistribution PMDistributionvalues = new taPMDistribution_ItemsTaPMDistribution();

                            taPopRcptLineInsert_ItemsTaPopRcptLineInsert RECVLineItemvalues = new taPopRcptLineInsert_ItemsTaPopRcptLineInsert();

 
                            for (iMappingSetup = 0; iMappingSetup < dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows.Count; iMappingSetup++)
                            {

                                switch (dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup]["eConnect Field"].ToString())
                                {
                                    case "POPTYPE":

                                        RECVLineItemvalues.POPTYPE = shValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "POPRCTNM":

                                        RECVLineItemvalues.POPRCTNM = recvHdrTrans.POPRCTNM;
                                        break;
                                    case "PONUMBER":

                                        RECVLineItemvalues.PONUMBER = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "ITEMNMBR":

                                        RECVLineItemvalues.ITEMNMBR = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "ITEMDESC":

                                        RECVLineItemvalues.ITEMDESC = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "VENDORID":

                                        RECVLineItemvalues.VENDORID = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "RCPTLNNM":

                                        RECVLineItemvalues.RCPTLNNM = longIntValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "VNDITNUM":

                                        RECVLineItemvalues.VNDITNUM = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "VNDITDSC":

                                        RECVLineItemvalues.VNDITDSC = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "ACTLSHIP":

                                        RECVLineItemvalues.ACTLSHIP = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "INVINDX":
                                        
                                        RECVLineItemvalues.INVINDX = shValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "InventoryAccount":
                                        if (POPFORMATACCOUNTSTRING == true)
                                        {
                                            RECVLineItemvalues.InventoryAccount = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        }
                                        else
                                        {
                                            RECVLineItemvalues.InventoryAccount = strAccountNumber(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount], gpCmpDBConnStr);
                                        }
                                        break;

                                       
                                    case "UOFM":

                                        RECVLineItemvalues.UOFM = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "UNITCOST":

                                        RECVLineItemvalues.UNITCOST = dcValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        if (RECVLineItemvalues.UNITCOST > 0)
                                        {
                                            RECVLineItemvalues.UNITCOSTSpecified = true;
                                        }
                                        break;
                                    case "EXTDCOST":

                                        RECVLineItemvalues.EXTDCOST = dcValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        if (RECVLineItemvalues.EXTDCOST > 0)
                                        {
                                            RECVLineItemvalues.EXTDCOSTSpecified = true;
                                        }
                                        break;
                                    case "NONINVEN":

                                        RECVLineItemvalues.NONINVEN = shValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "JOBNUMBR":

                                        RECVLineItemvalues.JOBNUMBR = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "BOLPRONUMBER":

                                        RECVLineItemvalues.BOLPRONUMBER = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "QTYSHPPD":

                                        RECVLineItemvalues.QTYSHPPD = dcValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "QTYINVCD":

                                        RECVLineItemvalues.QTYINVCD = dcValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "AUTOCOST":

                                        RECVLineItemvalues.AUTOCOST = shValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "Purchase_IV_Item_Taxable":

                                        RECVLineItemvalues.Purchase_IV_Item_Taxable = shValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        if (RECVLineItemvalues.Purchase_IV_Item_Taxable == 0)
                                        {
                                            RECVLineItemvalues.Purchase_IV_Item_Taxable = 2;
                                        }
                                        else
                                        {
                                            bTaxRequired = true;
                                        }
                                        break;
                                    case "Purchase_Item_Tax_Schedu":

                                        RECVLineItemvalues.Purchase_Item_Tax_Schedu = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "Purchase_Site_Tax_Schedu":

                                        RECVLineItemvalues.Purchase_Site_Tax_Schedu = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "TAXAMNT":

                                        RECVLineItemvalues.TAXAMNT = dcValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);

                                        break;
                                    case "Landed_Cost_Group_ID":

                                        RECVLineItemvalues.Landed_Cost_Group_ID = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "LOCNCODE":

                                        RECVLineItemvalues.LOCNCODE = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "POLNENUM":

                                        RECVLineItemvalues.POLNENUM = shValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "receiptdate":

                                        RECVLineItemvalues.receiptdate = dtValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]).ToString("yyyy-MM-dd");
                                        break;
                                    case "CURNCYID":

                                        RECVLineItemvalues.CURNCYID = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "ProjNum":

                                        RECVLineItemvalues.ProjNum = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "CostCatID":

                                        RECVLineItemvalues.CostCatID = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "AutoAssignBin":

                                        RECVLineItemvalues.AutoAssignBin = shValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "CMMTTEXT":

                                        RECVLineItemvalues.CMMTTEXT = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "RequesterTrx":

                                        RECVLineItemvalues.RequesterTrx = shValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "USRDEFND1":

                                        RECVLineItemvalues.USRDEFND1 = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "USRDEFND2":

                                        RECVLineItemvalues.USRDEFND2 = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "USRDEFND3":

                                        RECVLineItemvalues.USRDEFND3 = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "USRDEFND4":

                                        RECVLineItemvalues.USRDEFND4 = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;
                                    case "USRDEFND5":

                                        RECVLineItemvalues.USRDEFND5 = strValue(dsIntegrationRECVLineSetup.Tables["RecvTrxLine"].Rows[iMappingSetup], dtSourceFileDataRECVLine.Rows[iSourceDataRECVLineCount]);
                                        break;


                                }
                            }

                            RECVLineItemsArr[iSourceDataRECVLineCount] = RECVLineItemvalues;


                        }

                        // Tax Insert Start

                        if (bTaxRequired)
                        {
                            DataTable dtSourceFileRecvLineDataTax = dtSourceFileDataSource.Select("VNDDOCNM = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VNDDOCNM"]) + "' AND VENDORID = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VENDORID"]) + "' AND TAXABLE = 1", "VNDDOCNM").CopyToDataTable();


                            taPopRcptLineTaxInsert_ItemsTaPopRcptLineTaxInsert[] recvLineTaxArr = new taPopRcptLineTaxInsert_ItemsTaPopRcptLineTaxInsert[dtSourceFileRecvLineDataTax.Rows.Count];

                            for (int iSourceDataTaxCount = 0; iSourceDataTaxCount < dtSourceFileRecvLineDataTax.Rows.Count; iSourceDataTaxCount++)
                            {
                                taPopRcptLineTaxInsert_ItemsTaPopRcptLineTaxInsert recvLineTax = new taPopRcptLineTaxInsert_ItemsTaPopRcptLineTaxInsert();

                                for (iMappingSetup = 0; iMappingSetup < dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows.Count; iMappingSetup++)
                                {
                                    switch (dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup]["eConnect Field"].ToString())
                                    {
                                        case "VENDORID":
                                            recvLineTax.VENDORID = strValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "POPRCTNM":
                                            recvLineTax.POPRCTNM = recvHdrTrans.POPRCTNM;
                                            break;
                                        case "TAXDTLID":
                                            recvLineTax.TAXDTLID = strValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "TAXTYPE":
                                            recvLineTax.TAXTYPE = shValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "ACTINDX":
                                            recvLineTax.ACTINDX = shValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "ACTNUMST":
                                            recvLineTax.ACTNUMST = strValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "TAXAMNT":
                                            recvLineTax.TAXAMNT = dcValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "TAXPURCH":
                                            recvLineTax.TAXPURCH = dcValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "TOTPURCH":
                                            recvLineTax.TOTPURCH = dcValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "RCPTLNNM":
                                            recvLineTax.RCPTLNNM = shValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "FRTTXAMT":
                                            recvLineTax.FRTTXAMT = dcValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "MSCTXAMT":
                                            recvLineTax.MSCTXAMT = dcValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "RequesterTrx":
                                            recvLineTax.RequesterTrx = shValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "USRDEFND1":
                                            recvLineTax.USRDEFND1 = strValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "USRDEFND2":
                                            recvLineTax.USRDEFND2 = strValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "USRDEFND3":
                                            recvLineTax.USRDEFND3 = strValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "USRDEFND4":
                                            recvLineTax.USRDEFND4 = strValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;
                                        case "USRDEFND5":
                                            recvLineTax.USRDEFND5 = strValue(dsIntegrationRECVTaxSetup.Tables["RecvTrxLineTax"].Rows[iMappingSetup], dtSourceFileRecvLineDataTax.Rows[iSourceDataTaxCount]);
                                            break;


                                    }
                                }

                                recvLineTaxArr[iSourceDataTaxCount] = recvLineTax;
                            }


                            recvTransactiontype.taPopRcptLineTaxInsert_Items = recvLineTaxArr;
                            recvLineTaxArr = null;


                        }

                        // Tax Insert End

                        //Do not create distribution  use the Auto Create distribution and pass the Inventory Account to each line items. And update AP account after distribution Created
                        if (bDistRequired)
                        {
                            DataTable dtSourceFileRecvDataDist = dtSourceFileDataSource.Select("VNDDOCNM = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VNDDOCNM"]) + "' AND VENDORID = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VENDORID"]) + "'", "VNDDOCNM").CopyToDataTable();

                            taPopDistribution_ItemsTaPopDistribution[] RECVDistributionvaluesArr = new taPopDistribution_ItemsTaPopDistribution[dtSourceFileRecvDataDist.Rows.Count];


                            for (int iSourceDataDistCount = 0; iSourceDataDistCount < dtSourceFileRecvDataDist.Rows.Count; iSourceDataDistCount++)
                            {
                                taPopDistribution_ItemsTaPopDistribution RECVDistributionvalues = new taPopDistribution_ItemsTaPopDistribution();

                               
                                for (iMappingSetup = 0; iMappingSetup < dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows.Count; iMappingSetup++)
                                {

                                    switch (dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup]["eConnect Field"].ToString())
                                    {
                                        case "POPTYPE":
                                            RECVDistributionvalues.POPTYPE = shValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;

                                        case "POPRCTNM":
                                            RECVDistributionvalues.POPRCTNM = recvHdrTrans.POPRCTNM;
                                            break;

                                        case "SEQNUMBR":
                                            RECVDistributionvalues.SEQNUMBR = shValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;

                                        case "ACTINDX":
                                            RECVDistributionvalues.ACTINDX = shValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;

                                        case "ACTNUMST":

                                            if (POPFORMATACCOUNTSTRING == true)
                                            {
                                                RECVDistributionvalues.ACTNUMST = strValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            }
                                            else
                                            {
                                                //if (strValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]) != ""  || strValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]) !=null)
                                                //{
                                                RECVDistributionvalues.ACTNUMST = strAccountNumber(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount], gpCmpDBConnStr);
                                                //}
                                            }
                                            break;

                                        case "DEBITAMT":

                                            decimal dcTempDRValue = dcValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);

                                            RECVDistributionvalues.DEBITAMT = dcTempDRValue;

                                            break;

                                        case "CRDTAMNT":


                                            decimal dcTempCRValue = dcValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);

                                            RECVDistributionvalues.CRDTAMNT = dcTempCRValue;
                                            break;

                                        case "DistRef":
                                            RECVDistributionvalues.DistRef = strValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;

                                        case "DISTTYPE":
                                            RECVDistributionvalues.DISTTYPE = shValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;

                                        case "VENDORID":
                                            RECVDistributionvalues.VENDORID = strValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;

                                        case "RequesterTrx":
                                            RECVDistributionvalues.RequesterTrx = shValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;

                                        case "USRDEFND1":
                                            RECVDistributionvalues.USRDEFND1 = strValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;

                                        case "USRDEFND2":
                                            RECVDistributionvalues.USRDEFND2 = strValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;

                                        case "USRDEFND3":
                                            RECVDistributionvalues.USRDEFND3 = strValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;

                                        case "USRDEFND4":
                                            RECVDistributionvalues.USRDEFND4 = strValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;

                                        case "USRDEFND5":
                                            RECVDistributionvalues.USRDEFND5 = strValue(dsIntegrationRECVDistSetup.Tables["RecvTrxDist"].Rows[iMappingSetup], dtSourceFileRecvDataDist.Rows[iSourceDataDistCount]);
                                            break;



                                    }
                                }


                                RECVDistributionvaluesArr[iSourceDataDistCount] = RECVDistributionvalues;

                            }

                            recvTransactiontype.taPopDistribution_Items = RECVDistributionvaluesArr;
                            RECVDistributionvaluesArr = null;
                        }
                        


                        recvTransactiontype.taPopRcptHdrInsert = recvHdrTrans;


                        recvTransactiontype.taPopRcptLineInsert_Items = RECVLineItemsArr;

                        RECVLineItemsArr = null;

                         
                        POPReceivingsType[] myRECVtrx = { recvTransactiontype };
                        eConnect.POPReceivingsType = myRECVtrx;

                        FileStream fs = new FileStream("RecevingTransaction.xml", FileMode.Create);


                        XmlTextWriter writer = new XmlTextWriter(fs, new UTF8Encoding());
                        XmlSerializer serializer = new XmlSerializer(eConnect.GetType());
                        serializer.Serialize(writer, eConnect);

                        writer.Close();


                        XmlDocument xmldoc = new XmlDocument();
                        xmldoc.Load("RecevingTransaction.xml");
                        string RECVDocument = xmldoc.OuterXml;


                        EventLogger.WriteToEventLog("Warning: Receving Transaction file processing has been processed - Final.");
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog("Warning: Receving Transaction file processing has been processed - Final.", filePath, EventLogEntryType.Warning);
                        }

                        eConCall.CreateTransactionEntity(gpCmpDBConnStr, RECVDocument);
                        
                        if (bUpdateDistRequired)
                        {
                            DBLibrary.SQLUpdateRecvDistribution("sp_Update_Dist_Account", gpCmpDBConnStr, strNextNumber, APAccountIndx, MiscAccountIndx, FreightAccountIndx, TradeAccountIndx);
                        }
                        recvTransactiontype = null;

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

                                        RollBackDocument RollbackDoc = new RollBackDocument(Microsoft.Dynamics.GP.eConnect.TransactionType.POPReceipt, null, strNextNumber);
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
                bReturnValue = false;
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog("Warning: No Data found in the file " + ex.Message.ToString(), filePath, EventLogEntryType.Error);
                }
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
                    sValue = getNextNumber.GetNextPOPReceiptNumber(Microsoft.Dynamics.GP.eConnect.IncrementDecrement.Increment, gpCompDBConnString);
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

        private static int longIntValue(DataRow drMappingSetup, DataRow drSourceFileData)
        {
            int shValue = 0;

            string sValue = "";
            switch (drMappingSetup["Value Type"].ToString())
            {

                case "Source":
                    sValue = drSourceFileData[drMappingSetup["ValueorColumn"].ToString().Trim()].ToString().Trim().Replace("\"", "");
                    if (sValue != "")
                    {
                        shValue = Convert.ToInt32(sValue);
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
