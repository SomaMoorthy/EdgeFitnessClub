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
    class EnterMatchInvoiceFileDataPush
    {
        public static Boolean bAutoGenerateNum = false;
        public static string strNextNumber = "";
        public static string strExceptionMessage = "";
        public static Boolean EnterMatchInvoiceFileProcessing(DataTable dtSourceFileDataSource, DataSet dsIntegrationENTMATCHINVHdrSetup, DataSet dsIntegrationENTMATCHINVLineSetup, DataSet dsIntegrationENTMATCHINVTaxSetup, DataSet dsIntegrationENTMATCHINVDistSetup, DataRow drIntegrationSetup, string[] strSourceColumnNames, string gpCmpDBConnStr, string strCompanyName, string strInvoiceMapID, Boolean ENTMATCHINVFORMATACCOUNTSTRING)
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

                EventLogger.WriteToEventLog("Warning: Enter Match Invoice Header file processing has been processed - Start.");
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog("Warning: Enter Match Invoice Header file processing has been processed - Start.", filePath, EventLogEntryType.Error);
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
                        POPEnterMatchInvoiceType enterMatchTransactiontype = new POPEnterMatchInvoiceType();
                        taPopEnterMatchInvHdr enterMatchHdrTrans = new taPopEnterMatchInvHdr();
                        for (iMappingSetup = 0; iMappingSetup < dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows.Count; iMappingSetup++)
                        {

                            switch (dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup]["eConnect Field"].ToString())
                            {
                                case "POPRCTNM":

                                    enterMatchHdrTrans.POPRCTNM = strAutoValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount], gpCmpDBConnStr);
                                    break;
                                case "VNDDOCNM":

                                    enterMatchHdrTrans.VNDDOCNM = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "receiptdate":

                                    enterMatchHdrTrans.receiptdate = dtValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "BACHNUMB":

                                    enterMatchHdrTrans.BACHNUMB = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "VENDORID":

                                    enterMatchHdrTrans.VENDORID = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "VENDNAME":

                                    enterMatchHdrTrans.VENDNAME = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "SUBTOTAL":

                                    enterMatchHdrTrans.SUBTOTAL = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    //if (enterMatchHdrTrans.SUBTOTAL > 0)
                                    //{
                                    //    enterMatchHdrTrans.SUBTOTALSpecified = true;
                                    //}
                                    break;
                                case "TRDISAMT":

                                    enterMatchHdrTrans.TRDISAMT = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    //if (enterMatchHdrTrans.TRDISAMT > 0)
                                    //{
                                    //    enterMatchHdrTrans.TRDISAMTSpecified = true;
                                    //}
                                    break;
                                case "FRTAMNT":

                                    enterMatchHdrTrans.FRTAMNT = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "MISCAMNT":

                                    enterMatchHdrTrans.MISCAMNT = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "TAXAMNT":

                                    enterMatchHdrTrans.TAXAMNT = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "TEN99AMNT":

                                    enterMatchHdrTrans.TEN99AMNT = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "PYMTRMID":

                                    enterMatchHdrTrans.PYMTRMID = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "DSCPCTAM":

                                    enterMatchHdrTrans.DSCPCTAM = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (enterMatchHdrTrans.DSCPCTAM > 0)
                                    {
                                        enterMatchHdrTrans.DSCPCTAMSpecified = true;
                                    }
                                    break;
                                case "DSCDLRAM":

                                    enterMatchHdrTrans.DSCDLRAM = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (enterMatchHdrTrans.DSCDLRAM > 0)
                                    {
                                        enterMatchHdrTrans.DSCDLRAMSpecified = true;
                                    }
                                    break;
                                case "DISAVAMT":

                                    enterMatchHdrTrans.DISAVAMT = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (enterMatchHdrTrans.DISAVAMT > 0)
                                    {
                                        enterMatchHdrTrans.DISAVAMTSpecified = true;
                                    }
                                    break;
                                case "USER2ENT":

                                    enterMatchHdrTrans.USER2ENT = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "TIME1":

                                    enterMatchHdrTrans.TIME1 = dtValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "AUTOCOST":

                                    enterMatchHdrTrans.AUTOCOST = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "TAXSCHID":

                                    enterMatchHdrTrans.TAXSCHID = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "Purchase_Freight_Taxable":

                                    enterMatchHdrTrans.Purchase_Freight_Taxable = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "Purchase_Misc_Taxable":

                                    enterMatchHdrTrans.Purchase_Misc_Taxable = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "FRTSCHID":

                                    enterMatchHdrTrans.FRTSCHID = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "MSCSCHID":

                                    enterMatchHdrTrans.MSCSCHID = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "FRTTXAMT":

                                    enterMatchHdrTrans.FRTTXAMT = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "MSCTXAMT":

                                    enterMatchHdrTrans.MSCTXAMT = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USINGHEADERLEVELTAXES":

                                    enterMatchHdrTrans.USINGHEADERLEVELTAXES = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CURNCYID":

                                    enterMatchHdrTrans.CURNCYID = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "XCHGRATE":

                                    enterMatchHdrTrans.XCHGRATE = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "RATETPID":

                                    enterMatchHdrTrans.RATETPID = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "EXPNDATE":

                                    enterMatchHdrTrans.EXPNDATE = dtValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "EXCHDATE":

                                    enterMatchHdrTrans.EXCHDATE = dtValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "EXGTBDSC":

                                    enterMatchHdrTrans.EXGTBDSC = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "EXTBLSRC":

                                    enterMatchHdrTrans.EXTBLSRC = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "RATEEXPR":

                                    enterMatchHdrTrans.RATEEXPR = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "DYSTINCR":

                                    enterMatchHdrTrans.DYSTINCR = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "RATEVARC":

                                    enterMatchHdrTrans.RATEVARC = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "TRXDTDEF":

                                    enterMatchHdrTrans.TRXDTDEF = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "RTCLCMTD":

                                    enterMatchHdrTrans.RTCLCMTD = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "PRVDSLMT":

                                    enterMatchHdrTrans.PRVDSLMT = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "DATELMTS":

                                    enterMatchHdrTrans.DATELMTS = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "DUEDATE":

                                    enterMatchHdrTrans.DUEDATE = dtValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "DISCDATE":

                                    enterMatchHdrTrans.DISCDATE = dtValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "NOTETEXT":

                                    enterMatchHdrTrans.NOTETEXT = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "RequesterTrx":

                                    enterMatchHdrTrans.RequesterTrx = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "CREATEDIST":
                                    //Auto create distribution(Always set constant CREATEDIST = 1 in database)  use the Auto Create distribution because, the PURCH account is already Debited. We can use only discount, fright and misc account
                                    enterMatchHdrTrans.CREATEDIST = shValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (enterMatchHdrTrans.CREATEDIST == 0)
                                    {
                                        bDistRequired = true;
                                    }
                                    break;
                                case "BackoutTradeDiscTax":

                                    enterMatchHdrTrans.BackoutTradeDiscTax = dcValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "VADCDTRO":

                                    enterMatchHdrTrans.VADCDTRO = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USRDEFND1":

                                    enterMatchHdrTrans.USRDEFND1 = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USRDEFND2":

                                    enterMatchHdrTrans.USRDEFND2 = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USRDEFND3":

                                    enterMatchHdrTrans.USRDEFND3 = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USRDEFND4":

                                    enterMatchHdrTrans.USRDEFND4 = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "USRDEFND5":

                                    enterMatchHdrTrans.USRDEFND5 = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "APAccount":
                                    
                                    if (ENTMATCHINVFORMATACCOUNTSTRING == true)
                                    {
                                        APAccount = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    }
                                    else
                                    {
                                        APAccount = strAccountNumber(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount], gpCmpDBConnStr);
                                    }
                                    if (APAccount != "")
                                    {
                                        APAccountIndx = DBLibrary.SQLValidateAccountString("sp_Validate_Account_Number", gpCmpDBConnStr, APAccount);
                                        bUpdateDistRequired = true;
                                    }

                                    break;
                                case "MiscAccount":

                                    if (ENTMATCHINVFORMATACCOUNTSTRING == true)
                                    {
                                        MiscAccount = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    }
                                    else
                                    {
                                        MiscAccount = strAccountNumber(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount], gpCmpDBConnStr);
                                    }
                                    if (MiscAccount != "")
                                    {
                                        MiscAccountIndx = DBLibrary.SQLValidateAccountString("sp_Validate_Account_Number", gpCmpDBConnStr, MiscAccount);
                                        bUpdateDistRequired = true;
                                    }

                                    break;
                                case "FreigtAccount":

                                    if (ENTMATCHINVFORMATACCOUNTSTRING == true)
                                    {
                                        FreightAccount = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    }
                                    else
                                    {
                                        FreightAccount = strAccountNumber(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount], gpCmpDBConnStr);
                                    }
                                    if (FreightAccount != "")
                                    {
                                        FreightAccountIndx = DBLibrary.SQLValidateAccountString("sp_Validate_Account_Number", gpCmpDBConnStr, FreightAccount);
                                        bUpdateDistRequired = true;
                                    }

                                    break;
                                case "TradeDiscAccount":

                                    if (ENTMATCHINVFORMATACCOUNTSTRING == true)
                                    {
                                        TradeAccount = strValue(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    }
                                    else
                                    {
                                        TradeAccount = strAccountNumber(dsIntegrationENTMATCHINVHdrSetup.Tables["EnterMatchTrxHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount], gpCmpDBConnStr);
                                    }
                                    if (TradeAccount != "")
                                    {
                                        TradeAccountIndx = DBLibrary.SQLValidateAccountString("sp_Validate_Account_Number", gpCmpDBConnStr, TradeAccount);
                                        bUpdateDistRequired = true;
                                    }

                                    break;

                            }



                        }

                         


                        EventLogger.WriteToEventLog("Warning: Enter Match Invoice Header processing has been processed - Enter Match Line.");

                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog("Warning: Enter Match Invoice Header processing has been processed - Enter Match Line.", filePath, EventLogEntryType.Warning);
                        }

                        DataTable dtSourceFileDataENTMATCHINVLine = dtSourceFileDataSource.Select("VNDDOCNM = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VNDDOCNM"]) + "' AND VENDORID = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VENDORID"]) + "'", "VNDDOCNM").CopyToDataTable();
                        
                       
                        taPopEnterMatchInvLine_ItemsTaPopEnterMatchInvLine[] ENTMATCHLineItemsArr = new taPopEnterMatchInvLine_ItemsTaPopEnterMatchInvLine[dtSourceFileDataENTMATCHINVLine.Rows.Count];

                        for (int iSourceDataENTMATCHLineCount = 0; iSourceDataENTMATCHLineCount < dtSourceFileDataENTMATCHINVLine.Rows.Count; iSourceDataENTMATCHLineCount++)
                        {

                            taPopEnterMatchInvLine_ItemsTaPopEnterMatchInvLine ENTMATCHINVLineItemvalues = new taPopEnterMatchInvLine_ItemsTaPopEnterMatchInvLine();

 
                            for (iMappingSetup = 0; iMappingSetup < dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows.Count; iMappingSetup++)
                            {

                                switch (dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup]["eConnect Field"].ToString())
                                {
                                    case "POPRCTNM":

                                        ENTMATCHINVLineItemvalues.POPRCTNM = enterMatchHdrTrans.POPRCTNM;
                                        break;
                                    case "POPMtchShpRcpt":

                                        ENTMATCHINVLineItemvalues.POPMtchShpRcpt = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "ShipRCPTLNNM":

                                        ENTMATCHINVLineItemvalues.ShipRCPTLNNM = longIntValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "PONUMBER":

                                        ENTMATCHINVLineItemvalues.PONUMBER = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "QTYINVCD":

                                        ENTMATCHINVLineItemvalues.QTYINVCD = dcValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "Revalue_Inventory":

                                        ENTMATCHINVLineItemvalues.Revalue_Inventory = shValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "ITEMNMBR":

                                        ENTMATCHINVLineItemvalues.ITEMNMBR = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    
                                    case "VENDORID":

                                        ENTMATCHINVLineItemvalues.VENDORID = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "RCPTLNNM":

                                        ENTMATCHINVLineItemvalues.RCPTLNNM = shValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "VNDITNUM":

                                        ENTMATCHINVLineItemvalues.VNDITNUM = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "VNDITDSC":

                                        ENTMATCHINVLineItemvalues.VNDITDSC = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "VarianceAccount":
                                        if (ENTMATCHINVFORMATACCOUNTSTRING == true)
                                        {
                                            ENTMATCHINVLineItemvalues.VarianceAccount = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        }
                                        else
                                        {
                                            ENTMATCHINVLineItemvalues.VarianceAccount = strAccountNumber(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount], gpCmpDBConnStr);
                                        }
                                        break;
                                    case "UOFM":

                                        ENTMATCHINVLineItemvalues.UOFM = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "UNITCOST":

                                        ENTMATCHINVLineItemvalues.UNITCOST = dcValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        //if (ENTMATCHINVLineItemvalues.UNITCOST > 0)
                                        //{
                                        //    ENTMATCHINVLineItemvalues.UNITCOSTSpecified = true;
                                        //}
                                        break;
                                    case "EXTDCOST":

                                        ENTMATCHINVLineItemvalues.EXTDCOST = dcValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        //if (ENTMATCHINVLineItemvalues.EXTDCOST > 0)
                                        //{
                                        //    ENTMATCHINVLineItemvalues.EXTDCOSTSpecified = true;
                                        //}
                                        break;
                                    case "NONINVEN":

                                        ENTMATCHINVLineItemvalues.NONINVEN = shValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "AUTOCOST":

                                        ENTMATCHINVLineItemvalues.AUTOCOST = shValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "Purchase_IV_Item_Taxable":

                                        ENTMATCHINVLineItemvalues.Purchase_IV_Item_Taxable = shValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        if (ENTMATCHINVLineItemvalues.Purchase_IV_Item_Taxable == 0)
                                        {
                                            ENTMATCHINVLineItemvalues.Purchase_IV_Item_Taxable = 2;
                                        }
                                        else
                                        {
                                            bTaxRequired = true;
                                        }
                                        break;
                                    case "Purchase_Item_Tax_Schedu":

                                        ENTMATCHINVLineItemvalues.Purchase_Item_Tax_Schedu = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "Purchase_Site_Tax_Schedu":

                                        ENTMATCHINVLineItemvalues.Purchase_Site_Tax_Schedu = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "TAXAMNT":

                                        ENTMATCHINVLineItemvalues.TAXAMNT = dcValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);

                                        break;
                                    case "RequesterTrx":

                                        ENTMATCHINVLineItemvalues.RequesterTrx = shValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "POLNENUM":

                                        ENTMATCHINVLineItemvalues.POLNENUM = longIntValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "AutoInvoice":

                                        ENTMATCHINVLineItemvalues.AutoInvoice = shValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "CURNCYID":

                                        ENTMATCHINVLineItemvalues.CURNCYID = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "LandedCost":

                                        ENTMATCHINVLineItemvalues.LandedCost = shValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "ProjNum":

                                        ENTMATCHINVLineItemvalues.ProjNum = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "CostCatID":

                                        ENTMATCHINVLineItemvalues.CostCatID = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "TrackedDropShipped":

                                        ENTMATCHINVLineItemvalues.TrackedDropShipped = shValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "USRDEFND1":

                                        ENTMATCHINVLineItemvalues.USRDEFND1 = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "USRDEFND2":

                                        ENTMATCHINVLineItemvalues.USRDEFND2 = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "USRDEFND3":

                                        ENTMATCHINVLineItemvalues.USRDEFND3 = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "USRDEFND4":

                                        ENTMATCHINVLineItemvalues.USRDEFND4 = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;
                                    case "USRDEFND5":

                                        ENTMATCHINVLineItemvalues.USRDEFND5 = strValue(dsIntegrationENTMATCHINVLineSetup.Tables["EnterMatchTrxLine"].Rows[iMappingSetup], dtSourceFileDataENTMATCHINVLine.Rows[iSourceDataENTMATCHLineCount]);
                                        break;


                                }
                            }

                            ENTMATCHLineItemsArr[iSourceDataENTMATCHLineCount] = ENTMATCHINVLineItemvalues;


                        }

                        // Tax Insert Start

                        if (bTaxRequired)
                        {
                            DataTable dtSourceFileEntMatchInvLineDataTax = dtSourceFileDataSource.Select("VNDDOCNM = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VNDDOCNM"]) + "' AND VENDORID = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VENDORID"]) + "' AND TAXABLE = 1", "VNDDOCNM").CopyToDataTable();


                            taPopRcptLineTaxInsert_ItemsTaPopRcptLineTaxInsert[] entMatchInvLineTaxArr = new taPopRcptLineTaxInsert_ItemsTaPopRcptLineTaxInsert[dtSourceFileEntMatchInvLineDataTax.Rows.Count];

                            for (int iSourceDataEntMatchInvTaxCount = 0; iSourceDataEntMatchInvTaxCount < dtSourceFileEntMatchInvLineDataTax.Rows.Count; iSourceDataEntMatchInvTaxCount++)
                            {
                                taPopRcptLineTaxInsert_ItemsTaPopRcptLineTaxInsert entMatchInvLineTax = new taPopRcptLineTaxInsert_ItemsTaPopRcptLineTaxInsert();

                                for (iMappingSetup = 0; iMappingSetup < dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows.Count; iMappingSetup++)
                                {
                                    switch (dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup]["eConnect Field"].ToString())
                                    {
                                        case "VENDORID":
                                            entMatchInvLineTax.VENDORID = strValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "POPRCTNM":
                                            entMatchInvLineTax.POPRCTNM = enterMatchHdrTrans.POPRCTNM;
                                            break;
                                        case "TAXDTLID":
                                            entMatchInvLineTax.TAXDTLID = strValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "TAXTYPE":
                                            entMatchInvLineTax.TAXTYPE = shValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "ACTINDX":
                                            entMatchInvLineTax.ACTINDX = shValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "ACTNUMST":
                                            entMatchInvLineTax.ACTNUMST = strValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "TAXAMNT":
                                            entMatchInvLineTax.TAXAMNT = dcValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "TAXPURCH":
                                            entMatchInvLineTax.TAXPURCH = dcValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "TOTPURCH":
                                            entMatchInvLineTax.TOTPURCH = dcValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "RCPTLNNM":
                                            entMatchInvLineTax.RCPTLNNM = shValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "FRTTXAMT":
                                            entMatchInvLineTax.FRTTXAMT = dcValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "MSCTXAMT":
                                            entMatchInvLineTax.MSCTXAMT = dcValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "RequesterTrx":
                                            entMatchInvLineTax.RequesterTrx = shValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "USRDEFND1":
                                            entMatchInvLineTax.USRDEFND1 = strValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "USRDEFND2":
                                            entMatchInvLineTax.USRDEFND2 = strValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "USRDEFND3":
                                            entMatchInvLineTax.USRDEFND3 = strValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "USRDEFND4":
                                            entMatchInvLineTax.USRDEFND4 = strValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;
                                        case "USRDEFND5":
                                            entMatchInvLineTax.USRDEFND5 = strValue(dsIntegrationENTMATCHINVTaxSetup.Tables["EnterMatchTrxLineTax"].Rows[iMappingSetup], dtSourceFileEntMatchInvLineDataTax.Rows[iSourceDataEntMatchInvTaxCount]);
                                            break;


                                    }
                                }

                                entMatchInvLineTaxArr[iSourceDataEntMatchInvTaxCount] = entMatchInvLineTax;
                            }


                            enterMatchTransactiontype.taPopRcptLineTaxInsert_Items = entMatchInvLineTaxArr;
                            entMatchInvLineTaxArr = null;


                        }

                        // Tax Insert End

                        //Do not create distribution 
                        if (bDistRequired)
                        {
                            DataTable dtSourceFileEntMatchInvDataDist = dtSourceFileDataSource.Select("VNDDOCNM = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VNDDOCNM"]) + "' AND VENDORID = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VENDORID"]) + "'", "VNDDOCNM").CopyToDataTable();

                            taPopDistribution_ItemsTaPopDistribution[] ENTMATCHINVDistributionvaluesArr = new taPopDistribution_ItemsTaPopDistribution[dtSourceFileEntMatchInvDataDist.Rows.Count];


                            for (int iSourceDataEntMatchInvDistCount = 0; iSourceDataEntMatchInvDistCount < dtSourceFileEntMatchInvDataDist.Rows.Count; iSourceDataEntMatchInvDistCount++)
                            {
                                taPopDistribution_ItemsTaPopDistribution ENTMATCHINVDistributionvalues = new taPopDistribution_ItemsTaPopDistribution();

                               
                                for (iMappingSetup = 0; iMappingSetup < dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows.Count; iMappingSetup++)
                                {

                                    switch (dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup]["eConnect Field"].ToString())
                                    {
                                        case "POPTYPE":
                                            ENTMATCHINVDistributionvalues.POPTYPE = shValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;

                                        case "POPRCTNM":
                                            ENTMATCHINVDistributionvalues.POPRCTNM = enterMatchHdrTrans.POPRCTNM;
                                            break;

                                        case "SEQNUMBR":
                                            ENTMATCHINVDistributionvalues.SEQNUMBR = shValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;

                                        case "ACTINDX":
                                            ENTMATCHINVDistributionvalues.ACTINDX = shValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;

                                        case "ACTNUMST":

                                            if (ENTMATCHINVFORMATACCOUNTSTRING == true)
                                            {
                                                ENTMATCHINVDistributionvalues.ACTNUMST = strValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            }
                                            else
                                            {
                                                //if (strValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]) != ""  || strValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]) !=null)
                                                //{
                                                ENTMATCHINVDistributionvalues.ACTNUMST = strAccountNumber(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount], gpCmpDBConnStr);
                                                //}
                                            }
                                            break;

                                        case "DEBITAMT":

                                            decimal dcTempDRValue = dcValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);

                                            ENTMATCHINVDistributionvalues.DEBITAMT = dcTempDRValue;

                                            break;

                                        case "CRDTAMNT":


                                            decimal dcTempCRValue = dcValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);

                                            ENTMATCHINVDistributionvalues.CRDTAMNT = dcTempCRValue;
                                            break;

                                        case "DistRef":
                                            ENTMATCHINVDistributionvalues.DistRef = strValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;

                                        case "DISTTYPE":
                                            ENTMATCHINVDistributionvalues.DISTTYPE = shValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;

                                        case "VENDORID":
                                            ENTMATCHINVDistributionvalues.VENDORID = strValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;

                                        case "RequesterTrx":
                                            ENTMATCHINVDistributionvalues.RequesterTrx = shValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;

                                        case "USRDEFND1":
                                            ENTMATCHINVDistributionvalues.USRDEFND1 = strValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;

                                        case "USRDEFND2":
                                            ENTMATCHINVDistributionvalues.USRDEFND2 = strValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;

                                        case "USRDEFND3":
                                            ENTMATCHINVDistributionvalues.USRDEFND3 = strValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;

                                        case "USRDEFND4":
                                            ENTMATCHINVDistributionvalues.USRDEFND4 = strValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;

                                        case "USRDEFND5":
                                            ENTMATCHINVDistributionvalues.USRDEFND5 = strValue(dsIntegrationENTMATCHINVDistSetup.Tables["EnterMatchTrxDist"].Rows[iMappingSetup], dtSourceFileEntMatchInvDataDist.Rows[iSourceDataEntMatchInvDistCount]);
                                            break;



                                    }
                                }


                                ENTMATCHINVDistributionvaluesArr[iSourceDataEntMatchInvDistCount] = ENTMATCHINVDistributionvalues;

                            }

                            enterMatchTransactiontype.taPopDistribution_Items = ENTMATCHINVDistributionvaluesArr;
                            ENTMATCHINVDistributionvaluesArr = null;
                        }
                        


                        enterMatchTransactiontype.taPopEnterMatchInvHdr = enterMatchHdrTrans;


                        enterMatchTransactiontype.taPopEnterMatchInvLine_Items = ENTMATCHLineItemsArr;

                        ENTMATCHLineItemsArr = null;

                         
                        POPEnterMatchInvoiceType[] myENTMATCHINVtrx = { enterMatchTransactiontype };
                        eConnect.POPEnterMatchInvoiceType = myENTMATCHINVtrx;

                        FileStream fs = new FileStream("EnterMatchInvoice.xml", FileMode.Create);


                        XmlTextWriter writer = new XmlTextWriter(fs, new UTF8Encoding());
                        XmlSerializer serializer = new XmlSerializer(eConnect.GetType());
                        serializer.Serialize(writer, eConnect);

                        writer.Close();


                        XmlDocument xmldoc = new XmlDocument();
                        xmldoc.Load("EnterMatchInvoice.xml");
                        string ENTMATCHINVDocument = xmldoc.OuterXml;


                        EventLogger.WriteToEventLog("Warning: Enter Match Invoice file processing has been processed - Final.");
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog("Warning: Enter Match Invoice file processing has been processed - Final.", filePath, EventLogEntryType.Warning);
                        }

                        eConCall.CreateTransactionEntity(gpCmpDBConnStr, ENTMATCHINVDocument);
                        
                        if (bUpdateDistRequired)
                        {
                            DBLibrary.SQLUpdateRecvDistribution("sp_Update_Dist_Account", gpCmpDBConnStr, strNextNumber, APAccountIndx, MiscAccountIndx, FreightAccountIndx, TradeAccountIndx);
                        }
                        enterMatchTransactiontype = null;

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
