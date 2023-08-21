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
    class PurchaseOrderFileDataPush
    {
        public static Boolean bAutoGenerateNum = false;
        public static string strNextNumber = "";
        public static string strExceptionMessage = "";
        public static Boolean PurchaseOrderFileProcessing(DataTable dtSourceFileDataSource, DataSet dsIntegrationPOHdrSetup, DataSet dsIntegrationPOLineSetup, DataSet dsIntegrationPOTaxSetup,DataRow drIntegrationSetup, string[] strSourceColumnNames, string gpCmpDBConnStr, string strCompanyName, string strInvoiceMapID, Boolean POPFORMATACCOUNTSTRING)
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


                EventLogger.WriteToEventLog("Warning: Purchase Order Header file processing has been processed - Start.");
                if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                {
                    string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                    FileLogger.WriteToFileLog("Warning: Purchase Order Header file processing has been processed - Start.", filePath, EventLogEntryType.Error);
                }

                int iSourceDataCount, iMappingSetup;
                for (iSourceDataCount = 0; iSourceDataCount < dtSourceFileData.Rows.Count; iSourceDataCount++)
                {
                    try
                    {
                        Boolean bTaxRequired = false;
                        POPTransactionType popTransactiontype = new POPTransactionType();
                        taPoHdr popHdrTrans = new taPoHdr();
                        for (iMappingSetup = 0; iMappingSetup < dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows.Count; iMappingSetup++)
                        {



                            switch (dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup]["eConnect Field"].ToString())
                            {
                                case "POTYPE":

                                    popHdrTrans.POTYPE = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PONUMBER":

                                    popHdrTrans.PONUMBER = strAutoValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount],gpCmpDBConnStr);
                                    break;

                                case "VENDORID":
                                    popHdrTrans.VENDORID = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "VENDNAME":
                                    popHdrTrans.VENDNAME = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DOCDATE":
                                    popHdrTrans.DOCDATE = dtValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "BUYERID":
                                    popHdrTrans.BUYERID = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "ALLOWSOCMTS":
                                    popHdrTrans.ALLOWSOCMTS = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TRDISAMT":
                                    popHdrTrans.TRDISAMT = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "FRTAMNT":
                                    popHdrTrans.FRTAMNT = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "MSCCHAMT":
                                    popHdrTrans.MSCCHAMT = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TAXAMNT":
                                    popHdrTrans.TAXAMNT = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    if (popHdrTrans.TAXAMNT > 0)
                                    {
                                        popHdrTrans.TAXAMNTSpecified = true;
                                        bTaxRequired = true;
                                         
                                    }
                                    break;

                                case "SUBTOTAL":
                                    popHdrTrans.SUBTOTAL = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CUSTNMBR":
                                    popHdrTrans.CUSTNMBR = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PRSTADCD":
                                    popHdrTrans.PRSTADCD = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CMPNYNAM":
                                    popHdrTrans.CMPNYNAM = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CONTACT":
                                    popHdrTrans.CONTACT = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "ADDRESS1":
                                    popHdrTrans.ADDRESS1 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "ADDRESS2":
                                    popHdrTrans.ADDRESS2 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "ADDRESS3":
                                    popHdrTrans.ADDRESS3 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CITY":
                                    popHdrTrans.CITY = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "STATE":
                                    popHdrTrans.STATE = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "ZIPCODE":
                                    popHdrTrans.ZIPCODE = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CCode":
                                    popHdrTrans.CCode = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "COUNTRY":
                                    popHdrTrans.COUNTRY = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PHONE1":
                                    popHdrTrans.PHONE1 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PHONE2":
                                    popHdrTrans.PHONE2 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PHONE3":
                                    popHdrTrans.PHONE3 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "FAX":
                                    popHdrTrans.FAX = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "Print_Phone_NumberGB":
                                    popHdrTrans.Print_Phone_NumberGB = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "VADCDPAD":
                                    popHdrTrans.VADCDPAD = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHCMPNYNAM":
                                    popHdrTrans.PURCHCMPNYNAM = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHCONTACT":
                                    popHdrTrans.PURCHCONTACT = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHADDRESS1":
                                    popHdrTrans.PURCHADDRESS1 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHADDRESS2":
                                    popHdrTrans.PURCHADDRESS2 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHADDRESS3":
                                    popHdrTrans.PURCHADDRESS3 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHCITY":
                                    popHdrTrans.PURCHCITY = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHSTATE":
                                    popHdrTrans.PURCHSTATE = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHZIPCODE":
                                    popHdrTrans.PURCHZIPCODE = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHCCode":
                                    popHdrTrans.PURCHCCode = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHCOUNTRY":
                                    popHdrTrans.PURCHCOUNTRY = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHPHONE1":
                                    popHdrTrans.PURCHPHONE1 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHPHONE2":
                                    popHdrTrans.PURCHPHONE2 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHPHONE3":
                                    popHdrTrans.PURCHPHONE3 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PURCHFAX":
                                    popHdrTrans.PURCHFAX = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PRBTADCD":
                                    popHdrTrans.PRBTADCD = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "SHIPMTHD":
                                    popHdrTrans.SHIPMTHD = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PYMTRMID":
                                    popHdrTrans.PYMTRMID = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DSCPCTAM":
                                    popHdrTrans.DSCPCTAM = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DSCDLRAM":
                                    popHdrTrans.DSCDLRAM = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DISAMTAV":
                                    popHdrTrans.DISAMTAV = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DUEDATE":
                                    popHdrTrans.DUEDATE = dtValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "DISCDATE":
                                    popHdrTrans.DISCDATE = dtValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "TXRGNNUM":
                                    popHdrTrans.TXRGNNUM = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CONFIRM1":
                                    popHdrTrans.CONFIRM1 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "COMMNTID":
                                    popHdrTrans.COMMNTID = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "COMMENT_1":
                                    popHdrTrans.COMMENT_1 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "COMMENT_2":
                                    popHdrTrans.COMMENT_2 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "COMMENT_3":
                                    popHdrTrans.COMMENT_3 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "COMMENT_4":
                                    popHdrTrans.COMMENT_4 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "HOLD":
                                    popHdrTrans.HOLD = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TAXSCHID":
                                    popHdrTrans.TAXSCHID = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "Purchase_Freight_Taxable":
                                    popHdrTrans.Purchase_Freight_Taxable = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;
                                case "Purchase_Misc_Taxable":
                                    popHdrTrans.Purchase_Misc_Taxable = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "FRTSCHID":
                                    popHdrTrans.FRTSCHID = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "MSCSCHID":
                                    popHdrTrans.MSCSCHID = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "FRTTXAMT":
                                    popHdrTrans.FRTTXAMT = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "MSCTXAMT":
                                    popHdrTrans.MSCTXAMT = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "BCKTXAMT":
                                    popHdrTrans.BCKTXAMT = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "BackoutFreightTaxAmt":
                                    popHdrTrans.BackoutFreightTaxAmt = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "BackoutMiscTaxAmt":
                                    popHdrTrans.BackoutMiscTaxAmt = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "BackoutTradeDiscTax":
                                    popHdrTrans.BackoutTradeDiscTax = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USINGHEADERLEVELTAXES":
                                    popHdrTrans.USINGHEADERLEVELTAXES = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CURNCYID":
                                    popHdrTrans.CURNCYID = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "XCHGRATE":
                                    popHdrTrans.XCHGRATE = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RATETPID":
                                    popHdrTrans.RATETPID = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "EXPNDATE":
                                    popHdrTrans.EXPNDATE = dtValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;
                                case "EXCHDATE":
                                    popHdrTrans.EXCHDATE = dtValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "EXGTBDSC":
                                    popHdrTrans.EXGTBDSC = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "EXTBLSRC":
                                    popHdrTrans.EXTBLSRC = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RATEEXPR":
                                    popHdrTrans.RATEEXPR = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DYSTINCR":
                                    popHdrTrans.DYSTINCR = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RATEVARC":
                                    popHdrTrans.RATEVARC = dcValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TRXDTDEF":
                                    popHdrTrans.TRXDTDEF = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RTCLCMTD":
                                    popHdrTrans.RTCLCMTD = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PRVDSLMT":
                                    popHdrTrans.PRVDSLMT = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "DATELMTS":
                                    popHdrTrans.DATELMTS = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "TIME1":
                                    popHdrTrans.TIME1 = dtValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "USERID":
                                    popHdrTrans.USERID = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "POSTATUS":
                                    popHdrTrans.POSTATUS = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "CMMTTEXT":
                                    popHdrTrans.CMMTTEXT = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "PRMDATE":
                                    popHdrTrans.PRMDATE = dtValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "PRMSHPDTE":
                                    popHdrTrans.PRMSHPDTE = dtValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "REQDATE":
                                    popHdrTrans.REQDATE = dtValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "CONTENDDTE":
                                    popHdrTrans.CONTENDDTE = dtValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "CNTRLBLKTBY":
                                    popHdrTrans.CNTRLBLKTBY = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "REQTNDT":
                                    popHdrTrans.REQTNDT = dtValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]).ToString("yyyy-MM-dd");
                                    break;

                                case "UpdateIfExists":
                                    popHdrTrans.UpdateIfExists = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "NOTETEXT":
                                    popHdrTrans.NOTETEXT = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "RequesterTrx":
                                    popHdrTrans.RequesterTrx = shValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND1":
                                    popHdrTrans.USRDEFND1 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND2":
                                    popHdrTrans.USRDEFND2 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND3":
                                    popHdrTrans.USRDEFND3 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND4":
                                    popHdrTrans.USRDEFND4 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                                case "USRDEFND5":
                                    popHdrTrans.USRDEFND5 = strValue(dsIntegrationPOHdrSetup.Tables["POPHeader"].Rows[iMappingSetup], dtSourceFileData.Rows[iSourceDataCount]);
                                    break;

                            }



                        }


                        EventLogger.WriteToEventLog("Warning: Purchase Order Header processing has been processed - POP Line.");

                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog("Warning: Purchase Order Header processing has been processed - POP Line.", filePath, EventLogEntryType.Error);
                        }

                        DataTable dtSourceFileDataPOLine = dtSourceFileDataSource.Select("PONUMBER = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["PONUMBER"]) + "' AND VENDORID = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VENDORID"]) + "'", "PONUMBER").CopyToDataTable();
                        
                       
                        taPoLine_ItemsTaPoLine[] POPLineItemsArr = new taPoLine_ItemsTaPoLine[dtSourceFileDataPOLine.Rows.Count];

                        for (int iSourceDataPOLineCount = 0; iSourceDataPOLineCount < dtSourceFileDataPOLine.Rows.Count; iSourceDataPOLineCount++)
                        {
                            //taPMDistribution_ItemsTaPMDistribution PMDistributionvalues = new taPMDistribution_ItemsTaPMDistribution();

                            taPoLine_ItemsTaPoLine POPLineItemvalues = new taPoLine_ItemsTaPoLine();

 
                            for (iMappingSetup = 0; iMappingSetup < dsIntegrationPOLineSetup.Tables["POPLine"].Rows.Count; iMappingSetup++)
                            {

                                switch (dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup]["eConnect Field"].ToString())
                                {
                                    case "POTYPE":
                                        POPLineItemvalues.POTYPE = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.POTYPE > 0)
                                        {
                                            POPLineItemvalues.POTYPESpecified = true;
                                        }
                                        break;

                                    case "PONUMBER":
                                        POPLineItemvalues.PONUMBER = popHdrTrans.PONUMBER;
                                        break;

                                    case "DOCDATE":
                                        POPLineItemvalues.DOCDATE = dtValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]).ToString("yyyy-MM-dd");
                                        break;

                                    case "VENDORID":
                                        POPLineItemvalues.VENDORID = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "LOCNCODE":
                                        POPLineItemvalues.LOCNCODE = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "VNDITNUM":
                                        POPLineItemvalues.VNDITNUM = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "ITEMNMBR":
                                        POPLineItemvalues.ITEMNMBR = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "QUANTITY":
                                        POPLineItemvalues.QUANTITY = dcValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.QUANTITY > 0)
                                        {
                                            POPLineItemvalues.QUANTITYSpecified = true;
                                        }
                                        break;

                                    case "QTYCANCE":
                                        POPLineItemvalues.QTYCANCE = dcValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.QTYCANCE > 0)
                                        {
                                            POPLineItemvalues.QTYCANCESpecified = true;
                                        }
                                        break;

                                    case "FREEONBOARD":
                                        POPLineItemvalues.FREEONBOARD = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.FREEONBOARD > 0)
                                        {
                                            POPLineItemvalues.FREEONBOARDSpecified = true;
                                        }
                                        break;

                                    case "REQSTDBY":
                                        POPLineItemvalues.REQSTDBY = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "COMMNTID":
                                        POPLineItemvalues.COMMNTID = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "COMMENT_1":
                                        POPLineItemvalues.COMMENT_1 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "COMMENT_2":
                                        POPLineItemvalues.COMMENT_2 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "COMMENT_3":
                                        POPLineItemvalues.COMMENT_3 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "COMMENT_4":
                                        POPLineItemvalues.COMMENT_4 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "REQDATE":
                                        POPLineItemvalues.REQDATE = dtValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]).ToString("yyyy-MM-dd");
                                        break;

                                    case "RELEASEBYDATE":
                                        POPLineItemvalues.RELEASEBYDATE = dtValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]).ToString("yyyy-MM-dd");
                                        break;

                                    case "PRMDATE":
                                        POPLineItemvalues.PRMDATE = dtValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]).ToString("yyyy-MM-dd");
                                        break;

                                    case "PRMSHPDTE":
                                        POPLineItemvalues.PRMSHPDTE = dtValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]).ToString("yyyy-MM-dd");
                                        break;

                                    case "NONINVEN":
                                        POPLineItemvalues.NONINVEN = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "IVIVINDX":
                                        POPLineItemvalues.IVIVINDX = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.IVIVINDX > 0)
                                        {
                                            POPLineItemvalues.IVIVINDXSpecified = true;
                                        }
                                        break;

                                    case "InventoryAccount":
                                        if (POPFORMATACCOUNTSTRING == true)
                                        {
                                            POPLineItemvalues.InventoryAccount = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        }
                                        else
                                        {
                                            POPLineItemvalues.InventoryAccount = strAccountNumber(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount],gpCmpDBConnStr);
                                        }
                                        break;

                                    case "ITEMDESC":
                                        POPLineItemvalues.ITEMDESC = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "UNITCOST":
                                        POPLineItemvalues.UNITCOST = dcValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.UNITCOST > 0)
                                        {
                                            POPLineItemvalues.UNITCOSTSpecified = true;
                                        }
                                        break;

                                    case "VNDITDSC":
                                        POPLineItemvalues.VNDITDSC = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "UOFM":
                                        POPLineItemvalues.UOFM = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "Purchase_IV_Item_Taxable":
                                        POPLineItemvalues.Purchase_IV_Item_Taxable = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.Purchase_IV_Item_Taxable > 0)
                                        {
                                            POPLineItemvalues.Purchase_IV_Item_TaxableSpecified = true;

                                        }
                                        break;

                                    case "Purchase_Item_Tax_Schedu":
                                        POPLineItemvalues.Purchase_Item_Tax_Schedu = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "Purchase_Site_Tax_Schedu":
                                        POPLineItemvalues.Purchase_Site_Tax_Schedu = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "BSIVCTTL":
                                        POPLineItemvalues.BSIVCTTL = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "TAXAMNT":
                                        POPLineItemvalues.TAXAMNT = dcValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.TAXAMNT > 0)
                                        {
                                            bTaxRequired = true;
                                            POPLineItemvalues.TAXAMNTSpecified = true;
                                        }
                                        break;

                                    case "BCKTXAMT":
                                        POPLineItemvalues.BCKTXAMT = dcValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.BCKTXAMT > 0)
                                        {
                                           POPLineItemvalues.BCKTXAMTSpecified = true;
                                        }
                                        break;

                                    case "Landed_Cost_Group_ID":
                                        POPLineItemvalues.Landed_Cost_Group_ID = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "PLNNDSPPLID":
                                        POPLineItemvalues.PLNNDSPPLID = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.PLNNDSPPLID > 0)
                                        {
                                            POPLineItemvalues.PLNNDSPPLIDSpecified = true;
                                        }
                                        break;

                                    case "SHIPMTHD":
                                        POPLineItemvalues.SHIPMTHD = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "BackoutTradeDiscTax":
                                        POPLineItemvalues.BackoutTradeDiscTax = dcValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.BackoutTradeDiscTax > 0)
                                        {
                                            POPLineItemvalues.BackoutTradeDiscTaxSpecified = true;
                                        }
                                        break;

                                    case "POLNESTA":
                                        POPLineItemvalues.POLNESTA = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.POLNESTA > 0)
                                        {
                                            POPLineItemvalues.POLNESTASpecified = true;
                                        }
                                        break;

                                    case "CMMTTEXT":
                                        POPLineItemvalues.CMMTTEXT = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "ORD":
                                        POPLineItemvalues.ORD = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.ORD > 0)
                                        {
                                            POPLineItemvalues.ORDSpecified = true;
                                        }
                                        break;

                                    case "CUSTNMBR":
                                        POPLineItemvalues.CUSTNMBR = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "ADRSCODE":
                                        POPLineItemvalues.ADRSCODE = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "CMPNYNAM":
                                        POPLineItemvalues.CMPNYNAM = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "CONTACT":
                                        POPLineItemvalues.CONTACT = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "ADDRESS1":
                                        POPLineItemvalues.ADDRESS1 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "ADDRESS2":
                                        POPLineItemvalues.ADDRESS2 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "ADDRESS3":
                                        POPLineItemvalues.ADDRESS3 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "CITY":
                                        POPLineItemvalues.CITY = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "STATE":
                                        POPLineItemvalues.STATE = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "ZIPCODE":
                                        POPLineItemvalues.ZIPCODE = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "Ccode":
                                        POPLineItemvalues.CCode = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "COUNTRY":
                                        POPLineItemvalues.COUNTRY = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "PHONE1":
                                        POPLineItemvalues.PHONE1 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "PHONE2":
                                        POPLineItemvalues.PHONE2 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "PHONE3":
                                        POPLineItemvalues.PHONE3 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "FAX":
                                        POPLineItemvalues.FAX = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "Print_Phone_NumberGB":
                                        POPLineItemvalues.Print_Phone_NumberGB = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.Print_Phone_NumberGB > 0)
                                        {
                                            POPLineItemvalues.Print_Phone_NumberGBSpecified = true;
                                        }
                                        break;

                                    case "CURNCYID":
                                        POPLineItemvalues.CURNCYID = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "ProjNum":
                                        POPLineItemvalues.ProjNum = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "CostCatID":
                                        POPLineItemvalues.CostCatID = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "LineNumber":
                                        POPLineItemvalues.LineNumber = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        if (POPLineItemvalues.LineNumber > 0)
                                        {
                                            POPLineItemvalues.LineNumberSpecified = true;
                                        }
                                        break;

                                    case "UpdateIfExists":
                                        POPLineItemvalues.UpdateIfExists = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "NOTETEXT":
                                        POPLineItemvalues.NOTETEXT = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "RequesterTrx":
                                        POPLineItemvalues.RequesterTrx = shValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "USRDEFND1":
                                        POPLineItemvalues.USRDEFND1 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "USRDEFND2":
                                        POPLineItemvalues.USRDEFND2 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "USRDEFND3":
                                        POPLineItemvalues.USRDEFND3 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "USRDEFND4":
                                        POPLineItemvalues.USRDEFND4 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                    case "USRDEFND5":
                                        POPLineItemvalues.USRDEFND5 = strValue(dsIntegrationPOLineSetup.Tables["POPLine"].Rows[iMappingSetup], dtSourceFileDataPOLine.Rows[iSourceDataPOLineCount]);
                                        break;

                                }
                            }

                            POPLineItemsArr[iSourceDataPOLineCount] = POPLineItemvalues;


                        }

                        // Tax Insert Start

                        if (bTaxRequired)
                        {
                            DataTable dtSourceFileDataTax = dtSourceFileDataSource.Select("PONUMBER = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["PONUMBER"]) + "' AND VENDORID = '" + Convert.ToString(dtSourceFileData.Rows[iSourceDataCount]["VENDORID"]) + "' AND TAXABLE = 1", "PONUMBER").CopyToDataTable();


                            taPopIvcTaxInsert_ItemsTaPopIvcTaxInsert[] popLineTaxArr = new taPopIvcTaxInsert_ItemsTaPopIvcTaxInsert[dtSourceFileDataTax.Rows.Count];

                            for (int iSourceDataTaxCount = 0; iSourceDataTaxCount < dtSourceFileDataTax.Rows.Count; iSourceDataTaxCount++)
                            {
                                taPopIvcTaxInsert_ItemsTaPopIvcTaxInsert popLineTax = new taPopIvcTaxInsert_ItemsTaPopIvcTaxInsert();

                                for (iMappingSetup = 0; iMappingSetup < dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows.Count; iMappingSetup++)
                                {
                                    switch (dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup]["eConnect Field"].ToString())
                                    {
                                        case "PONUMBER":
                                            popLineTax.PONUMBER = popHdrTrans.PONUMBER;
                                            break;

                                        case "TAXTYPE":
                                            popLineTax.TAXTYPE = shValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "ORD":
                                            popLineTax.ORD = shValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            popLineTax.ORD = popLineTax.ORD * 16384;
                                            break;

                                        case "TAXDTLID":
                                            popLineTax.TAXDTLID = strValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "BKOUTTAX":
                                            popLineTax.BKOUTTAX = shValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "TAXAMNT":
                                            popLineTax.TAXAMNT = dcValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "FRTTXAMT":
                                            popLineTax.FRTTXAMT = dcValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "MSCTXAMT":
                                            popLineTax.MSCTXAMT = dcValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "TAXPURCH":
                                            popLineTax.TAXPURCH = dcValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "TOTPURCH":
                                            popLineTax.TOTPURCH = dcValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "TRXSORCE":
                                            popLineTax.TRXSORCE = strValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "VENDORID":
                                            popLineTax.VENDORID = strValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "RequesterTrx":
                                            popLineTax.RequesterTrx = shValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "USRDEFND1":
                                            popLineTax.USRDEFND1 = strValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "USRDEFND2":
                                            popLineTax.USRDEFND2 = strValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "USRDEFND3":
                                            popLineTax.USRDEFND3 = strValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;

                                        case "USRDEFND4":
                                            popLineTax.USRDEFND4 = strValue(dsIntegrationPOTaxSetup.Tables["POPLineTax"].Rows[iMappingSetup], dtSourceFileDataTax.Rows[iSourceDataTaxCount]);
                                            break;


                                    }
                                }

                                popLineTaxArr[iSourceDataTaxCount] = popLineTax;
                            }


                            popTransactiontype.taPopIvcTaxInsert_Items = popLineTaxArr;
                            popLineTaxArr = null;


                        }

                        // Tax Insert End

                        popTransactiontype.taPoHdr = popHdrTrans;

                         
                        popTransactiontype.taPoLine_Items = POPLineItemsArr;
                         
                        POPLineItemsArr = null;

                         
                        POPTransactionType[] myPOPtrx = { popTransactiontype };
                        eConnect.POPTransactionType = myPOPtrx;

                        FileStream fs = new FileStream("PurchaseOrder.xml", FileMode.Create);


                        XmlTextWriter writer = new XmlTextWriter(fs, new UTF8Encoding());
                        XmlSerializer serializer = new XmlSerializer(eConnect.GetType());
                        serializer.Serialize(writer, eConnect);

                        writer.Close();


                        XmlDocument xmldoc = new XmlDocument();
                        xmldoc.Load("PurchaseOrder.xml");
                        string POPDocument = xmldoc.OuterXml;


                        EventLogger.WriteToEventLog("Warning: Purchase Order file processing has been processed - Final.");
                        if (Convert.ToBoolean(drIntegrationSetup["ENABLETEXTEVENTLOG"].ToString().Trim()) == true)
                        {
                            string filePath = drIntegrationSetup["LOGFILEPATH"].ToString().Trim();
                            FileLogger.WriteToFileLog("Warning: Purchase Order Header file processing has been processed - Final.", filePath, EventLogEntryType.Error);
                        }

                        eConCall.CreateTransactionEntity(gpCmpDBConnStr, POPDocument);
                        popTransactiontype = null;

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

                                        RollBackDocument RollbackDoc = new RollBackDocument(Microsoft.Dynamics.GP.eConnect.TransactionType.POP, null, strNextNumber);
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
                    sValue = getNextNumber.GetNextPONumber(Microsoft.Dynamics.GP.eConnect.IncrementDecrement.Increment, gpCompDBConnString);
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
