using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SUTIAPGPIntegrationService.Utilities
{
    public class SQLLibrary
    {

        private static string query = null;

        public static string SQLSUTIAPGPIntegrationSetup()
        {
            query = "SELECT * FROM dbo.[SUTI_AP_GP_Integration_Setup] WHERE ACTIVE = 1";
            return query;
        }

        public static string SQLCompanyDBExists(string strCompanyDB)
        {
            query = "SELECT isnull(name,'') name FROM master.dbo.sysdatabases WHERE  name  = '" + strCompanyDB + "'";
            return query;
        }

        public static string SQLInvoicPaymenteHeaderMappingColumns(string strMapID, string strCompanyID)
        {
            query = "SELECT Distinct [ValueorColumn]  FROM [dbo].[SUTI_AP_GP_Integration_Invoice_Header_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [INVOICEMAPID] = '" + strMapID + "' and [Value Type] = 'Source'";
            return query;
        }



        public static string SQLInvoicPaymenteHeaderMapping(string strMapID,string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_Integration_Invoice_Header_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [INVOICEMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLPOPHeaderMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_POP_Header_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLPOPLineMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_POP_Line_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLPOPTaxMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_POP_Tax_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLPOPHeaderMappingColumns(string strMapID, string strCompanyID)
        {
            query = "SELECT Distinct [ValueorColumn]  FROM [dbo].[SUTI_AP_GP_POP_Header_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "' and [Value Type] = 'Source'";
           // query = "SELECT Distinct [ValueorColumn]  FROM [dbo].[SUTI_AP_GP_POP_Header_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLRECVHeaderMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_RECV_Header_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLRECVLineMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_RECV_Line_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLRECVTaxMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_RECV_Tax_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }
        public static string SQLRECVDistMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_RECV_Dist_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLRECVHeaderMappingColumns(string strMapID, string strCompanyID)
        {
            query = "SELECT Distinct [ValueorColumn]  FROM [dbo].[SUTI_AP_GP_RECV_Header_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "' and [Value Type] = 'Source'";
            // query = "SELECT Distinct [ValueorColumn]  FROM [dbo].[SUTI_AP_GP_POP_Header_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLENTMATCHINVHeaderMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_ENMATCHINV_Header_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLENTMATCHINVLineMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_ENMATCHINV_Line_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLENTMATCHINVTaxMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_ENMATCHINV_Tax_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }
        public static string SQLENTMATCHINVDistMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_ENMATCHINV_Dist_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLENTMATCHINVHeaderMappingColumns(string strMapID, string strCompanyID)
        {
            query = "SELECT Distinct [ValueorColumn]  FROM [dbo].[SUTI_AP_GP_ENMATCHINV_Header_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "' and [Value Type] = 'Source'";
            // query = "SELECT Distinct [ValueorColumn]  FROM [dbo].[SUTI_AP_GP_POP_Header_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [POPMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLSHIPEXPORTHeaderMappingColumns()
        {
            query = " select   'POPRCTNM'  ValueorColumn ";
            return query;
        }

        public static string SQLPOEXPORTHeaderMappingColumns()
        {
            query = " select   'PONUMBER'  ValueorColumn ";
            return query;
        }

        public static string SQLSHIPMENTTABLENAMES()
        {
            //query = "select * from [tbl_Posted_Shipment] WHERE EXPORTSTATUS = 0 ";
            query = " EXEC [sp_Get_Posted_Shipment] ";
            return query;
        }

        public static string SQLPURCHASEORDERTABLENAMES()
        {
            //query = "select * from [tbl_Posted_Shipment] WHERE EXPORTSTATUS = 0 ";
            query = " EXEC [sp_Get_PO_Details] ";
            return query;
        }

        public static string SQLInvoicPaymenteDistributionMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_Integration_Dist_Type_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [INVOICEMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLInvoicPaymenteTaxMapping(string strMapID, string strCompanyID)
        {
            query = "SELECT [eConnect Field],[Value Type],[ValueorColumn]  FROM [dbo].[SUTI_AP_GP_Integration_Tax_Mapping_Table]  where [COMPANYID] = '" + strCompanyID + "' and [INVOICEMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLIntercompanyTempAccount(string strMapID, string strCompanyID)
        {
            query = "SELECT isnull(ICTEMPACCOUNT,'')  FROM [dbo].[SUTI_AP_GP_Integration_Setup]  where [GPINTERID] = '" + strCompanyID + "' and [INVOICEMAPID] = '" + strMapID + "'";
            return query;
        }

        public static string SQLGetAccountIndex(string strAccountString, string strCompanyID)
        {
            query = "SELECT isnull(ACTINDX,0)  FROM [" + strCompanyID + "]..[GL00105]  where [ACTNUMST] = '" + strAccountString + "'";
            return query;
        }

        public static string SQLSMTPConfigdetails()
        {
            query = "select [SFTPHOSTNAME],[SFTPPORTNUMBER],[SFTPUSERNAME],[SFTPPASSWORD],[TargetName] from [dbo].[SUTI_AP_GP_Integration_Mail_Setup]";
            return query;
        }
    }
}
