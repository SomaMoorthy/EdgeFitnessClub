using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace SUTIAPGPIntegrationService.Utilities
{
    class DBLibrary
    {
        public static DataSet GetDataSetFromSQLSCript(string sqlQuery, string dsTableName, string connectionString)
        {
            //local variables
            SqlDataAdapter sqlAdap = default(SqlDataAdapter);
            DataSet dataSet = default(DataSet);

            //set the connection string to the connection object
            SqlConnection sqlConn = new SqlConnection(connectionString);

            try
            {
                dataSet = new DataSet();

                //Open SQL Connection
                sqlConn.Open();
                SqlCommand sqlCommand = sqlConn.CreateCommand();

                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.CommandText = sqlQuery;

                //retrieve the record set with data adapters
                sqlAdap = new SqlDataAdapter(sqlCommand);
                sqlAdap.Fill(dataSet, dsTableName);

                //return the dataset
                return dataSet;
            }
            catch (SqlException ex)
            {
                Utilities.EventLogger.WriteToEventLog(("!>>> Exception: " + ex.Message), EventLogEntryType.Error);
                throw ex;
            }
            finally
            {
                sqlConn.Close();
            }
        }

        public static DataTable GetDataTableFromSQLSCript(string sqlQuery,  string connectionString)
        {
            //local variables
            SqlDataAdapter sqlAdap = default(SqlDataAdapter);
            //DataSet dataSet = default(DataSet);
            DataTable dataTable = default(DataTable);
            //set the connection string to the connection object
            SqlConnection sqlConn = new SqlConnection(connectionString);

            try
            {
                dataTable = new DataTable();

                //Open SQL Connection
                sqlConn.Open();
                SqlCommand sqlCommand = sqlConn.CreateCommand();

                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.CommandText = sqlQuery;

                //retrieve the record set with data adapters
                sqlAdap = new SqlDataAdapter(sqlCommand);
                sqlAdap.Fill(dataTable);

                //return the dataset
                return dataTable;
            }
            catch (SqlException ex)
            {
                Utilities.EventLogger.WriteToEventLog(("!>>> Exception: " + ex.Message), EventLogEntryType.Error);
                throw ex;
            }
            finally
            {
                sqlConn.Close();
            }
        }

        public static string SQLGetFormattedAccountString(string spName, string _connectionString, string sInputValue)
        {
            SqlConnection sqlConn = new SqlConnection();

            try
            {
                string connectionString = _connectionString; //ConfigurationSettings.AppSettings["SQLDBConnectionString"].ToString();
                sqlConn = new SqlConnection(connectionString);
                SqlCommand sqlComm = new SqlCommand();

                sqlComm.Connection = sqlConn;
                sqlComm.Connection.Open();
                sqlComm.CommandText = spName;
                sqlComm.CommandType = CommandType.StoredProcedure;
                sqlComm.Parameters.AddWithValue("@ipAccountNumber", sInputValue);


                sqlComm.Parameters.Add("@opAccountNumber", SqlDbType.VarChar, 100);
                sqlComm.Parameters["@opAccountNumber"].Direction = ParameterDirection.Output;

                sqlComm.ExecuteNonQuery();
                 
                        //sqlComm.Connection.Close();
                        return Convert.ToString(sqlComm.Parameters["@opAccountNumber"].Value); 
                   
               
            }
            catch (SqlException ex)
            {
                Utilities.EventLogger.WriteToEventLog(("!>>> Exception: " + ex.Message), EventLogEntryType.Error);
                throw ex;
            }
            finally
            {
                sqlConn.Close();
            }
        }
        public static string SQLValidateAccountString(string spName, string _connectionString, string sInputValue)
        {
            SqlConnection sqlConn = new SqlConnection();

            try
            {
                string connectionString = _connectionString; //ConfigurationSettings.AppSettings["SQLDBConnectionString"].ToString();
                sqlConn = new SqlConnection(connectionString);
                SqlCommand sqlComm = new SqlCommand();

                sqlComm.Connection = sqlConn;
                sqlComm.Connection.Open();
                sqlComm.CommandText = spName;
                sqlComm.CommandType = CommandType.StoredProcedure;
                sqlComm.Parameters.AddWithValue("@ipAccountNumber", sInputValue);


                sqlComm.Parameters.Add("@opAccountNumber", SqlDbType.VarChar, 100);
                sqlComm.Parameters["@opAccountNumber"].Direction = ParameterDirection.Output;

                sqlComm.ExecuteNonQuery();

                string actIndx;
                actIndx = Convert.ToString(sqlComm.Parameters["@opAccountNumber"].Value);
                if (actIndx != "0")
                {
                    return actIndx;
                }
                else
                {
                    throw new Exception("Account Number "+ sInputValue + " does not exists.");
                }
                 


            }
            catch (SqlException ex)
            {
                Utilities.EventLogger.WriteToEventLog(("!>>> Exception: " + ex.Message), EventLogEntryType.Error);
                throw ex;
            }
            finally
            {
                sqlConn.Close();
            }
        }

        public static string SQLUpdateRecvDistribution(string spName, string _connectionString, string sRctNumber, string apAccIndx, string miscAccIndx, string frgtAccIndx, string trdisAccIndx)
        {
            SqlConnection sqlConn = new SqlConnection();

            try
            {
                string connectionString = _connectionString; //ConfigurationSettings.AppSettings["SQLDBConnectionString"].ToString();
                sqlConn = new SqlConnection(connectionString);
                SqlCommand sqlComm = new SqlCommand();

                sqlComm.Connection = sqlConn;
                sqlComm.Connection.Open();
                sqlComm.CommandText = spName;
                sqlComm.CommandType = CommandType.StoredProcedure;
                sqlComm.Parameters.AddWithValue("@isRctNumber", sRctNumber);
                sqlComm.Parameters.AddWithValue("@iapAccIndx", apAccIndx);
                sqlComm.Parameters.AddWithValue("@imiscAccIndx", miscAccIndx);
                sqlComm.Parameters.AddWithValue("@ifrgtAccIndx", frgtAccIndx);
                sqlComm.Parameters.AddWithValue("@itrdisAccIndx", trdisAccIndx);


                sqlComm.Parameters.Add("@oUpdErrFlag", SqlDbType.VarChar,100);
                sqlComm.Parameters["@oUpdErrFlag"].Direction = ParameterDirection.Output;

                sqlComm.ExecuteNonQuery();

                string updatErrorFlag;
                updatErrorFlag =  Convert.ToString(sqlComm.Parameters["@oUpdErrFlag"].Value);

                if (updatErrorFlag != "1")
                {
                    return updatErrorFlag;
                }
                else
                {
                    throw new Exception("Receiving Transaction Integrated with Distribution Account Error");
                }

        
            }
            catch (SqlException ex)
            {
                Utilities.EventLogger.WriteToEventLog(("!>>> Exception: " + ex.Message), EventLogEntryType.Error);
                throw ex;
            }
            finally
            {
                sqlConn.Close();
            }
        }

        public static string SQLUpdateExportedRecords(string spName, string _connectionString, Boolean isSingleUpdate,int exportType,int docType, string docNumber)
        {
            SqlConnection sqlConn = new SqlConnection();

            try
            {
                string connectionString = _connectionString; //ConfigurationSettings.AppSettings["SQLDBConnectionString"].ToString();
                sqlConn = new SqlConnection(connectionString);
                SqlCommand sqlComm = new SqlCommand();

                sqlComm.Connection = sqlConn;
                sqlComm.Connection.Open();
                sqlComm.CommandText = spName;
                sqlComm.CommandType = CommandType.StoredProcedure;
                sqlComm.Parameters.AddWithValue("@isSingleUpdate", isSingleUpdate);
                sqlComm.Parameters.AddWithValue("@exportType", exportType);
                sqlComm.Parameters.AddWithValue("@docType", docType);
                sqlComm.Parameters.AddWithValue("@docNumber", docNumber);

                sqlComm.ExecuteNonQuery();

                return "";

            }
            catch (SqlException ex)
            {
                Utilities.EventLogger.WriteToEventLog(("!>>> Exception: " + ex.Message), EventLogEntryType.Error);
                throw ex;
            }
            finally
            {
                sqlConn.Close();
            }
        }

        public static string SQLGetNextPaymentNumber(string spName, string _connectionString)
        {
            SqlConnection sqlConn = new SqlConnection();

            try
            {
                string connectionString = _connectionString; //ConfigurationSettings.AppSettings["SQLDBConnectionString"].ToString();
                sqlConn = new SqlConnection(connectionString);
                SqlCommand sqlComm = new SqlCommand();

                sqlComm.Connection = sqlConn;
                sqlComm.Connection.Open();
                sqlComm.CommandText = spName;
                sqlComm.CommandType = CommandType.StoredProcedure;
                sqlComm.Parameters.AddWithValue("@I_vInc_Dec", 1);


                sqlComm.Parameters.Add("@O_iPMNPYNBR", SqlDbType.VarChar, 100);
                sqlComm.Parameters["@O_iPMNPYNBR"].Direction = ParameterDirection.Output;

                sqlComm.Parameters.Add("@O_iErrorState", SqlDbType.Int);
                sqlComm.Parameters["@O_iErrorState"].Direction = ParameterDirection.Output;



                sqlComm.ExecuteNonQuery();

                //sqlComm.Connection.Close();
                if (Convert.ToInt32(sqlComm.Parameters["@O_iErrorState"].Value) == 0)
                {
                    return Convert.ToString(sqlComm.Parameters["@O_iPMNPYNBR"].Value);
                }
                else
                {
                    return "";
                }


            }
            catch (SqlException ex)
            {
                Utilities.EventLogger.WriteToEventLog(("!>>> Exception: " + ex.Message), EventLogEntryType.Error);
                throw ex;
            }
            finally
            {
                sqlConn.Close();
            }
        }

        public static string GetScalarValue(string sqlQuery, string _connectionString)
        {
            SqlConnection sqlConn = new SqlConnection();

            try
            {
                string connectionString = _connectionString; //ConfigurationSettings.AppSettings["SQLDBConnectionString"].ToString();
                sqlConn = new SqlConnection(connectionString);
                SqlCommand sqlComm = new SqlCommand();

                sqlComm.Connection = sqlConn;
                sqlComm.Connection.Open();
                sqlComm.CommandText = sqlQuery;

                if (sqlComm.ExecuteScalar() == System.DBNull.Value)
                {
                    //sqlComm.Connection.Close();
                    return "0";
                }
                else
                {
                    if (sqlComm.ExecuteScalar() != null)
                    {
                        //sqlComm.Connection.Close();
                        return sqlComm.ExecuteScalar().ToString();
                    }
                    else
                    {
                        //sqlComm.Connection.Close();
                        return "0";
                    }
                }
            }
            catch (SqlException ex)
            {
                Utilities.EventLogger.WriteToEventLog(("!>>> Exception: " + ex.Message), EventLogEntryType.Error);
                throw ex;
            }
            finally
            {
                sqlConn.Close();
            }
        }

    }
}
