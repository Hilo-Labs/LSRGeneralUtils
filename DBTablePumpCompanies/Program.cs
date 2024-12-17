using System;
using System.Data;
using System.Data.SqlClient;

namespace DataTransferService
{
    class Program
    {
        static int Main(string[] args)
        {
            string sourceConnectionString = "Data Source=thinker;Initial Catalog=LSRProd;Persist Security Info=True;Integrated Security=false;User ID=hiloUser;Password=Hawaii;MultipleActiveResultSets=True";
            string destinationConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";

            int companyIdToCopy = 272;

            try
            {
                using (IDbConnection sourceDb = new SqlConnection(sourceConnectionString))
                using (IDbConnection destinationDb = new SqlConnection(destinationConnectionString))
                {
                    sourceDb.Open();
                    destinationDb.Open();

                    CopyCompanyRow(sourceDb, destinationDb, companyIdToCopy);
                }

                Console.WriteLine("Company row restored successfully.");
                return 0;
            }
            catch (Exception err)
            {
                Console.WriteLine("An unexpected error occurred: " + err.Message);
                return -1;
            }
        }

        static void CopyCompanyRow(IDbConnection sourceDb, IDbConnection destinationDb, int companyId)
        {
            string tableName = "tblCompanies";

            DataTable companyRow = new DataTable();
            using (var sourceCommand = sourceDb.CreateCommand())
            {
                sourceCommand.CommandText = $"SELECT * FROM {tableName} WHERE companyID = @companyId";
                var param = sourceCommand.CreateParameter();
                param.ParameterName = "@companyId";
                param.Value = companyId;
                sourceCommand.Parameters.Add(param);

                using (var reader = sourceCommand.ExecuteReader())
                {
                    companyRow.Load(reader);
                }
            }

            if (companyRow.Rows.Count == 0)
            {
                throw new Exception($"No company found with companyID = {companyId}.");
            }

            using (var transaction = destinationDb.BeginTransaction())
            {
                try
                {
                    using (var destinationCommand = destinationDb.CreateCommand())
                    {
                        destinationCommand.Transaction = transaction;

                        destinationCommand.CommandText = $"SET IDENTITY_INSERT {tableName} ON";
                        destinationCommand.ExecuteNonQuery();

                        destinationCommand.CommandText = BuildInsertCommand(tableName, companyRow);
                        foreach (DataColumn column in companyRow.Columns)
                        {
                            destinationCommand.Parameters.Add(new SqlParameter($"@{column.ColumnName}", companyRow.Rows[0][column] ?? DBNull.Value));
                        }

                        destinationCommand.ExecuteNonQuery();

                        destinationCommand.CommandText = $"SET IDENTITY_INSERT {tableName} OFF";
                        destinationCommand.ExecuteNonQuery();
                    }

                    transaction.Commit();
                    Console.WriteLine($"Company with companyID = {companyId} restored successfully.");
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }

        static string BuildInsertCommand(string tableName, DataTable data)
        {
            var columns = string.Join(", ", data.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
            var parameters = string.Join(", ", data.Columns.Cast<DataColumn>().Select(c => "@" + c.ColumnName));
            return $"INSERT INTO {tableName} ({columns}) VALUES ({parameters})";
        }
    }
}
