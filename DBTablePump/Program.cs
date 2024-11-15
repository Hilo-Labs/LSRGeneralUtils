using System;
using System.Data;
using System.Data.SqlClient;

namespace DataTransferService
{
    class Program
    {
        static int Main(string[] args)
        {
            string sourceConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";
            //string destinationConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSRDevelopment;Persist Security Info=True;Integrated Security=false;User ID=web_app_login@lsr;Password=IUOPYjhf(*2390FOIJHjndjSsuwqp!!oI{MNS7yds7;MultipleActiveResultSets=True";
            string destinationConnectionString = "Data Source=thinker;Initial Catalog=LSRProd;Persist Security Info=True;Integrated Security=false;User ID=hiloUser;Password=Hawaii;MultipleActiveResultSets=True";
            string tableName = "tblNavigationLogs";

            try
            {
                using (IDbConnection sourceDb = new SqlConnection(sourceConnectionString))
                using (IDbConnection destinationDb = new SqlConnection(destinationConnectionString))
                {
                    sourceDb.Open();
                    destinationDb.Open();

                    PurgeDestinationTable(destinationDb, tableName);
                    CopyTableDataInBatches(sourceDb, destinationDb, tableName);
                }

                Console.WriteLine("Data transfer process complete.");
                return 0;
            }
            catch (Exception err)
            {
                Console.WriteLine("An unexpected error occurred: " + err.Message);
                return -1;
            }
        }

        static void PurgeDestinationTable(IDbConnection destinationDb, string tableName)
        {
            using (var command = destinationDb.CreateCommand())
            {
                command.CommandText = $"DELETE FROM {tableName}";
                command.ExecuteNonQuery();
            }
            Console.WriteLine($"Purged destination table: {tableName}");
        }

        static void CopyTableDataInBatches(IDbConnection sourceDb, IDbConnection destinationDb, string tableName)
        {
            int batchSize = 1000;
            int offset = 0;

            while (true)
            {
                using (var sourceCommand = sourceDb.CreateCommand())
                {
                    sourceCommand.CommandText = $"SELECT * FROM {tableName} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {batchSize} ROWS ONLY";
                    using (var reader = sourceCommand.ExecuteReader())
                    {
                        if (!(reader is SqlDataReader sqlReader) || !sqlReader.HasRows)
                            break;

                        using (var bulkCopy = new SqlBulkCopy((SqlConnection)destinationDb))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BatchSize = batchSize;
                            bulkCopy.WriteToServer(reader);
                        }
                    }
                }

                offset += batchSize;
                Console.WriteLine($"Copied {batchSize} rows to {tableName} (offset: {offset})");
            }
        }
    }
}
