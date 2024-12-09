using System.Data;
using System.Data.SqlClient;
using Dapper;
using OfficeOpenXml;
using System.IO;  // for file writing

namespace ExcelCompanyReversal
{
    public class Program
    {
        const string appPath = @"C:\Development\Utils\ExcelCompanyReversal\";

        static string excelFilePath = appPath + "Company reversal - phase II.xlsx";
        static string notChangedLogPath = appPath + "notChanged.log";
        static string notUpdatedLogPath = appPath + "notUpdated.log";

        //static string dbConnectionString = "Data Source=thinker;Initial Catalog=LSRProd;Persist Security Info=True;Integrated Security=false;User ID=hiloUser;Password=Hawaii;MultipleActiveResultSets=True";
        static string dbConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";

        static string tableName = "tblImages";
        static int chunkSize = 1000;

        public static async Task<int> Main()
        {
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

            //var updateResult = await UpdateCompaniesAsync();
            //if (updateResult != 0)
            //{
            //    Console.WriteLine("Updates encountered an error.");
            //    return updateResult;
            //}

            // Call the verify function
            var verifyResult = await VerifyUpdatesAsync();
            if (verifyResult != 0)
            {
                Console.WriteLine("Verification encountered an error.");
                return verifyResult;
            }

            Console.WriteLine("All processes completed successfully.");
            return 0;
        }

        private static async Task<int> UpdateCompaniesAsync()
        {
            try
            {
                using (var logWriter = new StreamWriter(notChangedLogPath, append: true))
                using (IDbConnection db = new SqlConnection(dbConnectionString))
                {
                    await foreach (var recordsChunk in ReadRecordsFromExcelInChunksAsync(excelFilePath, chunkSize))
                    {
                        foreach (var record in recordsChunk)
                        {
                            var currentCompanyId = await db.QueryFirstOrDefaultAsync<int?>(
                                $"SELECT CompanyID FROM {tableName} WHERE ImageID = @ImageID",
                                new { ImageID = record.ImageID }
                            );

                            if (currentCompanyId == null)
                            {
                                Console.WriteLine($"ImageID {record.ImageID} not found in {tableName}.");
                                await logWriter.WriteLineAsync($"{record.ImageID}, not found");
                                continue;
                            }

                            if (currentCompanyId.Value != record.FromCompanyId)
                            {
                                Console.WriteLine($"ImageID {record.ImageID} skipped because current CompanyID ({currentCompanyId.Value}) does not match from_company_id ({record.FromCompanyId}).");
                                await logWriter.WriteLineAsync($"{record.ImageID}, current={currentCompanyId.Value}, expected={record.FromCompanyId}");
                                continue;
                            }

                            var rowsAffected = await db.ExecuteAsync(
                                $"UPDATE {tableName} SET CompanyID = @ToCompanyId WHERE ImageID = @ImageID",
                                new { ImageID = record.ImageID, ToCompanyId = record.ToCompanyId }
                            );

                            Console.WriteLine($"ImageID {record.ImageID} updated. Rows affected: {rowsAffected}");
                        }
                    }
                }

                Console.WriteLine($"Update process completed.");
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred during the update process:");
                Console.WriteLine(ex.Message);
                return 1;
            }
        }
        private static async Task<int> VerifyUpdatesAsync()
        {
            try
            {
                using (var logWriter = new StreamWriter(notUpdatedLogPath, append: true))
                using (IDbConnection db = new SqlConnection(dbConnectionString))
                {
                    await foreach (var recordsChunk in ReadRecordsFromExcelInChunksAsync(excelFilePath, chunkSize))
                    {
                        foreach (var record in recordsChunk)
                        {
                            var currentCompanyId = await db.QueryFirstOrDefaultAsync<int?>(
                                $"SELECT CompanyID FROM {tableName} WHERE ImageID = @ImageID",
                                new { ImageID = record.ImageID }
                            );

                            if (currentCompanyId == null)
                            {
                                Console.WriteLine($"Verification: ImageID {record.ImageID} not found in {tableName}.");
                                await logWriter.WriteLineAsync($"{record.ImageID}, not found in verification");
                                continue;
                            }

                            if (currentCompanyId.Value != record.ToCompanyId)
                            {
                                Console.WriteLine($"Verification: ImageID {record.ImageID} has CompanyID={currentCompanyId.Value} but expected {record.ToCompanyId}.");
                                await logWriter.WriteLineAsync($"{record.ImageID}, current={currentCompanyId.Value}, expected={record.ToCompanyId}");
                            }
                            else
                            {
                                Console.WriteLine($"Verification: ImageID {record.ImageID} CompanyID correctly set to {currentCompanyId.Value}.");
                            }
                        }
                    }
                }

                Console.WriteLine($"Verification process completed.");
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred during the verification process:");
                Console.WriteLine(ex.Message);
                return 1;
            }
        }

        private static async IAsyncEnumerable<List<Record>> ReadRecordsFromExcelInChunksAsync(string excelFilePath, int chunkSize)
        {
            var records = new List<Record>();

            using (var package = new ExcelPackage(new FileInfo(excelFilePath)))
            {
                var worksheet = package.Workbook.Worksheets[0];

                int totalRows = worksheet.Dimension.End.Row;
                int totalColumns = worksheet.Dimension.End.Column;

                int imageIdColIndex = -1;
                int fromCompanyIdColIndex = -1;
                int toCompanyIdColIndex = -1;

                // Find headers
                for (int col = 1; col <= totalColumns; col++)
                {
                    var headerText = worksheet.Cells[1, col].Text.Trim();
                    if (headerText.Equals("imageid", StringComparison.OrdinalIgnoreCase))
                        imageIdColIndex = col;
                    else if (headerText.Equals("from_company_id", StringComparison.OrdinalIgnoreCase))
                        fromCompanyIdColIndex = col;
                    else if (headerText.Equals("to_company_id", StringComparison.OrdinalIgnoreCase))
                        toCompanyIdColIndex = col;
                }

                if (imageIdColIndex == -1 || fromCompanyIdColIndex == -1 || toCompanyIdColIndex == -1)
                {
                    throw new Exception("Required columns (imageid, from_company_id, to_company_id) not found in Excel file.");
                }

                // Read data starting from the second row
                for (int row = 2; row <= totalRows; row++)
                {
                    var imageIdValue = worksheet.Cells[row, imageIdColIndex].Text;
                    var fromCompanyIdValue = worksheet.Cells[row, fromCompanyIdColIndex].Text;
                    var toCompanyIdValue = worksheet.Cells[row, toCompanyIdColIndex].Text;

                    if (int.TryParse(imageIdValue, out int imageId) &&
                        int.TryParse(fromCompanyIdValue, out int fromCompanyId) &&
                        int.TryParse(toCompanyIdValue, out int toCompanyId))
                    {
                        records.Add(new Record
                        {
                            ImageID = imageId,
                            FromCompanyId = fromCompanyId,
                            ToCompanyId = toCompanyId
                        });

                        if (records.Count == chunkSize)
                        {
                            yield return new List<Record>(records);
                            records.Clear();
                        }
                    }
                }

                // Yield remaining records if any
                if (records.Count > 0)
                {
                    yield return records;
                }
            }

            await Task.CompletedTask;
        }

        private class Record
        {
            public int ImageID { get; set; }
            public int FromCompanyId { get; set; }
            public int ToCompanyId { get; set; }
        }
    }
}
