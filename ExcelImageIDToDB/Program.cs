using System.Data;
using System.Data.SqlClient;
using Dapper;
using OfficeOpenXml;


namespace ExcelImageIDToDB
{
    /*
CREATE TABLE tblInvalidImages (
    ImageID INT PRIMARY KEY
);
    */

    public class Program
    {
        static async Task<int> Main()
        {
            string excelFilePath = @"C:\Development\Utils\ExcelImageIDToDB\SystemInvalidRecords 2024-11-07.xlsx"; 
            string dbConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";
            int chunkSize = 1000; // Number of IDs per batch

            string tableName = "tblInvalidImages";

            try
            {
                // Initialize EPPlus license context
                ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

                using (IDbConnection db = new SqlConnection(dbConnectionString))
                {
                    await foreach (var imageIdChunk in ReadImageIdsFromExcelInChunksAsync(excelFilePath, chunkSize))
                    {
                        if (imageIdChunk.Any())
                        {
                            var sql = $@"
                                INSERT INTO {tableName} (ImageID)
                                VALUES (@ImageID)
                            ";

                            int rowsInserted = await db.ExecuteAsync(sql, imageIdChunk.Select(id => new { ImageID = id }));

                            Console.WriteLine($"{rowsInserted} records inserted for current batch.");
                        }
                    }
                }

                Console.WriteLine($"Data successfully inserted into {tableName}.");
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred during processing:");
                Console.WriteLine(ex.Message);
                return 1;
            }
        }

        private static async IAsyncEnumerable<List<int>> ReadImageIdsFromExcelInChunksAsync(string excelFilePath, int chunkSize)
        {
            var imageIds = new List<int>();

            using (var package = new ExcelPackage(new FileInfo(excelFilePath)))
            {
                var worksheet = package.Workbook.Worksheets[0]; 

                int totalRows = worksheet.Dimension.End.Row;
                int totalColumns = worksheet.Dimension.End.Column;

                int columnIdIndex = -1;

                // Assuming the first row contains headers
                for (int col = 1; col <= totalColumns; col++)
                {
                    var headerText = worksheet.Cells[1, col].Text;
                    if (headerText.Equals("Original ID", StringComparison.OrdinalIgnoreCase))
                    {
                        columnIdIndex = col;
                        break;
                    }
                }

                if (columnIdIndex == -1)
                {
                    throw new Exception("Column 'Original ID' not found in Excel file.");
                }

                // Read data starting from the second row (assuming first row is headers)
                for (int row = 2; row <= totalRows; row++)
                {
                    var cellValue = worksheet.Cells[row, columnIdIndex].Text;

                    if (int.TryParse(cellValue, out int imageId))
                    {
                        imageIds.Add(imageId);

                        if (imageIds.Count == chunkSize)
                        {
                            yield return new List<int>(imageIds);
                            imageIds.Clear();
                        }
                    }
                }

                // Return remaining IDs if any
                if (imageIds.Any())
                {
                    yield return imageIds;
                }
            }

            await Task.CompletedTask;
        }
    }
}
