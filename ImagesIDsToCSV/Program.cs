using System;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Dapper;

namespace ImageProcessorService
{
    public class Program
    {
        static async Task<int> Main()
        {
            string filePath = @"C:\Development\Utils\ImagesIDsToCSV\ImageIDs.txt"; // Hardcoded input file path
            string dbConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";
            int chunkSize = 1000; // Number of IDs per batch

            string tableName = "tblLSR855_ProcessedImageIDs";

            try
            {
                using (IDbConnection db = new SqlConnection(dbConnectionString))
                {
                    await foreach (var imageIdChunk in ReadImageIdsInChunksAsync(filePath, chunkSize))
                    {
                        if (imageIdChunk.Any())
                        {
                            var sql = $@"
                                INSERT INTO {tableName} (ImageID)
                                SELECT ImageID FROM tblImages WHERE ImageID IN @ImageIds
                            ";

                            int rowsInserted = await db.ExecuteAsync(sql, new { ImageIds = imageIdChunk });

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

        private static async IAsyncEnumerable<List<int>> ReadImageIdsInChunksAsync(string filePath, int chunkSize)
        {
            var imageIds = new List<int>();

            using (var reader = new StreamReader(filePath))
            {
                string line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (int.TryParse(line, out int imageId))
                    {
                        imageIds.Add(imageId);

                        if (imageIds.Count == chunkSize)
                        {
                            yield return imageIds;
                            imageIds = new List<int>();
                        }
                    }
                }

                // Return remaining IDs if any
                if (imageIds.Any())
                {
                    yield return imageIds;
                }
            }
        }
    }
}
