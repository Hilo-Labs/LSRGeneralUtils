using System;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Dapper;
using CsvHelper;
using System.Globalization;

namespace ImageProcessorService
{
    public class Program
    {
        static async Task<int> Main()
        {
            string filePath = @"C:\Development\Utils\ImagesIDsToCSV\ImageIDs.txt"; // Hardcoded input file path
            string dbConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";
            string outputCsvPath = @"C:\Development\Utils\ImagesIDsToCSV\output.csv"; // Hardcoded output CSV file path
            int chunkSize = 1000; // Number of IDs per batch

            try
            {
                using (IDbConnection db = new SqlConnection(dbConnectionString))
                using (var writer = new StreamWriter(outputCsvPath))
                using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteHeader<ImageRecord>();
                    csv.NextRecord();

                    await foreach (var imageIdChunk in ReadImageIdsInChunksAsync(filePath, chunkSize))
                    {
                        if (imageIdChunk.Any())
                        {
                            var sql = $@"
                                SELECT imageid, companyid, Deleted, Done, Sharelevel
                                FROM tblImages
                                WHERE Deleted <> 1 and sharelevel < 4 and imageid IN @ImageIds
                            ";

                            var imageRecords = (await db.QueryAsync<ImageRecord>(sql, new { ImageIds = imageIdChunk })).ToList();

                            csv.WriteRecords(imageRecords);
                        }
                    }
                }

                Console.WriteLine($"Data successfully written to {outputCsvPath}");
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

    public class ImageRecord
    {
        public int ImageId { get; set; }
        public int CompanyId { get; set; }
        //public bool Deleted { get; set; }
        //public bool Done { get; set; }
        public int ShareLevel { get; set; }
    }
}
