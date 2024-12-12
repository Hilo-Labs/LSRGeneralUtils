using System;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using Dapper;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Linq;
using System.Collections.Generic;

namespace ImageProcessorService
{
    class Program
    {
        const string dbConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";
        const string storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=landsurveyrecords;AccountKey=daDBzSf4cY+bQmlDPWWvbWpKcqXcSh31R9PHpngwstppwsUV3wsTvt144WMlz+SOO9KjkZE0Xpg3DgRwiLGLDQ==;EndpointSuffix=core.windows.net";
        const string imageBlobName = "images-v3";
        static StreamWriter logWriter;

        static async Task<int> Main(string[] args)
        {
            string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
            string logFolderName = "Log_" + DateTime.Now.ToString("yyyyMMddHHmmss");
            string logFolderPath = Path.Combine(baseDirectory, logFolderName);
            Directory.CreateDirectory(logFolderPath);
            string logFilePath = Path.Combine(logFolderPath, "log.txt");

            using (logWriter = new StreamWriter(logFilePath, append: true) { AutoFlush = true })
            {
                try
                {
                    Log("Starting process to copy medium.gif to high.gif for images with Status = 1100...");
                    await OverwriteHighGifAsync(dbConnectionString, storageAccountConnectionString);
                    Log("Process complete.");
                    return 0;
                }
                catch (Exception ex)
                {
                    Log("An error occurred during processing:");
                    Log(ex.Message);
                    return 1;
                }
            }
        }

        static async Task OverwriteHighGifAsync(string dbConnectionString, string storageAccountConnectionString)
        {
            Log("Initializing Blob Service Client...");
            BlobServiceClient blobServiceClient = new BlobServiceClient(storageAccountConnectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(imageBlobName);

            using (IDbConnection db = new SqlConnection(dbConnectionString))
            {
                Log("Querying database for images with Status = 1100...");
                var imageIds = (await db.QueryAsync<int>(
                    "SELECT ImageID FROM tblMetaUpdated WHERE Status = 1100"
                )).ToList();

                if (!imageIds.Any())
                {
                    Log("No images with Status = 1100 found.");
                    return;
                }

                Log($"Found {imageIds.Count} images with Status = 1100.");

                foreach (var imageId in imageIds)
                {
                    try
                    {
                        Log($"Processing ImageID: {imageId}");

                        var mediumGifBlobClient = blobContainerClient.GetBlobClient($"{imageId}/medium.gif");
                        var highGifBlobClient = blobContainerClient.GetBlobClient($"{imageId}/high.gif");

                        if (!await mediumGifBlobClient.ExistsAsync())
                        {
                            throw new FileNotFoundException($"medium.gif does not exist for ImageID {imageId}");
                        }

                        Log("Downloading medium.gif to memory...");
                        MemoryStream memoryStream = new MemoryStream();
                        await mediumGifBlobClient.DownloadToAsync(memoryStream);
                        memoryStream.Position = 0;

                        Log("Uploading memory stream to high.gif, overwriting existing...");
                        await highGifBlobClient.UploadAsync(memoryStream, overwrite: true);
                        Log($"Successfully overwrote high.gif for ImageID {imageId}");
                    }
                    catch (Exception ex)
                    {
                        Log($"Error processing ImageID {imageId}: {ex.Message}");
                        // You can decide if you want to update DB status here or just log the error.
                        // For now, we just log the error and continue.
                    }
                    Log("=============================================");
                }
            }
        }

        public static void Log(string message)
        {
            var logMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}";
            Console.WriteLine(logMessage);
            logWriter.WriteLine(logMessage);
        }
    }
}
