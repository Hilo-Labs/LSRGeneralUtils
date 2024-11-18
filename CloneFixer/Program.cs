using System;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using Dapper;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Threading.Tasks;
using System.Linq;
using Azure;
using System.Collections.Generic;

namespace ImageProcessorService
{
    class Program
    {
        //const string dbConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";
        const string dbConnectionString = "Data Source=thinker;Initial Catalog=LSRProd;Persist Security Info=True;Integrated Security=false;User ID=hiloUser;Password=Hawaii;MultipleActiveResultSets=True";

        const string storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=landsurveyrecords;AccountKey=daDBzSf4cY+bQmlDPWWvbWpKcqXcSh31R9PHpngwstppwsUV3wsTvt144WMlz+SOO9KjkZE0Xpg3DgRwiLGLDQ==;EndpointSuffix=core.windows.net";
        const string imageBlobName = "images-v3";

        static async Task<int> Main(string[] args)
        {
            try
            {
                Log("Initializing settings...");
                Log("Starting image copy process...");
                await CopyImagesAsync(dbConnectionString, storageAccountConnectionString);
                Log("Image copy process complete.");
                return 0;
            }
            catch (Exception ex)
            {
                Log("An error occurred during processing:");
                Log(ex.Message);
                return 1;
            }
        }

        static async Task CopyImagesAsync(string dbConnectionString, string storageAccountConnectionString)
        {
            Log("Initializing Blob Service Client...");
            BlobServiceClient blobServiceClient = new BlobServiceClient(storageAccountConnectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(imageBlobName);

            using (IDbConnection db = new SqlConnection(dbConnectionString))
            {
                Log("Fetching Source and Target ImageIDs to process...");

                var imagePairs = await db.QueryAsync<(int Target, int Source)>(
                    @"SELECT * FROM tblLSR886Fixer"
                );

                Log($"Found {imagePairs.Count()} image pairs to process.");

                foreach (var pair in imagePairs)
                {
                    int targetImageId = pair.Target;
                    int sourceImageId = pair.Source;

                    try
                    {
                        string sourcePrefix = $"{sourceImageId}/";
                        var blobs = blobContainerClient.GetBlobsAsync(prefix: sourcePrefix);

                        await foreach (var blobItem in blobs)
                        {
                            string sourceBlobName = blobItem.Name;
                            string fileName = sourceBlobName.Substring(sourcePrefix.Length);
                            string targetBlobName = $"{targetImageId}/{fileName}";

                            BlobClient sourceBlobClient = blobContainerClient.GetBlobClient(sourceBlobName);
                            BlobClient targetBlobClient = blobContainerClient.GetBlobClient(targetBlobName);

                            // Check if the target blob already exists
                            bool targetBlobExists = await targetBlobClient.ExistsAsync();

                            if (targetBlobExists)
                            {
                                Log($"Target blob {targetBlobName} already exists. Skipping copy.");
                                continue; // Skip to the next blob
                            }

                            try
                            {
                                // Start the copy operation
                                await targetBlobClient.StartCopyFromUriAsync(sourceBlobClient.Uri);

                                BlobProperties properties = await targetBlobClient.GetPropertiesAsync();

                                while (properties.CopyStatus == CopyStatus.Pending)
                                {
                                    await Task.Delay(500);
                                    properties = await targetBlobClient.GetPropertiesAsync();
                                }

                                if (properties.CopyStatus == CopyStatus.Success)
                                {
                                    Log($"Successfully copied {sourceBlobName} to {targetBlobName}");
                                }
                                else
                                {
                                    Log($"Failed to copy {sourceBlobName} to {targetBlobName}: {properties.CopyStatusDescription}");
                                }
                            }
                            catch (Exception ex)
                            {
                                Log($"Error copying blob {sourceBlobName} to {targetBlobName}: {ex.Message}");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Log($"Error copying from Source ImageID {sourceImageId} to Target ImageID {targetImageId}: {ex.Message}");
                    }
                }
            }
        }

        public static void Log(string message)
        {
            Console.WriteLine(message);
        }
    }
}
