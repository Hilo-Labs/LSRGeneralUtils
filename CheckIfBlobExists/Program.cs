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
                Log("Starting image existence check process...");
                await CheckImageExistsAsync(dbConnectionString, storageAccountConnectionString);
                Log("Image existence check process complete.");
                return 0;
            }
            catch (Exception ex)
            {
                Log("An error occurred during processing:");
                Log(ex.Message);
                return 1;
            }
        }

        static async Task CheckImageExistsAsync(string dbConnectionString, string storageAccountConnectionString)
        {
            Log("Initializing Blob Service Client...");
            BlobServiceClient blobServiceClient = new BlobServiceClient(storageAccountConnectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(imageBlobName);

            using (IDbConnection db = new SqlConnection(dbConnectionString))
            {
                Log("Fetching ImageIDs to process...");
                var imageIds = await db.QueryAsync<int>(
                    @"SELECT i.ImageID
                      FROM tblImages i
                      LEFT JOIN tblImageOnBlob b ON i.ImageID = b.ImageID
                      WHERE i.Deleted <> 1 AND b.ImageID IS NULL");

                Log($"Found {imageIds.Count()} images to process.");

                foreach (var imageId in imageIds)
                {
                    try
                    {
                        BlobClient blobClientFull = blobContainerClient.GetBlobClient($"{imageId}/full.tif");

                        bool exists = await blobClientFull.ExistsAsync();

                        await db.ExecuteAsync(
                            @"INSERT INTO tblImageOnBlob (ImageID, [Exists]) VALUES (@ImageID, @Exists)",
                            new
                            {
                                ImageID = imageId,
                                Exists = exists ? 1 : 0
                            }
                        );

                        Log($"ImageID {imageId} Exists: {(exists ? 1 : 0)} ");
                    }
                    catch (Exception ex)
                    {
                        Log($"Error processing ImageID {imageId}: {ex.Message}");
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
