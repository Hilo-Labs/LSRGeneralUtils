using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace BlobDownloader
{
    class Program
    {
        static async Task Main(string[] args)
        {
            const bool onlyFullTif = true;
            const string storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=landsurveyrecords;AccountKey=daDBzSf4cY+bQmlDPWWvbWpKcqXcSh31R9PHpngwstppwsUV3wsTvt144WMlz+SOO9KjkZE0Xpg3DgRwiLGLDQ==;EndpointSuffix=core.windows.net";

            const string containerName = "images-v3";

            /*
                SELECT STRING_AGG(CAST(imageid AS VARCHAR(20)), ',') AS ImageIDs
                FROM tblImages
                WHERE deleted <> 1
                  AND Uploading = 1
                  AND InProcess <> 1
                  AND done <> 1
                  AND companyID = 486;
             */

            int[] imageIds = {
                //3317957, 3317206, 3317961, 3317962, 3327369, 3327370, 3317963
                //3276182,3276185,3311216,3311399,3312582,3312597,3312743,3312967,3313028,3313050,3313067,3313307,3313573,3313878,3273242,3292684,3292686,3292688,3297995,3301471,3304063,3304067,3304069,3305475,3314643,3298285,3303971,3303973,3303975,3303987,3317206,3300433,3300434,3301442,3327369,3327370,3317957,3317961,3317962,3317963,3319242,3326580
                3273242,3276182,3292684,3292686,3292688,3297995,3298285,3300433,3300434,3301442,3303971,3303973,3303975,3303987,3304063,3304067,3304069,3305475,3314643,3319242,3326580,
            };

            string baseDirectory = @"C:\Users\Alexiel\Downloads\Azure_Blobs";

            if (!Directory.Exists(baseDirectory))
            {
                Directory.CreateDirectory(baseDirectory);
            }

            BlobServiceClient blobServiceClient = new BlobServiceClient(storageAccountConnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);

            foreach (var imageId in imageIds)
            {
                Console.WriteLine($"Processing ImageID: {imageId}");

                string prefix = imageId.ToString() + "/";

                await foreach (BlobItem blobItem in containerClient.GetBlobsAsync(prefix: prefix))
                {
                    if (onlyFullTif)
                    {
                        // Only download full.tif and put it in the base directory as full_{imageId}.tif
                        if (Path.GetFileName(blobItem.Name).Equals("full.tif", StringComparison.OrdinalIgnoreCase))
                        {
                            string localFilePath = Path.Combine(baseDirectory, $"full_{imageId}.tif");
                            Console.WriteLine($"Found full.tif: {blobItem.Name}. Downloading to {localFilePath}");

                            BlobClient blobClient = containerClient.GetBlobClient(blobItem.Name);
                            await blobClient.DownloadToAsync(localFilePath);

                            Console.WriteLine($"Downloaded {blobItem.Name} to {localFilePath}");
                        }
                    }
                    else
                    {
                        // Original behavior: Download all blobs into subfolders
                        string localFilePath = Path.Combine(baseDirectory, blobItem.Name.Replace('/', Path.DirectorySeparatorChar));
                        Console.WriteLine($"Found blob: {blobItem.Name}. Downloading to {localFilePath}");

                        string directoryName = Path.GetDirectoryName(localFilePath);
                        if (!Directory.Exists(directoryName))
                        {
                            Directory.CreateDirectory(directoryName);
                        }

                        BlobClient blobClient = containerClient.GetBlobClient(blobItem.Name);
                        await blobClient.DownloadToAsync(localFilePath);

                        Console.WriteLine($"Downloaded {blobItem.Name} to {localFilePath}");
                    }
                }

                Console.WriteLine($"Completed downloading all blobs for ImageID: {imageId}");
            }

            Console.WriteLine("All downloads completed.");
        }
    }
}
