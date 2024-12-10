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
            const string storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=landsurveyrecords;AccountKey=daDBzSf4cY+bQmlDPWWvbWpKcqXcSh31R9PHpngwstppwsUV3wsTvt144WMlz+SOO9KjkZE0Xpg3DgRwiLGLDQ==;EndpointSuffix=core.windows.net";

            const string containerName = "images-v3";

            int[] imageIds = { 2885738, 3186905, 3187497, 3187660, 3188116, 3189064, 3189166, 3189169, 3189482, 3189767 }; 

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

                Console.WriteLine($"Completed downloading all blobs for ImageID: {imageId}");
            }

            Console.WriteLine("All downloads completed.");
        }
    }
}
