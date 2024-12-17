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


            ----


                select top 100 imageid, companyid, region1, region2, region3, Cropping, RotationAngle, originalFilename, filename from tblImages where 1=1
                and companyID = 39
                and region1 <> '(0,0,0,0)'

                order by imageid desc

             */

            int[] imageIds = {
                3329588,3329163,3329141,3329130,3329118,3328862,3328861,3328860,3328859,3328852,3327691,3327606,3327605,3327604,3327603,3327586,3327585,3327584,3327583,3327582,3327581,3327580,3327579,3327578,3327577,3327576,3327575,3327574,3327573,3327572,3327551,3327550,3327549,3327548,3327547,3327546,3327545,3327544,3327543,3327542,3327541,3327540,3327539,3327538,3327537,3327521,3327520,3327519,3327518,3327517,3327516,3327515,3327514,3327513,3327512,3327511,3327510,3327509,3327508,3327507,3327491,3327490,3327489,3327488,3327487,3327486,3327485,3327484,3327483,3327482,3327481,3327480,3327479,3327478,3327477,3327476,3327475,3327474,3327473,3327472,3327471,3327470,3327469,3327468,3327467,3327466,3327465,3327464,3327463,3327462,3327461,3327460,3327459,3327458,3327457,3327456,3327455,3327454,3327453,3327452
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
