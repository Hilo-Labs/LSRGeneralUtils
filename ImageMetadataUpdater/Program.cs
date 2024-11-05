using System;
using System.Data.SqlClient;
using System.Data;
using Dapper;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using ImageMagick;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using Azure;

namespace ImageProcessorService
{
    class Program
    {
        const string dbConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";
        const string storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=landsurveyrecords;AccountKey=daDBzSf4cY+bQmlDPWWvbWpKcqXcSh31R9PHpngwstppwsUV3wsTvt144WMlz+SOO9KjkZE0Xpg3DgRwiLGLDQ==;EndpointSuffix=core.windows.net";
        const string imageBlobName = "images-v3";

        static async Task<int> Main(string[] args)
        {
            try
            {
                Console.WriteLine("Initializing settings...");

                Console.WriteLine("Starting image metadata update process...");
                await UpdateImageMetadataAsync(dbConnectionString, storageAccountConnectionString);
                Console.WriteLine("Image metadata update process complete.");

                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred during processing:");
                Console.WriteLine(ex.Message);
                return 1;
            }
        }

        static async Task UpdateImageMetadataAsync(string dbConnectionString, string storageAccountConnectionString)
        {
            Console.WriteLine("Initializing Blob Service Client...");

            BlobServiceClient blobServiceClient = new BlobServiceClient(storageAccountConnectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(imageBlobName);

            using (IDbConnection db = new SqlConnection(dbConnectionString))
            {
                while (true)
                {
                    Console.WriteLine("Attempting to fetch an image for processing...");

                    var updatingImageId = (await db.QueryAsync<int>(
                        @"UPDATE TOP(1) tblMetaUpdated
                          SET Status = -1
                          OUTPUT INSERTED.ImageID
                          WHERE Status = 0"
                    )).FirstOrDefault();

                    if (updatingImageId <= 0)
                    {
                        Console.WriteLine("No images to process. Exiting...");
                        break;
                    }

                    try
                    {
                        Console.WriteLine($"Processing ImageID: {updatingImageId}");

                        BlobClient blobClient = blobContainerClient.GetBlobClient($"{updatingImageId}/full.tif");

                        if (!await blobClient.ExistsAsync())
                        {
                            throw new FileNotFoundException($"Blob {updatingImageId}/full.tif does not exist.");
                        }

                        Console.WriteLine("Opening blob stream for reading...");

                        var blobOpenReadOptions = new BlobOpenReadOptions(allowModifications: false);

                        using (var blobStream = await blobClient.OpenReadAsync(blobOpenReadOptions))
                        {
                            uint width, height;
                            try
                            {
                                using (var magickImage = new MagickImage())
                                {
                                    magickImage.Ping(blobStream);

                                    width = magickImage.Width;
                                    height = magickImage.Height;

                                    Console.WriteLine($"Image dimensions (using Ping): Width = {width}, Height = {height}");
                                }
                            }
                            catch (Exception pingEx)
                            {
                                Console.WriteLine($"Ping failed for ImageID {updatingImageId}: {pingEx.Message}");
                                Console.WriteLine("Downloading the entire image to read dimensions...");

                                using (var fullImageStream = new MemoryStream())
                                {
                                    await blobClient.DownloadToAsync(fullImageStream);
                                    fullImageStream.Position = 0;

                                    using (var magickImage = new MagickImage(fullImageStream))
                                    {
                                        width = magickImage.Width;
                                        height = magickImage.Height;

                                        Console.WriteLine($"Image dimensions (after full download): Width = {width}, Height = {height}");
                                    }
                                }
                            }

                            BlobProperties properties = await blobClient.GetPropertiesAsync();
                            IDictionary<string, string> metadata = properties.Metadata;

                            metadata["width"] = width.ToString();
                            metadata["height"] = height.ToString();

                            await blobClient.SetMetadataAsync(metadata);

                            Console.WriteLine($"Metadata updated for blob {updatingImageId}/full.tif");

                            await db.ExecuteAsync(
                                @"UPDATE tblMetaUpdated SET Status = 2, Width = @Width, Height = @Height WHERE ImageID = @ImageID",
                                new
                                {
                                    ImageID = updatingImageId,
                                	Width = (int)width,
                                    Height = (int)height
                                }
                            );
                            Console.WriteLine("Database status updated to '2' (success) with width and height.");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error processing ImageID {updatingImageId}: {ex.Message}");

                        await db.ExecuteAsync(
                            @"UPDATE tblMetaUpdated SET Status = -40, Reason = @Reason WHERE ImageID = @ImageID",
                            new
                            {
                                ImageID = updatingImageId,
                                Reason = ex.Message
                            }
                        );
                        Console.WriteLine("Database status updated to '-40' (failure).");
                    }
                }
            }
        }
    }
}
