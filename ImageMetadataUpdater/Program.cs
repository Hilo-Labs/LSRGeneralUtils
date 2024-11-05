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
        const double Threshold = 0.15; // 15%

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

                        BlobClient blobClientFull = blobContainerClient.GetBlobClient($"{updatingImageId}/full.tif");
                        BlobClient blobClientMedium = blobContainerClient.GetBlobClient($"{updatingImageId}/medium.gif");

                        if (!await blobClientFull.ExistsAsync())
                        {
                            throw new FileNotFoundException($"Blob {updatingImageId}/full.tif does not exist.");
                        }

                        if (!await blobClientMedium.ExistsAsync())
                        {
                            throw new FileNotFoundException($"Blob {updatingImageId}/medium.gif does not exist.");
                        }

                        Console.WriteLine("Opening blob stream for full.tif...");
                        var blobOpenReadOptions = new BlobOpenReadOptions(allowModifications: false);

                        uint widthFull, heightFull;
                        using (var blobStreamFull = await blobClientFull.OpenReadAsync(blobOpenReadOptions))
                        {
                            try
                            {
                                using (var magickImage = new MagickImage())
                                {
                                    magickImage.Ping(blobStreamFull);

                                    widthFull = magickImage.Width;
                                    heightFull = magickImage.Height;

                                    Console.WriteLine($"Full image dimensions: Width = {widthFull}, Height = {heightFull}");
                                }
                            }
                            catch (Exception pingEx)
                            {
                                Console.WriteLine($"Ping failed for ImageID {updatingImageId} full.tif: {pingEx.Message}");
                                Console.WriteLine("Downloading the entire full.tif to read dimensions...");

                                using (var fullImageStream = new MemoryStream())
                                {
                                    await blobClientFull.DownloadToAsync(fullImageStream);
                                    fullImageStream.Position = 0;

                                    using (var magickImage = new MagickImage(fullImageStream))
                                    {
                                        widthFull = magickImage.Width;
                                        heightFull = magickImage.Height;

                                        Console.WriteLine($"Full image dimensions (after full download): Width = {widthFull}, Height = {heightFull}");
                                    }
                                }
                            }
                        }

                        Console.WriteLine("Opening blob stream for medium.gif...");
                        uint widthMedium, heightMedium;
                        using (var blobStreamMedium = await blobClientMedium.OpenReadAsync(new BlobOpenReadOptions(allowModifications: false)))
                        {
                            using (var magickImageMedium = new MagickImage())
                            {
                                magickImageMedium.Ping(blobStreamMedium);

                                widthMedium = magickImageMedium.Width;
                                heightMedium = magickImageMedium.Height;

                                Console.WriteLine($"Medium image dimensions: Width = {widthMedium}, Height = {heightMedium}");
                            }
                        }

                        double aspectFull = (double)widthFull / heightFull;
                        double aspectMedium = (double)widthMedium / heightMedium;
                        double aspectFullRotated = (double)heightFull / widthFull;

                        double aspectDifferenceOriginal = Math.Abs(aspectFull - aspectMedium) / aspectMedium;
                        double aspectDifferenceRotated = Math.Abs(aspectFullRotated - aspectMedium) / aspectMedium;

                        int changed;

                        if (aspectDifferenceOriginal <= Threshold)
                        {
                            changed = 0;
                            Console.WriteLine("Aspect ratios match within threshold. Changed = 0");
                        }
                        else if (aspectDifferenceRotated <= Threshold)
                        {
                            changed = 1;
                            Console.WriteLine("Aspect ratios match after rotation within threshold. Changed = 1");
                        }
                        else
                        {
                            if (aspectDifferenceOriginal < aspectDifferenceRotated)
                            {
                                changed = 2;
                                Console.WriteLine("Aspect ratio closer to original orientation. Changed = 2");
                            }
                            else
                            {
                                changed = 3;
                                Console.WriteLine("Aspect ratio closer to rotated orientation. Changed = 3");
                            }
                        }

                        BlobProperties properties = await blobClientFull.GetPropertiesAsync();
                        IDictionary<string, string> metadata = properties.Metadata;

                        metadata["width"] = widthFull.ToString();
                        metadata["height"] = heightFull.ToString();

                        await blobClientFull.SetMetadataAsync(metadata);

                        Console.WriteLine($"Metadata updated for blob {updatingImageId}/full.tif");

                        await db.ExecuteAsync(
                            @"UPDATE tblMetaUpdated SET Status = 2, Width = @Width, Height = @Height, GifWidth = @GifWidth, GifHeight = @GifHeight, Changed = @Changed WHERE ImageID = @ImageID",
                            new
                            {
                                ImageID = updatingImageId,
                                Width = (int)widthFull,
                                Height = (int)heightFull,
                                GifWidth = (int)widthMedium,
                                GifHeight = (int)heightMedium,
                                Changed = changed
                            }
                        );
                        Console.WriteLine("Database status updated to '2' (success) with width, height, gif dimensions, and changed.");
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
