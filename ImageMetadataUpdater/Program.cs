using System;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using Dapper;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using ImageMagick;
using System.Threading.Tasks;
using System.Linq;
using Azure;
using System.Collections.Generic;

namespace ImageProcessorService
{
    class Program
    {
        const string dbConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";
        const string storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=landsurveyrecords;AccountKey=daDBzSf4cY+bQmlDPWWvbWpKcqXcSh31R9PHpngwstppwsUV3wsTvt144WMlz+SOO9KjkZE0Xpg3DgRwiLGLDQ==;EndpointSuffix=core.windows.net";
        const string imageBlobName = "images-v3";
        const double Threshold = 0.15; // 15%
        const int HighResolutionMaxSize = 10000;
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
                    Log("Initializing settings...");
                    Log("Starting image metadata update process...");
                    await UpdateImageMetadataAsync(dbConnectionString, storageAccountConnectionString);
                    Log("Image metadata update process complete.");
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

        static async Task UpdateImageMetadataAsync(string dbConnectionString, string storageAccountConnectionString)
        {
            Log("Initializing Blob Service Client...");
            BlobServiceClient blobServiceClient = new BlobServiceClient(storageAccountConnectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(imageBlobName);

            using (IDbConnection db = new SqlConnection(dbConnectionString))
            {
                while (!(Console.KeyAvailable && Console.ReadKey().Key == ConsoleKey.Escape))
                {
                    Log("Attempting to fetch an image for processing...");
                    var updatingImageId = (await db.QueryAsync<int>(
                        @"UPDATE TOP(1) tblMetaUpdated
                          SET Status = -1
                          OUTPUT INSERTED.ImageID
                          WHERE Status = 0"
                    )).FirstOrDefault();

                    if (updatingImageId <= 0)
                    {
                        Log("No images to process. Exiting...");
                        break;
                    }

                    try
                    {
                        Log($"Processing ImageID: {updatingImageId}");
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

                        Log("Opening blob stream for full.tif...");
                        var blobOpenReadOptions = new BlobOpenReadOptions(allowModifications: false);

                        uint widthFull, heightFull;
                        MagickImage fullImage = null;
                        using (var blobStreamFull = await blobClientFull.OpenReadAsync(blobOpenReadOptions))
                        {
                            try
                            {
                                fullImage = new MagickImage();
                                fullImage.Ping(blobStreamFull);
                                widthFull = fullImage.Width;
                                heightFull = fullImage.Height;
                                Log($"Full image dimensions: Width = {widthFull}, Height = {heightFull}");
                            }
                            catch (Exception pingEx)
                            {
                                Log($"Ping failed for ImageID {updatingImageId} full.tif: {pingEx.Message}");
                                Log("Downloading the entire full.tif to read dimensions...");
                                fullImage = new MagickImage(blobStreamFull);
                                widthFull = fullImage.Width;
                                heightFull = fullImage.Height;
                                Log($"Full image dimensions (after full download): Width = {widthFull}, Height = {heightFull}");
                            }
                        }

                        Log("Opening blob stream for medium.gif...");
                        uint widthMedium, heightMedium;
                        MagickImage mediumImage = null;
                        using (var blobStreamMedium = await blobClientMedium.OpenReadAsync(new BlobOpenReadOptions(allowModifications: false)))
                        {
                            mediumImage = new MagickImage();
                            mediumImage.Ping(blobStreamMedium);
                            widthMedium = mediumImage.Width;
                            heightMedium = mediumImage.Height;
                            Log($"Medium image dimensions: Width = {widthMedium}, Height = {heightMedium}");
                        }

                        double aspectFull = (double)widthFull / heightFull;
                        double aspectMedium = (double)widthMedium / heightMedium;
                        double aspectFullRotated = (double)heightFull / widthFull;

                        double aspectDifferenceOriginal = Math.Abs(aspectFull - aspectMedium) / aspectMedium;
                        double aspectDifferenceRotated = Math.Abs(aspectFullRotated - aspectMedium) / aspectMedium;

                        int changed;
                        int rotationAngle = 0;

                        if (aspectDifferenceOriginal <= Threshold)
                        {
                            changed = 0;
                            Log("Aspect ratios match within threshold. Changed = 0");
                        }
                        else if (aspectDifferenceRotated <= Threshold)
                        {
                            changed = 1;
                            Log("Aspect ratios match after rotation within threshold. Changed = 1");

                            Log("Determining rotation direction using image comparison...");
                            uint compareSize = 256;

                            IMagickImage<byte> fullResized = fullImage.Clone();
                            fullResized.Resize(compareSize, compareSize);

                            IMagickImage<byte> mediumResized = mediumImage.Clone();
                            mediumResized.Resize(compareSize, compareSize);

                            double diffNoRotation = fullResized.Compare(mediumResized, ErrorMetric.RootMeanSquared);
                            Log($"Difference without rotation: {diffNoRotation}");

                            fullResized.Rotate(90);
                            double diffRotateCW = fullResized.Compare(mediumResized, ErrorMetric.RootMeanSquared);
                            Log($"Difference after 90 degrees CW rotation: {diffRotateCW}");

                            fullResized.Rotate(-180);
                            double diffRotateCCW = fullResized.Compare(mediumResized, ErrorMetric.RootMeanSquared);
                            Log($"Difference after 90 degrees CCW rotation: {diffRotateCCW}");

                            double minDiff = Math.Min(Math.Min(diffNoRotation, diffRotateCW), diffRotateCCW);
                            if (minDiff == diffRotateCW)
                            {
                                rotationAngle = 90;
                                Log("Rotation direction determined: 90 degrees clockwise.");
                            }
                            else if (minDiff == diffRotateCCW)
                            {
                                rotationAngle = -90;
                                Log("Rotation direction determined: 90 degrees counter-clockwise.");
                            }
                            else
                            {
                                Log("No significant difference found with rotation.");
                            }
                        }
                        else
                        {
                            if (aspectDifferenceOriginal < aspectDifferenceRotated)
                            {
                                changed = 2;
                                Log("Aspect ratio closer to original orientation. Changed = 2");
                            }
                            else
                            {
                                changed = 3;
                                Log("Aspect ratio closer to rotated orientation. Changed = 3");
                            }
                        }

                        BlobProperties properties = await blobClientFull.GetPropertiesAsync();
                        IDictionary<string, string> metadata = properties.Metadata;

                        metadata["width"] = widthFull.ToString();
                        metadata["height"] = heightFull.ToString();

                        await blobClientFull.SetMetadataAsync(metadata);

                        Log($"Metadata updated for blob {updatingImageId}/full.tif");

                        int statusToUpdate = 2;

                        if (changed == 1)
                        {
                            Log("Changed is 1, generating high.gif from full.tif with rotation...");
                            using (var blobStreamFull = await blobClientFull.OpenReadAsync(new BlobOpenReadOptions(allowModifications: false)))
                            {
                                using (var originalImage = new MagickImage(blobStreamFull))
                                {
                                    var highGifMemoryStream = ConvertToGifWithMaxSize(originalImage, HighResolutionMaxSize, rotationAngle);
                                    highGifMemoryStream.Position = 0;
                                    Log("Conversion to high.gif complete.");

                                    var highGifBlobClient = blobContainerClient.GetBlobClient($"{updatingImageId}/high.gif");
                                    await highGifBlobClient.UploadAsync(highGifMemoryStream, overwrite: true);
                                    Log($"Regenerated high.gif from full.tif and uploaded for ImageID {updatingImageId}");
                                }
                            }
                            statusToUpdate = 4;
                        }
                        else if (changed == 2 || changed == 3)
                        {
                            Log($"Changed is {changed}, copying medium.gif to high.gif...");
                            var highGifBlobClient = blobContainerClient.GetBlobClient($"{updatingImageId}/high.gif");
                            var mediumGifBlobClient = blobContainerClient.GetBlobClient($"{updatingImageId}/medium.gif");
                            await highGifBlobClient.StartCopyFromUriAsync(mediumGifBlobClient.Uri);
                            Log($"Copied medium.gif to high.gif for ImageID {updatingImageId}");
                            statusToUpdate = 6;
                        }
                        else
                        {
                            Log("Changed is 0, no action needed.");
                        }

                        await db.ExecuteAsync(
                            @"UPDATE tblMetaUpdated SET Status = @Status, Width = @Width, Height = @Height, GifWidth = @GifWidth, GifHeight = @GifHeight, Changed = @Changed WHERE ImageID = @ImageID",
                            new
                            {
                                ImageID = updatingImageId,
                                Width = (int)widthFull,
                                Height = (int)heightFull,
                                GifWidth = (int)widthMedium,
                                GifHeight = (int)heightMedium,
                                Changed = changed,
                                Status = statusToUpdate
                            }
                        );
                        Log($"Database status updated to '{statusToUpdate}' with width, height, gif dimensions, and changed.");
                    }
                    catch (Exception ex)
                    {
                        Log($"Error processing ImageID {updatingImageId}: {ex.Message}");
                        await db.ExecuteAsync(
                            @"UPDATE tblMetaUpdated SET Status = -40, Reason = @Reason WHERE ImageID = @ImageID",
                            new
                            {
                                ImageID = updatingImageId,
                                Reason = ex.Message
                            }
                        );
                        Log("Database status updated to '-40' (failure).");
                    }
                    Log("=============================================");
                }
            }
        }

        public static MemoryStream ConvertToGifWithMaxSize(IMagickImage image, int maxSize, int rotationAngle)
        {
            var originalWidth = image.Width;
            var originalHeight = image.Height;
            var maxDimension = Math.Max(originalWidth, originalHeight);

            if (maxDimension > maxSize)
            {
                double scaleFactor = (double)maxSize / maxDimension;
                var newWidth = (uint)Math.Round(originalWidth * scaleFactor);
                var newHeight = (uint)Math.Round(originalHeight * scaleFactor);

                var resizeSettings = new MagickGeometry(newWidth, newHeight)
                {
                    IgnoreAspectRatio = false
                };
                image.Resize(resizeSettings);
            }

            if (rotationAngle != 0)
            {
                image.Rotate(rotationAngle);
                Log($"Applied rotation of {rotationAngle} degrees.");
            }

            image.Quantize(new QuantizeSettings
            {
                Colors = 128,
                ColorSpace = ColorSpace.sRGB,
                DitherMethod = DitherMethod.Riemersma
            });

            image.Format = MagickFormat.Gif;

            var memoryStream = new MemoryStream();
            image.Write(memoryStream);
            memoryStream.Position = 0;

            return memoryStream;
        }

        public static void Log(string message)
        {
            var logMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}";
            Console.WriteLine(logMessage);
            logWriter.WriteLine(logMessage);
        }
    }
}
