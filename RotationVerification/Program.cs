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
        const double SquareThreshold = 0.06; 
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
                    Log("Starting rotation verification process using aspect ratios only...");
                    await VerifyRotationAsync(dbConnectionString, storageAccountConnectionString);
                    Log("Rotation verification process complete.");
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

        static async Task VerifyRotationAsync(string dbConnectionString, string storageAccountConnectionString)
        {
            Log("Initializing Blob Service Client...");
            BlobServiceClient blobServiceClient = new BlobServiceClient(storageAccountConnectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(imageBlobName);

            using (IDbConnection db = new SqlConnection(dbConnectionString))
            {
                while (!(Console.KeyAvailable && Console.ReadKey().Key == ConsoleKey.Escape))
                {
                    Log("Attempting to fetch an image with Status = 4 for verification...");
                    var verifyingImageId = (await db.QueryAsync<int>(
                        @"UPDATE TOP(1) tblMetaUpdated
                          SET Status = -1
                          OUTPUT INSERTED.ImageID
                          WHERE Status = 4"
                    )).FirstOrDefault();

                    if (verifyingImageId <= 0)
                    {
                        Log("No images with Status = 4 found. Exiting...");
                        break;
                    }

                    try
                    {
                        Log($"Verifying ImageID: {verifyingImageId}");
                        BlobClient blobClientFull = blobContainerClient.GetBlobClient($"{verifyingImageId}/full.tif");
                        BlobClient blobClientMedium = blobContainerClient.GetBlobClient($"{verifyingImageId}/medium.gif");

                        if (!await blobClientFull.ExistsAsync())
                        {
                            throw new FileNotFoundException($"Blob {verifyingImageId}/full.tif does not exist.");
                        }

                        if (!await blobClientMedium.ExistsAsync())
                        {
                            throw new FileNotFoundException($"Blob {verifyingImageId}/medium.gif does not exist.");
                        }

                        BlobProperties fullProperties = await blobClientFull.GetPropertiesAsync();
                        IDictionary<string, string> fullMetadata = fullProperties.Metadata;
                        if (!fullMetadata.ContainsKey("width") || !fullMetadata.ContainsKey("height"))
                        {
                            throw new InvalidDataException($"Metadata for {verifyingImageId}/full.tif does not contain width/height.");
                        }

                        if (!uint.TryParse(fullMetadata["width"], out uint widthFull) ||
                            !uint.TryParse(fullMetadata["height"], out uint heightFull))
                        {
                            throw new InvalidDataException($"Invalid width/height metadata for {verifyingImageId}/full.tif.");
                        }

                        Log($"Full image dimensions (from metadata): Width = {widthFull}, Height = {heightFull}");

                        Log("Fetching dimensions for medium.gif via Ping...");
                        uint widthMedium, heightMedium;
                        using (var blobStreamMedium = await blobClientMedium.OpenReadAsync(new BlobOpenReadOptions(allowModifications: false)))
                        {
                            MagickImage mediumImage = new MagickImage();
                            mediumImage.Ping(blobStreamMedium);
                            widthMedium = (uint)mediumImage.Width;
                            heightMedium = (uint)mediumImage.Height;
                            Log($"Medium image dimensions: Width = {widthMedium}, Height = {heightMedium}");
                        }

                        double aspectFull = (double)widthFull / heightFull;
                        double aspectMedium = (double)widthMedium / heightMedium;
                        double aspectFullRotated = (double)heightFull / widthFull;

                        bool isAlmostSquare = Math.Abs(aspectFull - 1.0) < SquareThreshold;
                        if (isAlmostSquare)
                        {
                            Log("Full image is almost square. Setting Status = 1100.");
                            await UpdateDatabaseStatus(db, verifyingImageId, 1100, widthFull, heightFull, widthMedium, heightMedium);
                            continue;
                        }

                        double aspectDifferenceOriginal = Math.Abs(aspectFull - aspectMedium) / aspectMedium;
                        double aspectDifferenceRotated = Math.Abs(aspectFullRotated - aspectMedium) / aspectMedium;

                        if (aspectDifferenceRotated < aspectDifferenceOriginal && aspectDifferenceRotated <= Threshold)
                        {
                            Log("Aspect ratio comparison suggests the image is rotated. Status = 400.");
                            await UpdateDatabaseStatus(db, verifyingImageId, 400, widthFull, heightFull, widthMedium, heightMedium);
                        }
                        else
                        {
                            Log("Aspect ratio comparison suggests the image is not rotated. Status = 1000.");
                            await UpdateDatabaseStatus(db, verifyingImageId, 1000, widthFull, heightFull, widthMedium, heightMedium);
                        }
                    }
                    catch (Exception ex)
                    {
                        Log($"Error verifying ImageID {verifyingImageId}: {ex.Message}");
                        await db.ExecuteAsync(
                            @"UPDATE tblMetaUpdated SET Status = -40, Reason = @Reason WHERE ImageID = @ImageID",
                            new
                            {
                                ImageID = verifyingImageId,
                                Reason = ex.Message
                            }
                        );
                        Log("Database status updated to '-40' (failure).");
                    }
                    Log("=============================================");
                }
            }
        }

        private static async Task UpdateDatabaseStatus(IDbConnection db, int imageId, int status, uint widthFull, uint heightFull, uint widthMedium, uint heightMedium)
        {
            await db.ExecuteAsync(
                @"UPDATE tblMetaUpdated 
                  SET Status = @Status, 
                      Width = @Width, 
                      Height = @Height, 
                      GifWidth = @GifWidth, 
                      GifHeight = @GifHeight 
                  WHERE ImageID = @ImageID",
                new
                {
                    ImageID = imageId,
                    Width = (int)widthFull,
                    Height = (int)heightFull,
                    GifWidth = (int)widthMedium,
                    GifHeight = (int)heightMedium,
                    Status = status
                }
            );
        }

        public static void Log(string message)
        {
            var logMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}";
            Console.WriteLine(logMessage);
            logWriter.WriteLine(logMessage);
        }
    }
}
