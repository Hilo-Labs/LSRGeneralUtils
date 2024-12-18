using System.Data.SqlClient;
using System.Data;
using Dapper;
using ImageMagick;
using System.Text.RegularExpressions;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using ImageMagick.Drawing;

namespace ImageRegionDrawer
{
    class Test
    {
        public class ImageRecord
        {
            public int ImageID { get; set; }
            public string Region1 { get; set; }
            public string Region2 { get; set; }
            public string Region3 { get; set; }
        }

        public class RegionRectangle
        {
            const double factor = 15;

            public static RegionRectangle CreateOrNull(string region)
            {
                if (IsEmptyRegion(region))
                {
                    return null;
                }
                return new RegionRectangle(region);
            }

            public RegionRectangle() { }

            public RegionRectangle(string region)
            {
                const string regexPattern = @"\((\d+(\.\d+)?),(\d+(\.\d+)?),(\d+(\.\d+)?),(\d+(\.\d+)?)\)";
                var match = Regex.Match(region, regexPattern);
                if (match.Success)
                {
                    double yVal = double.Parse(match.Groups[1].Value);
                    double xVal = double.Parse(match.Groups[3].Value);
                    double hVal = double.Parse(match.Groups[5].Value);
                    double wVal = double.Parse(match.Groups[7].Value);

                    Y = yVal / factor;
                    X = xVal / factor;
                    Height = hVal / factor;
                    Width = wVal / factor;
                }
                else
                {
                    Console.WriteLine($"Invalid region format: {region}");
                }
            }

            public double X { get; set; }
            public double Y { get; set; }
            public double Width { get; set; }
            public double Height { get; set; }

            private static bool IsEmptyRegion(string region)
            {
                return string.IsNullOrEmpty(region) || region == "(0,0,0,0)";
            }
        }

        private static bool markOnly = false;

        public static async Task RunTest()
        {
            string dbConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";

            const string storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=landsurveyrecords;AccountKey=daDBzSf4cY+bQmlDPWWvbWpKcqXcSh31R9PHpngwstppwsUV3wsTvt144WMlz+SOO9KjkZE0Xpg3DgRwiLGLDQ==;EndpointSuffix=core.windows.net";
            const string containerName = "images-v3";

            string baseDirectory = @"C:\Users\Alexiel\Downloads\Regions";
            if (!Directory.Exists(baseDirectory))
            {
                Directory.CreateDirectory(baseDirectory);
            }

            string sqlQuery = @"
                select imageid, region1, region2, region3
                from tblImages 
                where 1=1

                and companyID = 39

                and (
                       (region1 is not null and region1 <> '(0,0,0,0)') 
                    or (region2 is not null and region2 <> '(0,0,0,0)') 
                    or (region3 is not null and region3 <> '(0,0,0,0)')
                )

                --and imageid > 3327572

                --and imageid in (3316394, 3316395)

                --and imageid in (3315646,3315903,3316394,3316395,3316396,3316397,3316398,3316399,3316400,3316401,3316402,3316403,3316404,3316405,3316406,3316407,3316408,3316409,3316410,3316411,3316412,3316413,3316414,3316415,3316416,3316417,3316419,3316421,3316422,3316423,3316424,3316425,3316426,3316427,3316428,3316429,3316430,3316431,3316432,3316433,3316434,3316435,3316436,3316437,3316438,3316439,3316440,3316441,3316443,3316444,3316445,3316446,3316447,3316448,3316449,3316450,3316451,3316452,3316453,3316454,3316455,3316456,3316457,3316458,3316459,3316460,3316461,3316462,3316464,3316465,3316466,3316468,3316469,3316470,3316471,3316472,3316473,3316474,3316475,3316477,3316478,3316479,3316480,3316481,3316482,3316483,3316484,3316485,3316488,3316489,3316490,3316491,3316492,3316493,3316494,3316495,3316496,3316497,3316498,3316499,3316500,3316501,3316502,3316507,3316508,3316509,3316512,3316513,3316518,3316570,3316571,3316572,3316573,3316574,3316575,3316576,3316577,3316578,3316579,3316580,3316581,3316582,3316583,3316584,3316585,3316586,3316587,3316588,3316589,3316590,3316591,3316592,3316593,3316594,3316595,3316596,3316597,3316598,3316599,3316600,3316601,3316602,3316603,3316649,3316650,3316651,3316652,3316722,3316725,3316764,3316830,3316831,3316832,3316833,3316834,3316835,3316836,3316837,3316838,3316839,3316840,3316841,3316842,3316843,3316844,3316846,3316847,3316848,3316849,3316850,3316851,3316852,3316853,3316854,3316855,3316856,3316857,3316858,3316859,3316860,3316861,3316864,3316865,3316866,3316867,3316869,3316870,3316871,3316872,3316873,3316874,3316875,3316876,3316877,3316878,3316879,3316880,3316881,3316882,3316883,3316884,3316885,3316886,3316887,3316888,3316889,3317021,3317022,3317023,3317024,3317025,3317026,3317027,3317028,3317029,3317030,3317032,3317033,3317034,3317655,3317657,3317658,3326025,3326185,3326189,3327572)

                and timedatePosted >= '2024-08-01'
                --and timedatePosted <= '2024-11-02'

                order by imageid desc
            ";

            List<ImageRecord> imageRecords;
            using (IDbConnection db = new SqlConnection(dbConnectionString))
            {
                imageRecords = db.Query<ImageRecord>(sqlQuery).ToList();
            }

            if (!imageRecords.Any())
            {
                Console.WriteLine("No records found matching the criteria.");
                return;
            }

            BlobServiceClient blobServiceClient = new BlobServiceClient(storageAccountConnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);

            foreach (var record in imageRecords)
            {
                int imageId = record.ImageID;
                Console.WriteLine($"Processing ImageID: {imageId}");

                var regions = new List<string> { record.Region1, record.Region2, record.Region3 };
                var regionRectangles = regions
                    .Select(r => RegionRectangle.CreateOrNull(r))
                    .Where(rr => rr != null)
                    .ToList();

                if (regionRectangles.Count == 0)
                {
                    Console.WriteLine($"No valid regions for ImageID: {imageId}");
                    continue;
                }

                BlobClient blobClientFull = containerClient.GetBlobClient($"{imageId}/full.tif");
                var blobOpenReadOptions = new BlobOpenReadOptions(allowModifications: false);
                uint widthFull, heightFull;

                using (var blobStreamFull = await blobClientFull.OpenReadAsync(blobOpenReadOptions))
                {
                    MagickImage fullImage = null;
                    try
                    {
                        fullImage = new MagickImage();
                        fullImage.Ping(blobStreamFull); // Attempt to get dimensions without fully loading
                        widthFull = fullImage.Width;
                        heightFull = fullImage.Height;
                        Console.WriteLine($"Full image dimensions: Width = {widthFull}, Height = {heightFull}");
                    }
                    catch (Exception pingEx)
                    {
                        Console.WriteLine($"Ping failed for ImageID {imageId} full.tif: {pingEx.Message}");
                        Console.WriteLine("Downloading the entire full.tif to read dimensions...");

                        blobStreamFull.Position = 0;
                        fullImage = new MagickImage(blobStreamFull);
                        widthFull = fullImage.Width;
                        heightFull = fullImage.Height;
                        Console.WriteLine($"Full image dimensions (after full download): Width = {widthFull}, Height = {heightFull}");
                    }
                }

                double fullWidth = widthFull;
                double fullHeight = heightFull;
                double maxDim = Math.Max(fullWidth, fullHeight);

                double chosenScale = 1.0;
                MagickColor color = MagickColor.FromRgb(255, 0, 0);

                bool linearScale = maxDim < 10000;
                double expandedScaleFactor = linearScale ? maxDim / 2000 : 5;

                var (maxX1, maxY1) = GetMaxCoordinates(regionRectangles, 1.0);
                var (maxXExpanded, maxYExpanded) = GetMaxCoordinates(regionRectangles, expandedScaleFactor);

                bool expandedOutOfBounds = maxXExpanded > fullWidth || maxYExpanded > fullHeight;
                bool scale1TooSmall = (maxX1 < (fullWidth * 0.5) && maxY1 < (fullHeight * 0.5));

                if (!expandedOutOfBounds && scale1TooSmall)
                {
                    // Tentatively choose the expandedScaleFactor
                    chosenScale = expandedScaleFactor;
                    color = maxDim < 10000 ? MagickColor.FromRgb(0, 255, 0) : MagickColor.FromRgb(0, 0, 255);

                    // After deciding expanded scale, check if any region at expandedScaleFactor is too large
                    double imageArea = fullWidth * fullHeight;
                    bool anyRegionTooLarge = regionRectangles.Any(r =>
                        ((r.Width * expandedScaleFactor) * (r.Height * expandedScaleFactor)) >= (0.3 * imageArea)
                    );

                    if (anyRegionTooLarge)
                    {
                        // If large region at expanded scale, revert to scale=1
                        chosenScale = 1.0;
                        color = MagickColor.FromRgb(255, 0, 0);
                        Console.WriteLine($"A region is too large at expanded scale for ImageID: {imageId}, reverting to scale=1.");
                    }
                }
                else
                {
                    chosenScale = 1.0;
                    color = MagickColor.FromRgb(255, 0, 0);
                }

                if (!markOnly && chosenScale == 1.0)
                {
                    Console.WriteLine($"chosenScale = 1.0 for ImageID: {imageId}, skipping image.");
                    continue;
                }

                MemoryStream imageStream = await DownloadFullTifToMemory(blobClientFull);
                if (imageStream == null)
                {
                    Console.WriteLine($"Failed to download full image data for ImageID: {imageId}. Skipping.");
                    continue;
                }

                imageStream.Position = 0;
                using (var image = new MagickImage(imageStream))
                {
                    if (markOnly)
                    {
                        // Mark scenario
                        DrawScenario(image, regionRectangles, chosenScale,
                                     Path.Combine(baseDirectory, $"marked_full_{imageId}.tif"),
                                     color);
                    }
                    else
                    {
                        // Crop scenario
                        string imageDirectory = Path.Combine(baseDirectory, imageId.ToString());
                        if (!Directory.Exists(imageDirectory))
                        {
                            Directory.CreateDirectory(imageDirectory);
                        }

                        var namedRegions = new[]
                        {
                            ("region1", record.Region1),
                            ("region2", record.Region2),
                            ("region3", record.Region3)
                        };

                        foreach (var (regionName, regionStr) in namedRegions)
                        {
                            var rect = RegionRectangle.CreateOrNull(regionStr);
                            if (rect == null) continue;

                            // Apply the chosenScale to the region coordinates
                            double scaledX = rect.X * chosenScale;
                            double scaledY = rect.Y * chosenScale;
                            double scaledWidth = rect.Width * chosenScale;
                            double scaledHeight = rect.Height * chosenScale;

                            var cropGeometry = new MagickGeometry(
                                (int)scaledX,
                                (int)scaledY,
                                (uint)scaledWidth,
                                (uint)scaledHeight
                            );

                            using (var cropped = image.Clone())
                            {
                                try
                                {
                                    cropped.Crop(cropGeometry);
                                    string gifFileName = Path.Combine(imageDirectory, $"{regionName}.gif");
                                    cropped.Write(gifFileName, MagickFormat.Gif);

                                    Console.WriteLine($"Saved cropped region to {gifFileName}");
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"Error cropping/writing {regionName} for ImageID {imageId}: {ex.Message}");
                                }
                            }
                        }
                    }
                }

                Console.WriteLine($"Completed processing ImageID: {imageId}");
            }

            Console.WriteLine("All processing completed.");
        }

        private static (double maxX, double maxY) GetMaxCoordinates(IEnumerable<RegionRectangle> rectangles, double scale)
        {
            double maxX = 0, maxY = 0;
            foreach (var rr in rectangles)
            {
                double scaledX = rr.X * scale + rr.Width * scale;
                double scaledY = rr.Y * scale + rr.Height * scale;
                if (scaledX > maxX) maxX = scaledX;
                if (scaledY > maxY) maxY = scaledY;
            }
            return (maxX, maxY);
        }

        private static void DrawScenario(MagickImage sourceImage, IEnumerable<RegionRectangle> rectangles, double scaleFactor, string outputFilePath, MagickColor color)
        {
            using (var scenarioImage = (MagickImage)sourceImage.Clone())
            {
                bool drewAnyRegion = false;
                foreach (var regionRect in rectangles)
                {
                    double scaledX = regionRect.X * scaleFactor;
                    double scaledY = regionRect.Y * scaleFactor;
                    double scaledWidth = regionRect.Width * scaleFactor;
                    double scaledHeight = regionRect.Height * scaleFactor;

                    DrawRectangle(scenarioImage, scaledX, scaledY, scaledWidth, scaledHeight, color);
                    drewAnyRegion = true;
                }

                scenarioImage.Scale(2000, 2000);
                scenarioImage.Write(outputFilePath);
                Console.WriteLine($"Saved processed image to {outputFilePath} (Drew regions: {drewAnyRegion}, ScaleFactor: {scaleFactor})");
            }
        }

        private static void DrawRectangle(MagickImage image, double x, double y, double width, double height, MagickColor color)
        {
            double endX = x + width;
            double endY = y + height;

            // Clamp start coordinates
            if (x < 0) x = 0;
            if (y < 0) y = 0;

            // Clamp end coordinates to the image boundaries
            if (endX > image.Width - 1) endX = image.Width - 1;
            if (endY > image.Height - 1) endY = image.Height - 1;

            // Recalculate width and height after clamping
            double clampedWidth = endX - x;
            double clampedHeight = endY - y;

            if (clampedWidth > 0 && clampedHeight > 0)
            {
                var drawables = new Drawables()
                    .StrokeColor(color)
                    .StrokeWidth(20)
                    .FillColor(new MagickColor("transparent"))
                    .Rectangle(x, y, x + clampedWidth, y + clampedHeight);

                drawables.Draw(image);
            }
        }

        private static async Task<MemoryStream> DownloadFullTifToMemory(BlobClient blobClientFull)
        {
            MemoryStream memoryStream = new MemoryStream();
            try
            {
                await blobClientFull.DownloadToAsync(memoryStream);
                Console.WriteLine($"Downloaded {blobClientFull.Name} into memory.");
                memoryStream.Position = 0;
                return memoryStream;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error downloading {blobClientFull.Name} to memory: {ex.Message}");
                return null;
            }
        }
    }
}
