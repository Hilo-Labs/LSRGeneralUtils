using System;
using System.Data.SqlClient;
using System.Data;
using Dapper;
using ImageMagick;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using ImageMagick.Drawing;

namespace ImageRegionDrawer
{
    class Program
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
                    throw new FormatException($"Invalid region format: {region}");
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

        static void Main(string[] args)
        {
            string dbConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";

            string inputDirectory = @"C:\Users\Alexiel\Downloads\Azure_Blobs";
            string outputDirectory = @"C:\Users\Alexiel\Downloads\Regions";

            if (!Directory.Exists(inputDirectory))
            {
                Console.WriteLine($"Input directory does not exist: {inputDirectory}");
                return;
            }

            if (!Directory.Exists(outputDirectory))
            {
                Console.WriteLine($"Output directory does not exist: {outputDirectory}");
                return;
            }

            //var tifFiles = Directory.GetFiles(inputDirectory, "full_3327453.tif", SearchOption.TopDirectoryOnly);
            var tifFiles = Directory.GetFiles(inputDirectory, "*.tif", SearchOption.TopDirectoryOnly);

            if (!tifFiles.Any())
            {
                Console.WriteLine("No TIF files found in the input directory.");
                return;
            }

            using (IDbConnection db = new SqlConnection(dbConnectionString))
            {
                foreach (var tifFile in tifFiles)
                {
                    string sourceFileName = Path.GetFileName(tifFile);

                    var match = Regex.Match(sourceFileName, @"_(\d+)\.tif", RegexOptions.IgnoreCase);
                    if (!match.Success)
                    {
                        Console.WriteLine($"Skipping file {sourceFileName} - Unable to parse ImageID.");
                        continue;
                    }

                    int imageId = int.Parse(match.Groups[1].Value);

                    var sql = @"SELECT imageid, region1, region2, region3 
                                FROM tblImages 
                                WHERE imageid = @ImageID";
                    var record = db.Query<ImageRecord>(sql, new { ImageID = imageId }).FirstOrDefault();

                    if (record == null)
                    {
                        Console.WriteLine($"No database record found for ImageID: {imageId}");
                        continue;
                    }

                    Console.WriteLine($"Processing ImageID: {imageId}");

                    try
                    {
                        using (var image = new MagickImage(tifFile))
                        {
                            double fullWidth = image.Width;
                            double fullHeight = image.Height;
                            double maxDim = Math.Max(fullWidth, fullHeight);

                            var linearScale = maxDim < 10000;
                            double mediumScaleFactor = linearScale ? (maxDim / 2000.0) : 5.0;

                            var regions = new List<RegionRectangle>();
                            foreach (var rStr in new[] { record.Region1, record.Region2, record.Region3 })
                            {
                                var rr = RegionRectangle.CreateOrNull(rStr);
                                if (rr != null) regions.Add(rr);
                            }
                            if (regions.Count == 0)
                            {
                                image.Write(Path.Combine(outputDirectory, sourceFileName));
                                Console.WriteLine("No valid regions. Saved original image.");
                                continue;
                            }

                            double finalScale = 1.0;
                            if (mediumScaleFactor > 1.0)
                            {
                                var (outOfBounds1, bbox1) = CheckRectangles(regions, fullWidth, fullHeight, 1.0, 0, 0);
                                var (outOfBoundsMedium, bboxMedium) = CheckRectangles(regions, fullWidth, fullHeight, mediumScaleFactor, 0, 0);

                                if (!outOfBoundsMedium)
                                {
                                    double smallThreshold = 0.1;
                                    bool isSmallAt1 = (bbox1.Width < fullWidth * smallThreshold) && (bbox1.Height < fullHeight * smallThreshold);
                            //DrawScenario(image, regions, 2.15, Path.Combine(outputDirectory, $"{justfileName}.Medium.tif"), color, xShift, yShift);
                                    double betterThreshold = 0.2;
                                    bool isBetterAtMedium = (bboxMedium.Width >= fullWidth * betterThreshold) || (bboxMedium.Height >= fullHeight * betterThreshold);
                                    if (isSmallAt1 && isBetterAtMedium)
                                    {
                                        finalScale = mediumScaleFactor;
                                    }
                                    else
                                    {
                                        finalScale = 1.0;
                                    }
                                }
                                else
                                {
                                    finalScale = 1.0;
                                }
                            }
                            else
                            {
                                finalScale = 1.0;
                            }

                            //DrawScenario(image, regions, 5, Path.Combine(outputDirectory, $"{justfileName}.Medium.tif"), color, xShift, yShift);
                            var (finalOutOfBounds, finalBbox) = CheckRectangles(regions, fullWidth, fullHeight, finalScale, 0, 0);
                            if (finalOutOfBounds)
                            {
                                finalScale = 1.0;
                            }

                            using (var scenarioImage = image.Clone() as MagickImage)
                            {
                                bool drewAnyRegion = false;
                                foreach (var rr in regions)
                                {
                                    double scaledX = rr.X * finalScale;
                                    double scaledY = rr.Y * finalScale;
                                    double scaledWidth = rr.Width * finalScale;
                                    double scaledHeight = rr.Height * finalScale;

                                    // At this point we assume no out-of-bound because we checked previously.
                                    DrawRectangleNoCheck(scenarioImage, scaledX, scaledY, scaledWidth, scaledHeight, MagickColors.Red);
                                    drewAnyRegion = true;
                                }

                                string outputFilePath = Path.Combine(outputDirectory, sourceFileName);
                                scenarioImage.Write(outputFilePath);
                                Console.WriteLine($"Saved processed image to {outputFilePath} (Drew regions: {drewAnyRegion}, ScaleFactor: {finalScale})");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error processing ImageID {imageId}: {ex.Message}");
                    }
                }
            }

            Console.WriteLine("Processing completed.");
        }

        private static (bool outOfBounds, (double X, double Y, double Width, double Height)) CheckRectangles(
            List<RegionRectangle> regions,
            double imgWidth,
            double imgHeight,
            double scaleFactor,
            double xShift,
            double yShift)
        {
            double minX = double.MaxValue;
            double minY = double.MaxValue;
            double maxX = double.MinValue;
            double maxY = double.MinValue;

            bool outOfBounds = false;

            foreach (var rr in regions)
            {
                double scaledX = (rr.X * scaleFactor) + xShift;
                double scaledY = (rr.Y * scaleFactor) + yShift;
                double scaledW = rr.Width * scaleFactor;
                double scaledH = rr.Height * scaleFactor;

                double endX = scaledX + scaledW;
                double endY = scaledY + scaledH;

                if (scaledX < 0 || scaledY < 0 || endX > imgWidth || endY > imgHeight)
                {
                    outOfBounds = true;
                }

                if (scaledX < minX) minX = scaledX;
                if (scaledY < minY) minY = scaledY;
                if (endX > maxX) maxX = endX;
                if (endY > maxY) maxY = endY;
            }

            if (minX == double.MaxValue || minY == double.MaxValue || maxX == double.MinValue || maxY == double.MinValue)
            {
                return (false, (0, 0, 0, 0));
            }

            double bboxWidth = maxX - minX;
            double bboxHeight = maxY - minY;

            return (outOfBounds, (X: minX, Y: minY, Width: bboxWidth, Height: bboxHeight));
        }

        private static void DrawRectangleNoCheck(MagickImage image, double x, double y, double width, double height, MagickColor color)
        {
            var drawables = new Drawables()
                .StrokeColor(color)
                .StrokeWidth(3)
                .FillColor(new MagickColor("transparent"))
                .Rectangle(x, y, x + width, y + height);

            drawables.Draw(image);
        }
    }
}
