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
                            double mediumScaleFactor = linearScale ? maxDim / 2000 : 5;

                            var regions = new List<string> { record.Region1, record.Region2, record.Region3 };
                            var regionRectangles = regions
                                .Select(r => RegionRectangle.CreateOrNull(r))
                                .Where(rr => rr != null)
                                .ToList();

                            var (maxX1, maxY1) = GetMaxCoordinates(regionRectangles, 1.0);
                            var (maxXMed, maxYMed) = GetMaxCoordinates(regionRectangles, mediumScaleFactor);

                            bool mediumOutOfBounds = maxXMed > fullWidth || maxYMed > fullHeight;

                            bool scale1TooSmall = (maxX1 < (fullWidth * 0.5) && maxY1 < (fullHeight * 0.5));

                            double chosenScale;
                            MagickColor color;
                            if (!mediumOutOfBounds && scale1TooSmall)
                            {
                                chosenScale = mediumScaleFactor;
                                color = maxDim < 10000 ? MagickColor.FromRgb(0, 255, 0) : MagickColor.FromRgb(0, 0, 255);
                            }
                            else
                            {
                                chosenScale = 1.0;
                                color = MagickColor.FromRgb(255, 0, 0);
                            }

                            var justfileName = Path.GetFileNameWithoutExtension(tifFile);

                            DrawScenario(image, regionRectangles, chosenScale, 
                                         Path.Combine(outputDirectory, $"{justfileName}.SelectedScale.tif"), 
                                         color);

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
            using (var scenarioImage = sourceImage.Clone() as MagickImage)
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
                    .StrokeWidth(10)
                    .FillColor(new MagickColor("transparent"))
                    .Rectangle(x, y, x + clampedWidth, y + clampedHeight);

                drawables.Draw(image);
            }
        }
    }
}