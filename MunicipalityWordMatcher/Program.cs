using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;

namespace MunicipalityWordMatcher
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";

            // Define stop words
            List<string> stopWords = new List<string> { "of", "[original]" };

            // Define general words
            List<string> generalWords = new List<string> { "city", "township", "village", "town", "lake", "area", "townsite", "island", "district" };

            // Define word mappings
            Dictionary<string, string> wordMappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                { "twp", "township" },
                { "twps", "township" }
            };

            // Fetch data from tblImages
            DataTable imagesTable = FetchData(connectionString, @"
                SELECT 
                    I.ImageID, 
                    I.RegOfficeID,
                    I.Municipality AS ImageMunicipality,
                    I.ConsolidatedMunicipality
                FROM 
                    tblImages I
                WHERE 1=1
                    AND I.Deleted <> 1 AND I.Done = 1
                    AND I.MunicipalityId IS NULL
                    AND I.Municipality <> ''
            ");

            // Fetch data from tblRefMunicipality
            DataTable municipalitiesTable = FetchData(connectionString, @"
                SELECT 
                    M.MunicipalityID,
                    M.RegOfficeID,
                    M.Municipality, 
                    M.Name 
                FROM 
                    tblRefMunicipality M
            ");

            // Clear the results table before processing
            ClearResultsTable(connectionString);

            // Process the data
            var results = ProcessData(imagesTable, municipalitiesTable, stopWords, generalWords, wordMappings);

            // Insert results into the database
            InsertResults(connectionString, results);

            // Output the header
            //Console.WriteLine("ImageID | Image Words | RefMunicipality Words | Coefficient | IsSameRegOfficeID | MunicipalityID");

            //// Output the results
            //foreach (var result in results)
            //{
            //    Console.WriteLine($"{result.ImageID} | {string.Join(", ", result.ImageWords)} | {string.Join(", ", result.MunicipalityWords)} | {result.SimilarityCoefficient} | {result.IsSameRegOfficeID} | {result.MunicipalityID}");
            //}
        }

        static DataTable FetchData(string connectionString, string query)
        {
            DataTable table = new DataTable();
            using (SqlConnection conn = new SqlConnection(connectionString))
            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                conn.Open();
                table.Load(cmd.ExecuteReader());
            }
            return table;
        }

        static void ClearResultsTable(string connectionString)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            using (SqlCommand cmd = new SqlCommand("TRUNCATE TABLE tblLSR_887_MuniMatch", conn))
            {
                conn.Open();
                cmd.ExecuteNonQuery();
            }
        }

        static void InsertResults(string connectionString, List<MatchResult> results)
        {
            // Create a DataTable with the same schema as the destination table
            DataTable resultTable = new DataTable();
            resultTable.Columns.Add("ImageID", typeof(int));
            resultTable.Columns.Add("ImageWords", typeof(string));
            resultTable.Columns.Add("RefMunicipalityWords", typeof(string));
            resultTable.Columns.Add("Coefficient", typeof(int));
            resultTable.Columns.Add("IsSameRegOfficeID", typeof(bool));
            resultTable.Columns.Add("MunicipalityID", typeof(int));

            // Fill the DataTable with the results
            foreach (var result in results)
            {
                DataRow row = resultTable.NewRow();
                row["ImageID"] = result.ImageID;
                row["ImageWords"] = string.Join(", ", result.ImageWords);
                row["RefMunicipalityWords"] = string.Join(", ", result.MunicipalityWords);
                row["Coefficient"] = result.SimilarityCoefficient;
                row["IsSameRegOfficeID"] = result.IsSameRegOfficeID;
                row["MunicipalityID"] = result.MunicipalityID.HasValue ? (object)result.MunicipalityID.Value : DBNull.Value;

                resultTable.Rows.Add(row);
            }

            // Use SqlBulkCopy to insert the data into the database
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = "tblLSR_887_MuniMatch";

                    // Map the columns
                    bulkCopy.ColumnMappings.Add("ImageID", "ImageID");
                    bulkCopy.ColumnMappings.Add("ImageWords", "ImageWords");
                    bulkCopy.ColumnMappings.Add("RefMunicipalityWords", "RefMunicipalityWords");
                    bulkCopy.ColumnMappings.Add("Coefficient", "Coefficient");
                    bulkCopy.ColumnMappings.Add("IsSameRegOfficeID", "IsSameRegOfficeID");
                    bulkCopy.ColumnMappings.Add("MunicipalityID", "MunicipalityID");

                    // Optionally, set the batch size to improve performance
                    // bulkCopy.BatchSize = 10000;

                    // Write data to the server
                    bulkCopy.WriteToServer(resultTable);
                }
            }
        }

        static List<MatchResult> ProcessData(DataTable imagesTable, DataTable municipalitiesTable, List<string> stopWords, List<string> generalWords, Dictionary<string, string> wordMappings)
        {
            // Create a list to hold the results
            List<MatchResult> results = new List<MatchResult>();

            // Build municipalities data
            List<MunicipalityData> allMunicipalities = municipalitiesTable.AsEnumerable()
                .Select(row => new MunicipalityData
                {
                    MunicipalityID = row.Field<int>("MunicipalityID"),
                    RegOfficeID = row.Field<int>("RegOfficeID"),
                    Municipality = row.Field<string>("Municipality"),
                    Name = row.Field<string>("Name")
                }).ToList();

            // Build a dictionary for municipalities keyed by RegOfficeID
            var municipalitiesByRegOffice = allMunicipalities
                .GroupBy(m => m.RegOfficeID)
                .ToDictionary(
                    group => group.Key,
                    group => group.ToList()
                );

            // Iterate over each image
            foreach (DataRow imageRow in imagesTable.Rows)
            {
                int imageID = imageRow.Field<int>("ImageID");
                int regOfficeID = imageRow.Field<int>("RegOfficeID");

                string imageMunicipality = imageRow.Field<string>("ImageMunicipality") ?? string.Empty;
                string consolidatedMunicipality = imageRow.Field<string>("ConsolidatedMunicipality") ?? string.Empty;

                // Process words from tblImages
                var imageWords = ProcessWords($"{imageMunicipality} {consolidatedMunicipality}", stopWords, wordMappings);

                MunicipalityData bestMatchMunicipality = null;
                int highestSimilarityCoefficient = int.MinValue;
                HashSet<string> bestMatchMunicipalityWords = new HashSet<string>();
                bool isSameRegOfficeID = true;

                // First, try matching within same RegOfficeID
                List<MunicipalityData> matchingMunicipalities = null;

                if (municipalitiesByRegOffice.ContainsKey(regOfficeID))
                {
                    matchingMunicipalities = municipalitiesByRegOffice[regOfficeID];
                }
                else
                {
                    matchingMunicipalities = new List<MunicipalityData>();
                }

                // Try to find best match within same RegOfficeID
                foreach (var municipalityData in matchingMunicipalities)
                {
                    // Process words from tblRefMunicipality
                    var municipalityWords = ProcessWords($"{municipalityData.Municipality} {municipalityData.Name}", stopWords, wordMappings);

                    // Calculate Similarity Coefficient
                    int similarityCoefficient = CalculateSimilarityCoefficient(imageWords, municipalityWords, generalWords);

                    // Update the best match if this municipality has a higher similarity coefficient
                    if (similarityCoefficient > highestSimilarityCoefficient)
                    {
                        highestSimilarityCoefficient = similarityCoefficient;
                        bestMatchMunicipality = municipalityData;
                        bestMatchMunicipalityWords = municipalityWords;
                        isSameRegOfficeID = true;
                    }
                }

                // If no match or similarity coefficient is zero or negative, try matching with other RegOfficeIDs
                if (highestSimilarityCoefficient <= 0)
                {
                    foreach (var municipalityData in allMunicipalities.Where(m => m.RegOfficeID != regOfficeID))
                    {
                        // Process words from tblRefMunicipality
                        var municipalityWords = ProcessWords($"{municipalityData.Municipality} {municipalityData.Name}", stopWords, wordMappings);

                        // Calculate Similarity Coefficient
                        int similarityCoefficient = CalculateSimilarityCoefficient(imageWords, municipalityWords, generalWords);

                        // Update the best match if this municipality has a higher similarity coefficient
                        if (similarityCoefficient > highestSimilarityCoefficient)
                        {
                            highestSimilarityCoefficient = similarityCoefficient;
                            bestMatchMunicipality = municipalityData;
                            bestMatchMunicipalityWords = municipalityWords;
                            isSameRegOfficeID = false;
                        }
                    }
                }

                // If no matching municipality was found or similarity coefficient is zero or negative, set default values
                if (bestMatchMunicipality == null || highestSimilarityCoefficient <= 0)
                {
                    bestMatchMunicipality = null;
                    highestSimilarityCoefficient = 0;
                    bestMatchMunicipalityWords = new HashSet<string>();
                    isSameRegOfficeID = false;
                }

                // Add result to the list
                results.Add(new MatchResult
                {
                    ImageID = imageID,
                    ImageWords = imageWords,
                    MunicipalityWords = bestMatchMunicipalityWords,
                    SimilarityCoefficient = highestSimilarityCoefficient,
                    MunicipalityID = bestMatchMunicipality?.MunicipalityID,
                    IsSameRegOfficeID = isSameRegOfficeID
                });
            }

            return results;
        }

        static int CalculateSimilarityCoefficient(HashSet<string> imageWords, HashSet<string> municipalityWords, List<string> generalWords)
        {
            var matchingWords = imageWords.Intersect(municipalityWords).ToHashSet();

            // Count of matching non-general words
            int matchingNonGeneralWordsCount = matchingWords.Except(generalWords).Count();

            if (matchingNonGeneralWordsCount == 0)
                return 0;

            // Count of matching general words
            int matchingGeneralWordsCount = matchingWords.Intersect(generalWords).Count();

            // Exclude general words from imageWords and municipalityWords
            var imageWordsWithoutGeneral = imageWords.Except(generalWords);
            var municipalityWordsWithoutGeneral = municipalityWords.Except(generalWords);

            // Count of non-matching non-general words
            int nonMatchingWordsCountWithoutGeneral = (imageWordsWithoutGeneral.Count() - matchingNonGeneralWordsCount)
                                    + (municipalityWordsWithoutGeneral.Count() - matchingNonGeneralWordsCount);

            // Calculate the Similarity Coefficient
            int similarityCoefficient = (matchingNonGeneralWordsCount << 7) * (1 + (matchingGeneralWordsCount << 2)) - nonMatchingWordsCountWithoutGeneral;

            return similarityCoefficient;
        }

        static HashSet<string> ProcessWords(string input, List<string> stopWords, Dictionary<string, string> wordMappings)
        {
            var cleanedInput = input.Replace(",", " ")
                                    .Replace(".", " ")
                                    .Replace(";", " ");

            cleanedInput = Regex.Replace(cleanedInput, @"\s+", " ");

            var words = cleanedInput.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            words = words.Select(w => w.Trim(new char[] { '(', ')', '[', ']' }))
                         .Where(w => w.Length > 0)
                         .Select(w => w.ToLower())
                         .Except(stopWords)
                         .Select(w => wordMappings.ContainsKey(w) ? wordMappings[w].ToLower() : w)
                         .ToArray();

            var wordSet = new HashSet<string>(words);

            return wordSet;
        }
    }

    class MunicipalityData
    {
        public int MunicipalityID { get; set; }
        public int RegOfficeID { get; set; }
        public string Municipality { get; set; }
        public string Name { get; set; }
    }

    class MatchResult
    {
        public int ImageID { get; set; }
        public HashSet<string> ImageWords { get; set; }
        public HashSet<string> MunicipalityWords { get; set; }
        public int SimilarityCoefficient { get; set; }
        public int? MunicipalityID { get; set; }
        public bool IsSameRegOfficeID { get; set; }
    }
}
