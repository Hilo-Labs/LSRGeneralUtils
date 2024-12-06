using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace GenerateCompanyCSVs
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";

            // Generate Missing Municipality files
            GenerateFiles(
                connectionString,
                "C:\\Development\\Utils\\GenerateCompanyCSVs\\CSVs\\Missing Municipality",
                "Missing Municipality ({0}) {1} - {2}.csv",
                @"
                SELECT 
                    I.ImageID, 
                    I.companyID,
                    C.CompanyNumberPrefix,
                    C.CompanyName
                FROM 
                    tblImages I
                JOIN 
                    tblCompanies C 
                    ON C.CompanyID = I.CompanyID
                WHERE 
                    1=1
                    AND I.ImageID IN (SELECT imageid FROM tblLSR887_NoMunicipality)
                    AND I.MunicipalityId IS NULL
                ORDER BY I.companyID
                "
            );

            // Generate Missing Concession and RegPlanNo files
            GenerateFiles(
                connectionString,
                "C:\\Development\\Utils\\GenerateCompanyCSVs\\CSVs\\Missing Concession and RegPlanNo",
                "Missing Concession and RegPlanNo ({0}) {1} - {2}.csv",
                @"
                SELECT 
                    I.ImageID, 
                    I.companyID,
                    C.CompanyNumberPrefix,
                    C.CompanyName
                FROM 
                    tblImages I
                JOIN 
                    tblCompanies C 
                    ON C.CompanyID = I.CompanyID
                WHERE 
                    1=1
                    AND I.ImageID IN (SELECT imageid FROM tblInvalidImages)
                    AND (I.MunicipalityId IS NOT NULL)
                    AND (I.RegPlanNo IS NULL OR I.RegPlanNo = '')
                    AND (I.Concession IS NULL OR I.Concession = '')
                ORDER BY I.companyID
                "
            );
        }

        static void GenerateFiles(string connectionString, string directoryPath, string fileNameFormat, string query)
        {
            // Ensure directory exists
            Directory.CreateDirectory(directoryPath);

            // Fetch data
            DataTable table = FetchData(connectionString, query);

            // Group data by CompanyID
            var groupedData = table.AsEnumerable()
                .GroupBy(row => new
                {
                    CompanyID = row.Field<int>("companyID"),
                    CompanyNumberPrefix = row.Field<string>("CompanyNumberPrefix"),
                    CompanyName = row.Field<string>("CompanyName")
                });

            // Generate CSV files
            foreach (var group in groupedData)
            {
                string companyNumberPrefix = group.Key.CompanyNumberPrefix;
                string CompanyID = group.Key.CompanyID.ToString();
                string companyName = group.Key.CompanyName;
                List<int> imageIDs = group.Select(row => row.Field<int>("ImageID")).ToList();

                // Generate file name
                string sanitizedCompanyName = string.Join("_", companyName.Split(Path.GetInvalidFileNameChars()));
                string fileName = Path.Combine(directoryPath, string.Format(fileNameFormat, companyNumberPrefix, sanitizedCompanyName, CompanyID));

                // Write to CSV
                WriteToCsv(fileName, imageIDs);
                Console.WriteLine($"File generated: {fileName}");
            }
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

        static void WriteToCsv(string fileName, List<int> imageIDs)
        {
            using (StreamWriter writer = new StreamWriter(fileName))
            {
                // Write header
                writer.WriteLine("ImageID");

                // Write data
                foreach (int imageID in imageIDs)
                {
                    writer.WriteLine(imageID);
                }
            }
        }
    }
}
