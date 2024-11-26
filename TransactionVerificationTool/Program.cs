using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace TransactionVerificationTool
{
    class Program
    {
        const string connectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";

        static void Main(string[] args)
        {
            MainAsync().Wait();
        }

        static async Task MainAsync()
        {
            List<int> transactionIds = new List<int>();
            List<FailedTransaction> failedTransactions = new List<FailedTransaction>();

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                // Fetch transaction IDs based on the date range
                transactionIds = GetTransactionIds(connection);

                if (transactionIds.Count == 0)
                {
                    Console.WriteLine("No transactions found for the specified date range.");
                    return;
                }

                // Set up HttpClient
                var client = new HttpClient();
                client.Timeout = TimeSpan.FromSeconds(500);

                // Authentication - obtain access token
                string serverUrl = "https://krcmar.landsurveyrecords.com";
                //string serverUrl = "http://virtuosa.homedns.org:3000";
                var tokenUri = serverUrl + "/oauth/token";

                var tokenRequestBody = new
                {
                    companyId = 1,
                    secret = "5D424EA36B8E3419A7A66C2418E96",
                    grantType = "hiloAdmin",
                };

                var tokenContent = new StringContent(JsonConvert.SerializeObject(tokenRequestBody), Encoding.UTF8, "application/json");

                try
                {
                    // Fetch access token
                    var tokenResponse = await client.PostAsync(tokenUri, tokenContent);
                    if (!tokenResponse.IsSuccessStatusCode)
                    {
                        Console.WriteLine("Failed to obtain access token.");
                        return;
                    }

                    var tokenResponseContent = await tokenResponse.Content.ReadAsStringAsync();
                    var tokenData = JsonConvert.DeserializeObject<dynamic>(tokenResponseContent);
                    string accessToken = tokenData.token;

                    // Set Authorization header
                    client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);
                    client.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));

                    foreach (var transactionId in transactionIds)
                    {
                        Console.WriteLine($"Processing TransactionID={transactionId}");

                        // Get transaction data
                        var lineItems = GetTransactionLineItems(connection, transactionId);

                        if (lineItems == null || lineItems.Count == 0)
                        {
                            Console.WriteLine($"No line items found.");
                            failedTransactions.Add(new FailedTransaction
                            {
                                TransactionID = transactionId,
                                Reasons = new List<string> { "No line items found" }
                            });
                            continue;
                        }

                        var purchasingCompanyID = lineItems[0].PurchasingCompanyID;
                        var purchasingCompanyName = lineItems[0].PurchasingCompanyName;

                        string companyIdForApiCall = (purchasingCompanyID == 38) && (purchasingCompanyName == "Protect Your Boundaries") ? "public" : purchasingCompanyID.ToString();

                        var imageIds = lineItems.Select(li => li.ImageID).Distinct();
                        string imagesIdParam = string.Join(",", imageIds);

                        // Call API
                        var apiResult = await CallApi(client, serverUrl, companyIdForApiCall, imagesIdParam);

                        var reasons = CompareValues(lineItems, apiResult);

                        if (reasons.Any())
                        {
                            failedTransactions.Add(new FailedTransaction
                            {
                                TransactionID = transactionId,
                                Reasons = reasons
                            });
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An error occurred: {ex.Message}");
                }
            }

            // Summary of failed transactions
            Console.WriteLine("\nSummary of Failed Transactions:");
            if (failedTransactions.Any())
            {
                foreach (var failedTransaction in failedTransactions)
                {
                    Console.WriteLine($"TransactionID={failedTransaction.TransactionID} - Reasons: {string.Join(", ", failedTransaction.Reasons)}");
                }
            }
            else
            {
                Console.WriteLine("All transactions passed.");
            }
        }

        private static List<int> GetTransactionIds(SqlConnection connection)
        {
            string query = @"
                SELECT DISTINCT
                    T.TransactionID 
                FROM 
                    tblTransactions T
                JOIN 
                    tblLineItems L ON L.TransactionID = T.TransactionID
                JOIN 
                    tblImages I ON I.ImageID = L.ImageID
                WHERE 1=1
                    AND T.Paid = 1
                    AND T.T_DatePaid IS NOT NULL
                    AND T.CompanyID IS NOT NULL
                    AND T.T_DatePaid >= @StartDate AND T.T_DatePaid < @EndDate
                    AND I.CompanyID = 3

                    --AND T.TransactionID in (153756, 153458)

                ORDER BY 
                    T.TransactionID DESC";

            // Modify the date range as needed
            DateTime startDate = new DateTime(2024, 10, 1);
            DateTime endDate = new DateTime(2024, 11, 1);

            var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@StartDate", startDate);
            command.Parameters.AddWithValue("@EndDate", endDate);

            var transactionIds = new List<int>();

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    transactionIds.Add(Convert.ToInt32(reader["TransactionID"]));
                }
            }

            return transactionIds;
        }

        private static List<TransactionLineItem> GetTransactionLineItems(SqlConnection connection, int transactionId)
        {
            string query = @"
                SELECT 
                    T.TransactionID AS Transaction_ID,
                    T.T_DatePaid,
                    T.SubTotal AS Subtotal_Amount,
                    T.GST AS GST_Amount,
                    T.Total AS Total_Amount,
                    L.Price,
                    L.Surcharge,
                    L.LSRAmount,
                    L.OwnerAmount,
                    T.CompanyID AS Purchasing_Company_ID,
                    T.CompanyName PurchasingCompanyName,
                    L.ImageID AS Image_ID,
                    I.CompanyID AS Selling_Company_ID
                FROM 
                    tblTransactions T
                JOIN 
                    tblLineItems L ON L.TransactionID = T.TransactionID
                JOIN 
                    tblImages I ON I.ImageID = L.ImageID
                LEFT JOIN 
                    tblCompanies PC ON T.CompanyID = PC.CompanyID
                LEFT JOIN 
                    tblCompanies SC ON I.CompanyID = SC.CompanyID
                WHERE 
                    L.TransactionID = @TransactionID
                ORDER BY 
                    T.TransactionID DESC";

            var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@TransactionID", transactionId);

            var lineItems = new List<TransactionLineItem>();

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var item = new TransactionLineItem
                    {
                        TransactionID = Convert.ToInt32(reader["Transaction_ID"]),
                        T_DatePaid = (DateTime)reader["T_DatePaid"],
                        SubTotal = (decimal)reader["Subtotal_Amount"],
                        GST = (decimal)reader["GST_Amount"],
                        Total = (decimal)reader["Total_Amount"],
                        Price = (decimal)reader["Price"],
                        Surcharge = (decimal)reader["Surcharge"],
                        LSRAmount = (decimal)reader["LSRAmount"],
                        OwnerAmount = (decimal)reader["OwnerAmount"],
                        PurchasingCompanyID = Convert.ToInt32(reader["Purchasing_Company_ID"]),
                        ImageID = (int)reader["Image_ID"],
                        SellingCompanyID = (int)reader["Selling_Company_ID"],
                        PurchasingCompanyName = reader["PurchasingCompanyName"] != DBNull.Value ? (string)reader["PurchasingCompanyName"] : null
                    };
                    lineItems.Add(item);
                }
            }

            return lineItems;
        }

        private static async Task<ApiResponse> CallApi(HttpClient client, string serverUrl, string companyId, string imagesId)
        {
            string apiUrl = $"{serverUrl}/api/getDetailedImagePrices?companyId={companyId}&imagesId={imagesId}";

            try
            {
                var response = await client.GetAsync(apiUrl);

                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();
                    var apiResponse = JsonConvert.DeserializeObject<ApiResponse>(content);
                    return apiResponse;
                }
                else
                {
                    Console.WriteLine($"API call failed for companyId={companyId}, imagesId={imagesId}, StatusCode={response.StatusCode}");
                    return null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception during API call for companyId={companyId}, imagesId={imagesId}: {ex.Message}");
                return null;
            }
        }

        private static List<string> CompareValues(List<TransactionLineItem> lineItems, ApiResponse apiResponse)
        {
            var reasons = new List<string>();

            if (apiResponse == null)
            {
                reasons.Add("No API response");
                return reasons;
            }

            decimal totalPriceDB = 0;
            decimal totalPriceAPI = 0;

            decimal totalLSRAmountDB = 0;
            decimal totalLSRAmountAPI = 0;

            decimal totalOwnerAmountDB = 0;
            decimal totalOwnerAmountAPI = 0;

            const decimal Tolerance = 0.025M;

            foreach (var lineItem in lineItems)
            {
                var apiImagePrice = apiResponse.imagesPrice.FirstOrDefault(ip => ip.imageId == lineItem.ImageID);

                if (apiImagePrice == null)
                {
                    reasons.Add($"ImageID={lineItem.ImageID}: No data from API");
                    continue;
                }

                if (Math.Abs(lineItem.LSRAmount - apiImagePrice.lsrAmount) >= Tolerance)
                    reasons.Add($"ImageID={lineItem.ImageID} LSRAmount Mismatch: DB={lineItem.LSRAmount}, API={apiImagePrice.lsrAmount}");

                if (Math.Abs(lineItem.OwnerAmount - apiImagePrice.ownerAmount) >= Tolerance)
                    reasons.Add($"ImageID={lineItem.ImageID} OwnerAmount Mismatch: DB={lineItem.OwnerAmount}, API={apiImagePrice.ownerAmount}");

                totalPriceDB += lineItem.Price;
                totalPriceAPI += apiImagePrice.price;
            }

            var transaction = lineItems[0];

            if (Math.Abs(transaction.SubTotal - totalPriceAPI) >= Tolerance)
                reasons.Add($"SubTotal Mismatch: DB={transaction.SubTotal}, API={totalPriceAPI}");

            return reasons;
        }
    }

    class TransactionLineItem
    {
        public int TransactionID { get; set; }
        public DateTime T_DatePaid { get; set; }
        public decimal SubTotal { get; set; }
        public decimal GST { get; set; }
        public decimal Total { get; set; }
        public decimal Price { get; set; }
        public decimal Surcharge { get; set; }
        public decimal LSRAmount { get; set; }
        public decimal OwnerAmount { get; set; }
        public int PurchasingCompanyID { get; set; }
        public string PurchasingCompanyName { get; set; }
        public int ImageID { get; set; }
        public int SellingCompanyID { get; set; }
    }

    public class ApiResponse
    {
        public int? purchasingCompanyId { get; set; }
        public List<ImagePrice> imagesPrice { get; set; }
    }

    public class ImagePrice
    {
        public bool succeed { get; set; }
        public int imageId { get; set; }
        public int sellingCompanyId { get; set; }
        public decimal basePrice { get; set; }
        public decimal surcharge { get; set; }
        public decimal price { get; set; }
        public decimal lsrAmount { get; set; }
        public decimal ownerAmount { get; set; }
        public List<string> notes { get; set; }
    }

    public class FailedTransaction
    {
        public int TransactionID { get; set; }
        public List<string> Reasons { get; set; }
    }
}
