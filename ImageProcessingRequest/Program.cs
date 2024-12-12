using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Newtonsoft.Json;

namespace QueueMessageInserter
{
    class Program
    {
        public class ImageProcessingRequest
        {
            public string Requester { get; set; }
            public long ImageID { get; set; }
            public string FileType { get; set; }
            public int FinalStatus { get; set; }
            public int CreatedByUserId { get; set; }
        }

        static async Task Main(string[] args)
        {
            // Storage and queue info
            const string storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=landsurveyrecords;AccountKey=daDBzSf4cY+bQmlDPWWvbWpKcqXcSh31R9PHpngwstppwsUV3wsTvt144WMlz+SOO9KjkZE0Xpg3DgRwiLGLDQ==;EndpointSuffix=core.windows.net";
            const string queueName = "image-processing-request-v3";

            string dbConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";

            long[] imageIds = {
                3273242,3276182,3292684,3292686,3292688,3297995,3298285,3300433,3300434,3301442,3303971,3303973,3303975,3303987,3304063,3304067,3304069,3305475,3314643,3319242,3326580
            };

            QueueClient queueClient = new QueueClient(storageAccountConnectionString, queueName);
            await queueClient.CreateIfNotExistsAsync();

            using (IDbConnection dbConnection = new SqlConnection(dbConnectionString))
            {
                dbConnection.Open();

                foreach (var imageId in imageIds)
                {
                    int createdByUserId = GetCreatedByUserId(dbConnection, imageId);

                    var request = new ImageProcessingRequest
                    {
                        Requester = "LSR-local",
                        ImageID = imageId,
                        FileType = "tif",
                        FinalStatus = 1,
                        CreatedByUserId = createdByUserId
                    };

                    string messageJson = JsonConvert.SerializeObject(request);

                    string base64EncodedMessage = ConvertToBase64(messageJson);
                    await queueClient.SendMessageAsync(base64EncodedMessage);

                    Console.WriteLine($"Message inserted for ImageID: {imageId} with CreatedByUserId: {createdByUserId}");
                }
            }

            Console.WriteLine("All messages inserted successfully.");
        }

        static int GetCreatedByUserId(IDbConnection dbConnection, long imageId)
        {
            using (var command = dbConnection.CreateCommand())
            {
                command.CommandText = $"SELECT CreatedByUserID FROM tblImages WHERE ImageID = @ImageID";
                var param = command.CreateParameter();
                param.ParameterName = "@ImageID";
                param.Value = imageId;
                command.Parameters.Add(param);

                object result = command.ExecuteScalar();

                if (result == null || result == DBNull.Value)
                {
                    return 1156;
                }

                return Convert.ToInt32(result);
            }
        }

        static string ConvertToBase64(string input)
        {
            var bytes = Encoding.UTF8.GetBytes(input);
            return Convert.ToBase64String(bytes);
        }
    }
}
