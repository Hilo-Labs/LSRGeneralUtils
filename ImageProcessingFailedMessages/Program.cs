using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Newtonsoft.Json;

namespace QueueMessageBrowser
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
            const string storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=landsurveyrecords;AccountKey=daDBzSf4cY+bQmlDPWWvbWpKcqXcSh31R9PHpngwstppwsUV3wsTvt144WMlz+SOO9KjkZE0Xpg3DgRwiLGLDQ==;EndpointSuffix=core.windows.net";
            const string failedQueueName = "image-processing-request-v3-failed";

            QueueClient queueClient = new QueueClient(storageAccountConnectionString, failedQueueName);
            await queueClient.CreateIfNotExistsAsync();

            int maxMessages = 32;
            var peekedMessages = await queueClient.PeekMessagesAsync(maxMessages);

            List<long> imageIds = new List<long>();

            foreach (var peekedMessage in peekedMessages.Value)
            {
                var decodedJson = Encoding.UTF8.GetString(Convert.FromBase64String(peekedMessage.MessageText));

                var request = JsonConvert.DeserializeObject<ImageProcessingRequest>(decodedJson);

                if (request != null)
                {
                    imageIds.Add(request.ImageID);
                }
            }

            Console.WriteLine(string.Join(",", imageIds));
        }
    }
}
