using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace StorageLifecyle
{
    public static class LifecycleOrchestrator
    {
        [FunctionName("LifecycleOrchestrator")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context)
        {
            var outputs = new List<string>();

            // Replace "hello" with the name of your Durable Activity Function.
            outputs.Add(await context.CallActivityAsync<string>("LifecycleOrchestrator_GetBlobs", null));

            return outputs;
        }

        [FunctionName("LifecycleOrchestrator_GetBlobs")]
        public static void GetBlobs([ActivityTrigger] string name, ILogger log)
        {
            List<IListBlobItem>  blobs = getBlobList(log);
            foreach (var blob in blobs)
            {
                log.LogInformation(blob.StorageUri.PrimaryUri.AbsolutePath);
            }
        }

        [FunctionName("LifecycleOrchestrator_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("LifecycleOrchestrator", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        private static List<IListBlobItem> getBlobList(ILogger log)
        {
            try
            {
                var config = new ConfigurationBuilder()
                    .AddEnvironmentVariables()
                    .Build();

                string saName = config["storageaccountname"];
                string saKey = config["storageaccountkey"];


                CloudStorageAccount storageAccount = new CloudStorageAccount(
                new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(
                    saName,
                    saKey), true);

            BlobContinuationToken token = null;
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            
            IEnumerable<CloudBlobContainer> containers;
            do
            {
                containers = blobClient.ListContainersSegmentedAsync(token).Result.Results;

            }while (token != null);
            
            //reset continuation token
            token = null;

            List<IListBlobItem> blobs = new List<IListBlobItem>();
            foreach (var c in containers)
            {
                do
                {
                    var result = c.ListBlobsSegmentedAsync(token);
                    token = result.Result.ContinuationToken;
                    blobs.AddRange(result.Result.Results);
                    //Now do something with the blobs
                } while (token != null);
            }

            return blobs;

            }
            catch (System.Exception ex)
            {
                log.LogInformation(ex.InnerException.ToString());
                return null;
            }
        }
    }
}