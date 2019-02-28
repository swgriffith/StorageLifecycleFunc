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

            // Start the first step in the orchestration to get the blob list
            outputs.Add(await context.CallActivityAsync<string>("LifecycleOrchestrator_GetBlobs", null));

            return outputs;
        }

        [FunctionName("LifecycleOrchestrator_GetBlobs")]
        public static void GetBlobs([ActivityTrigger] string name, ILogger log)
        {
            //Get the list of Blobs from the storage account
            List<IListBlobItem>  blobs = getBlobList(log);

            //For now just write the file absolute paths
            //TODO: Should add functionality to write blob data (name, path, size, etc) to azure table storage
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
            // Start the orchestration on the HTTP trigger
            string instanceId = await starter.StartNewAsync("LifecycleOrchestrator", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        private static List<IListBlobItem> getBlobList(ILogger log)
        {
            try
            {

                //Get the blob storage name and access key
                //You can use key vault to store these https://azure.microsoft.com/en-us/blog/simplifying-security-for-serverless-and-web-apps-with-azure-functions-and-app-service/
                //TODO: Should change out code to use a SAS Policy and SAS Token in the future for added security
                var config = new ConfigurationBuilder()
                    .AddEnvironmentVariables()
                    .Build();


                string saName = config["storageaccountname"];
                string saKey = config["storageaccountkey"];

            
                //Connect to the storage account
                CloudStorageAccount storageAccount = new CloudStorageAccount(
                new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(
                    saName,
                    saKey), true);

                
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

                //Get the containers for the account. Continuation token starts null, and will be used if the listing is long and 
                //additional calls are needed
                BlobContinuationToken token = null;
                IEnumerable<CloudBlobContainer> containers;
                do
                {
                    containers = blobClient.ListContainersSegmentedAsync(token).Result.Results;

                }while (token != null);
            
                //reset continuation token
                token = null;

                //Get the blobs for the container. Continuation token starts null, and will be used if the listing is long and 
                //additional calls are needed
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
                //TODO: Should make the error handling a bit more robust
                log.LogInformation(ex.InnerException.ToString());
                return null;
            }
        }
    }
}