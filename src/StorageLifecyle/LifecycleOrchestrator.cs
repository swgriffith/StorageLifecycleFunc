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
       // private static List<CloudBlockBlob> m_blobs;

        [FunctionName("LifecycleOrchestrator")]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context)
        {
            try
            {

                // Start the first step in the orchestration to get the blob list
                var getBlobsResult = await context.CallActivityAsync<List<blob>>("LifecycleOrchestrator_GetBlobs", null);
                var archveBlobsResult = await context.CallActivityAsync<List<blob>>("LifecycleOrchestrator_ArchiveBlobs", getBlobsResult);

                return "Success";
            }
            catch (System.Exception ex)
            {
                //TODO: Should make the error handling a bit more robust
                //context.log.LogInformation(ex.InnerException.ToString());
                return null;
                throw ex;
            }
        }

        [FunctionName("LifecycleOrchestrator_GetBlobs")]
        public static async Task<List<blob>> GetBlobs([ActivityTrigger] string name, ILogger log)
        {
            try
            {

            //Get the blob storage name and access key
            //You can use key vault to store these https://azure.microsoft.com/en-us/blog/simplifying-security-for-serverless-and-web-apps-with-azure-functions-and-app-service/
            //TODO: Should change out code to use a SAS Policy and SAS Token in the future for added security
            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();

            //Get Storage Account details from config files or env variables
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

            } while (token != null);

            //reset continuation token
            token = null;

            //Get the blobs for the container. Continuation token starts null, and will be used if the listing is long and 
            //additional calls are needed
            List<blob> blobListResults = new List<blob>();

            //Get the prefix to be used to identify blobs to archive
            string archiveFilePrefix = config["archiveFilePrefix"];

            //Iterate through all containers and pull all blob items
            foreach (var c in containers)
            {
                do
                {
                    var result = c.ListBlobsSegmentedAsync(token);
                    token = result.Result.ContinuationToken;
                    foreach (IListBlobItem blob in result.Result.Results)
                    {
                        //ListBlobsSegmentedAsync returns directories as well, so we need to skip those
                        //To address, we attempt to cast to a block blob object and skip if we fail
                        //TODO: Should be a more elegant way to handle this
                        CloudBlockBlob blockBlob;
                        try
                        {
                            blockBlob = (CloudBlockBlob)blob;

                            //Check the file prefix and if the file is Hot or Cool and if so add to the list.
                            if (blockBlob.Name.StartsWith(archiveFilePrefix) &&
                                (blockBlob.Properties.StandardBlobTier == StandardBlobTier.Hot
                                || blockBlob.Properties.StandardBlobTier == StandardBlobTier.Cool))
                            {
                                    blob item = new blob();
                                    item.name = blockBlob.Name;
                                    item.container = blockBlob.Container.Name;
                                    item.absolutePath = blockBlob.StorageUri.PrimaryUri.AbsolutePath;
                                    item.tier = blockBlob.Properties.StandardBlobTier;
                                    item.size = blockBlob.Properties.Length;
                                    blobListResults.Add(item);
                            }
                        }
                        catch (System.Exception)
                        {
                            //not a block blob (ex. directory), do nothing
                        }

                    }
                } while (token != null);
            }

            return blobListResults;

        }
            catch (System.Exception ex)
            {
                //TODO: Should make the error handling a bit more robust
                log.LogInformation(ex.InnerException.ToString());
                return null;
            }

}

        [FunctionName("LifecycleOrchestrator_ArchiveBlobs")]
        public static void ArchiveBlobs([ActivityTrigger] List<blob> blobsToArchive, ILogger log)
        {
            //Get the blob storage name and access key
            //You can use key vault to store these https://azure.microsoft.com/en-us/blog/simplifying-security-for-serverless-and-web-apps-with-azure-functions-and-app-service/
            //TODO: Should change out code to use a SAS Policy and SAS Token in the future for added security
            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();

            //Get Storage Account details from config files or env variables
            string saName = config["storageaccountname"];
            string saKey = config["storageaccountkey"];


            //Connect to the storage account
            CloudStorageAccount storageAccount = new CloudStorageAccount(
            new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(
                saName,
                saKey), true);


            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            //Loop through the blobs and move to archive tier
            foreach (blob item in blobsToArchive)
            {
                var container = blobClient.GetContainerReference(item.container);
                CloudBlockBlob blob = container.GetBlockBlobReference(item.name);
                blob.SetStandardBlobTierAsync(StandardBlobTier.Archive);
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


        public class blob
        {
            public string container { get; set; }
            public string name { get; set; }
            public string absolutePath { get; set; }
            public long size { get; set; }
            public StandardBlobTier? tier { get; set; }
        }
    }
}