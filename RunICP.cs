
using System.IO;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System;
using System.Diagnostics;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using System.Collections.Generic;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;

namespace TrimbleICP.Functions
{
    public class RunICPRequest
    {
        public string ScanPoints { get; set; }
        public string ModelPoints { get; set; }
    }

    public class RunICPResponse
    {
        public string Content { get; set; }
    }

    public static class RunICP
    {
        // Batch credentials
        private const string BatchAccountName = "alignment";
        private const string BatchAccountKey = "lu01qM6MPxpxUSU1Ib09goI1TC89nsC+iu9chgNpfG78soHpzMpr8Gsqfc2xdUVR4tnOjSxDSD0wAGnRA9yVaA==";
        private const string BatchAccountUrl = "https://alignment.westeurope.batch.azure.com";

        // Storage account credentials
        private const string StorageAccountName = "meshinfo";
        private const string StorageAccountKey = "f202AnxCfRlySOeQH1ERf3KibKNwov58ntVpBbNUkhtqFZVtf5NVsA8/c2FB/j1cbmC/kadVnl9V0J0rictTDg==";
        private static string StorageConnectionString => String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", StorageAccountName, StorageAccountKey);

        // Storage account names
        private const string InputContainerName = "inputs";
        private const string ModelPointsFileName = "model_points.csv";
        private const string ScanPointsFileName = "scan_points.csv";

        // Pool settings
        private const string PoolName = "TrimbleICP_Pool_v2";
        private const string PoolVmSize = "Standard_D2s_v3";
        private const string NodeOsPublisher = "MicrosoftWindowsServer";
        private const string NodeOsOffer = "WindowsServer";
        private const string NodeOsSku = "2016-Datacenter";
        private const string NodeOsVersion = "latest";
        private const string NodeAgentSkuId = "batch.node.windows amd64";
        public const int PoolDedicatedNodeCount = 0;
        public const int PoolLowPriorityNodeCount = 1;
        public const int PoolMaxTasksPerNodeCount = 3;
        public const int PoolMaxAutoscalingNumofVMs = 3;
        public static string PoolAutoscaleFormula =
            $"startingNumberOfVMs = {PoolLowPriorityNodeCount};" +
            $"maxNumberofVMs = {PoolMaxAutoscalingNumofVMs};" +
            $"pendingTaskSamplePercent = $PendingTasks.GetSamplePercent(180 * TimeInterval_Second);" +
            $"pendingTaskSamples = pendingTaskSamplePercent < 70 ? startingNumberOfVMs : (avg($PendingTasks.GetSample(180 * TimeInterval_Second))/{PoolMaxTasksPerNodeCount});" +
            $"$TargetLowPriorityNodes=min(maxNumberofVMs, pendingTaskSamples+1);";

        // Job settings
        private const bool CreateNewJobOnEveryRun = true;
        private const string JobNamePrefix = "TrimbleICP_RunICP_Job_v2";
        private const string TaskNamePrefix = "TrimbleICP_RunICP_Task";
        private static Random _rand = new Random();

        // App Settings
        private const string ApplicationPackageId = "ICP";
        private const string ApplicationPackageVersion = "03";
        private const string ApplicationOutputFileName = "AlignmentInfo.txt";

        [FunctionName("RunICP")]
        public async static Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)]RunICPRequest req, TraceWriter log)
        {
            var startTime = DateTimeOffset.Now;
            Console.WriteLine("Dunction started: {0}", startTime);
            Console.WriteLine();
            var timer = new Stopwatch();
            timer.Start();

            if (String.IsNullOrEmpty(BatchAccountName) ||
                String.IsNullOrEmpty(BatchAccountKey) ||
                String.IsNullOrEmpty(BatchAccountUrl) ||
                String.IsNullOrEmpty(StorageAccountName) ||
                String.IsNullOrEmpty(StorageAccountKey))
            {
                throw new InvalidOperationException("One or more account credential strings have not been populated. Please ensure that your Batch and Storage account credentials have been specified.");
            }

            // Create batch job and task names
            var nameUnifier = startTime.ToUniversalTime().ToString("yyyyMMdd_HHmmss_ffff");
            var jobName = JobNamePrefix + (CreateNewJobOnEveryRun ? $"__{nameUnifier}" : String.Empty);
            var taskName = TaskNamePrefix + $"__{nameUnifier}";

            // Create Blob client
            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();

            // Create input container
            var container = blobClient.GetContainerReference(InputContainerName);
            await container.CreateIfNotExistsAsync();

            // Upload the data files to Azure Storage. This is the data that will be processed by each of the tasks that are
            // executed on the compute nodes within the pool.
            var inputFiles = new List<ResourceFile>();

            var blobFileNamePrefix = $"{jobName}/{taskName}/";
            inputFiles.Add(await UploadStringToContainer(blobClient, InputContainerName, blobFileNamePrefix + ModelPointsFileName, ModelPointsFileName, req.ModelPoints));
            inputFiles.Add(await UploadStringToContainer(blobClient, InputContainerName, blobFileNamePrefix + ScanPointsFileName, ScanPointsFileName, req.ScanPoints));

            // Get a Batch client using account creds

            var batchCredentials = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);

            using (var batchClient = BatchClient.Open(batchCredentials))
            {
                // Create a Batch pool, VM configuration, Windows Server image
                Console.WriteLine("Creating pool [{0}]...", PoolName);

                var imageReference = new ImageReference(
                    publisher: NodeOsPublisher,
                    offer: NodeOsOffer,
                    sku: NodeOsSku,
                    version: NodeOsVersion);

                var virtualMachineConfiguration =
                new VirtualMachineConfiguration(
                    imageReference: imageReference,
                    nodeAgentSkuId: NodeAgentSkuId);

                try
                {
                    var pool = batchClient.PoolOperations.CreatePool(
                        poolId: PoolName,
                        virtualMachineSize: PoolVmSize,
                        virtualMachineConfiguration: virtualMachineConfiguration);

                    if (!String.IsNullOrEmpty(PoolAutoscaleFormula))
                    {
                        pool.AutoScaleEnabled = true;
                        pool.AutoScaleFormula = PoolAutoscaleFormula;
                        pool.AutoScaleEvaluationInterval = TimeSpan.FromMinutes(5);
                    }
                    else
                    {
                        pool.TargetDedicatedComputeNodes = PoolDedicatedNodeCount;
                        pool.TargetLowPriorityComputeNodes = PoolLowPriorityNodeCount;
                    }

                    pool.MaxTasksPerComputeNode = PoolMaxTasksPerNodeCount;

                    pool.Commit();
                }
                catch (BatchException be)
                {
                    // Accept the specific error code PoolExists as that is expected if the pool already exists
                    if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.PoolExists)
                    {
                        Console.WriteLine("The pool {0} already existed when we tried to create it", PoolName);
                    }
                    else
                    {
                        throw; // Any other exception is unexpected
                    }
                }

                // Create a Batch job
                Console.WriteLine("Creating job [{0}]...", jobName);

                try
                {
                    CloudJob job = batchClient.JobOperations.CreateJob();
                    job.Id = jobName;
                    job.PoolInformation = new PoolInformation { PoolId = PoolName };

                    job.Commit();
                }
                catch (BatchException be)
                {
                    // Accept the specific error code JobExists as that is expected if the job already exists
                    if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.JobExists)
                    {
                        Console.WriteLine("The job {0} already existed when we tried to create it", JobNamePrefix);
                    }
                    else
                    {
                        throw; // Any other exception is unexpected
                    }
                }

                // Adding task
                var taskCommandLine = String.Format($"cmd /c %AZ_BATCH_APP_PACKAGE_ICP%\\ICP_PCL.exe %AZ_BATCH_TASK_WORKING_DIR% {ScanPointsFileName} {ModelPointsFileName} 1 1");
                var task = new CloudTask(taskName, taskCommandLine)
                {
                    ResourceFiles = inputFiles,
                    ApplicationPackageReferences = new List<ApplicationPackageReference>
                    {
                        new ApplicationPackageReference { ApplicationId = ApplicationPackageId, Version = ApplicationPackageVersion }
                    }
                };

                await batchClient.JobOperations.AddTaskAsync(jobName, task);

                // Monitor task
                var stateMonitor = batchClient.Utilities.CreateTaskStateMonitor();

                task = batchClient.JobOperations.GetTask(jobName, taskName);
                stateMonitor.WaitAll(new List<CloudTask> { task }, TaskState.Completed, timeout: TimeSpan.FromMinutes(5));


                Console.WriteLine("Task completed");
                try
                {
                    var result = new RunICPResponse
                    {
                        Content = (await task.GetNodeFileAsync("wd\\" + ApplicationOutputFileName)).ReadAsString()
                    };

                    return new OkObjectResult(result);
                }
                catch (Exception e)
                {
                    return new NotFoundObjectResult(e);
                }
            }
        }

        /// <summary>
        /// Uploads the specified file to the specified Blob container.
        /// </summary>
        /// <param name="blobClient">A <see cref="CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the blob storage container to which the file should be uploaded.</param>
        /// <param name="filePath">The full path to the file to upload to Storage.</param>
        /// <returns>A ResourceFile instance representing the file within blob storage.</returns>
        private async static Task<ResourceFile> UploadFileToContainer(CloudBlobClient blobClient, string containerName, string filePath)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);

            string blobName = Path.GetFileName(filePath);

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
            await blobData.UploadFromFileAsync(filePath);

            // Set the expiry time and permissions for the blob shared access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            // Construct the SAS URL for blob
            string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

            return new ResourceFile(blobSasUri, blobName);
        }

        /// <summary>
        /// Uploads the specified string to the specified Blob container.
        /// </summary>
        /// <param name="blobClient">A <see cref="CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the blob storage container to which the file should be uploaded.</param>
        /// <param name="blobName">The name of the blob in Storage (accepts /).</param>
        /// <param name="batchWdFilePath">The path to the file on Batch VM.</param>
        /// <param name="content">File content as a string</param>
        /// <returns>A ResourceFile instance representing the file within blob storage.</returns>
        private async static Task<ResourceFile> UploadStringToContainer(CloudBlobClient blobClient, string containerName, string blobName, string batchWdFilePath, string content)
        {
            Console.WriteLine("Uploading content to container [{0}/{1}] with Batch working directory file path {2}...", containerName, blobName, batchWdFilePath);

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
            await blobData.UploadTextAsync(content);

            // Set the expiry time and permissions for the blob shared access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(23),
                Permissions = SharedAccessBlobPermissions.Read
            };

            // Construct the SAS URL for blob
            string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

            return new ResourceFile(blobSasUri, batchWdFilePath);
        }
    }
}
