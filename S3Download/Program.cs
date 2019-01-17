using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace S3Download
{
    class TrackMPUUsingHighLevelAPITest
    {
        private static IAmazonS3 s3Client;

        private static ConcurrentDictionary<string, string> progressList = new ConcurrentDictionary<string, string>();
        private static int _cursorTop = 0;

        public static void Main(string[] args)
        {
            if (args == null || args.Length != 6)
            {
                Console.WriteLine("Usage: S3Download.exe <bucketName> <comma-separated-source-files-list-or-single-file> " +
                    "<comma-separated-target-folders-list-or-single-folder>" +
                    " <accessKeyId> <secretAccessKey> <serviceURL>");
                return;
            }

            _cursorTop = Console.CursorTop;

            string bucketName = args[0];
            var files = args[1].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Select(p => p.Trim()).ToArray();
            var folders = args[2].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Select(p => p.Trim()).ToArray();
            string accessKeyId = args[3];
            string secretAccessKey = args[4];
            string serviceURL = args[5];

            AWSCredentials сredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);

            AmazonS3Config config = new AmazonS3Config()
            {
                ServiceURL = serviceURL,
                UseHttp = true,
                MaxErrorRetry = 5
            };

            s3Client = new AmazonS3Client(сredentials, config);

            var watch = new Stopwatch();
            watch.Start();

            List<Task> TaskList = new List<Task>();
            foreach (var file in files)
            {
                foreach (var folder in folders)
                {
                    var target = Path.Combine(folder, file);
                    if (File.Exists(target))
                    {
                        Console.WriteLine(target + " already exists, skipping...");
                        _cursorTop = Console.CursorTop;
                    }
                    else
                    {
                        TaskList.Add(DownloadAsync(bucketName, file, Path.Combine(folder, file)));
                    }
                }
            }

            Task.WaitAll(TaskList.ToArray());
            watch.Stop();
            Console.CursorTop = _cursorTop + TaskList.Count() + 1;
            Console.WriteLine($"Completed in {watch.Elapsed.Hours:D2}:{watch.Elapsed.Minutes:D2}:{watch.Elapsed.Seconds:D2}");
        }

        private static async Task DownloadAsync(string bucketName, string keyName, string filePath)
        {
            try
            {
                var fileTransferUtility = new TransferUtility(s3Client);

                var downloadRequest =
                    new TransferUtilityDownloadRequest
                    {
                        BucketName = bucketName,
                        FilePath = filePath,
                        Key = keyName
                    };

                downloadRequest.WriteObjectProgressEvent += new EventHandler<WriteObjectProgressArgs>(DownloadPartProgressEvent);
                await fileTransferUtility.DownloadAsync(downloadRequest);
                Console.WriteLine("Upload completed");
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }
        }

        static void DownloadPartProgressEvent(object sender, WriteObjectProgressArgs e)
        {
            int cursorTop = _cursorTop;
            progressList[e.FilePath] = e.IsCompleted ? "Completed".PadRight(60) : $"{e.TransferredBytes:N}/{e.TotalBytes:N} bytes ({e.PercentDone.ToString()}% done)";
            foreach (var key in progressList.Keys)
            {
                Console.CursorTop = cursorTop;
                Console.CursorVisible = false;
                Console.WriteLine($"{key}: {progressList[key]}");
                cursorTop++;
            }
        }
    }
}