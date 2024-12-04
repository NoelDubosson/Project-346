using System;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
 
public class OutputBucketManager
{
    private readonly string _bucketName;
    private readonly AmazonS3Client _s3Client;
 
    public OutputBucketManager(string bucketName, RegionEndpoint region)
    {
        _bucketName = bucketName;
        _s3Client = new AmazonS3Client(region);
    }
 
    public async System.Threading.Tasks.Task DownloadFileAsync(string keyName, string localFilePath)
    {
        try
        {
            var request = new GetObjectRequest
            {
                BucketName = _bucketName,
                Key = keyName
            };
 
            using (var response = await _s3Client.GetObjectAsync(request))
            {
                await response.WriteResponseStreamToFileAsync(localFilePath, false, default);
                Console.WriteLine($"Datei {keyName} erfolgreich von {_bucketName} heruntergeladen.");
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"Fehler beim Herunterladen der Datei: {e.Message}");
        }
    }
}