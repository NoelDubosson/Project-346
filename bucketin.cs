using System;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;
 
public class InputBucketManager
{
    private readonly string _bucketName;
    private readonly AmazonS3Client _s3Client;
 
    public InputBucketManager(string bucketName, RegionEndpoint region)
    {
        _bucketName = bucketName;
        _s3Client = new AmazonS3Client(region);
    }
 
    public async System.Threading.Tasks.Task UploadFileAsync(string localFilePath, string keyName)
    {
        try
        {
            var fileTransferUtility = new TransferUtility(_s3Client);
            await fileTransferUtility.UploadAsync(localFilePath, _bucketName, keyName);
            Console.WriteLine($"Datei {localFilePath} erfolgreich nach {_bucketName} hochgeladen.");
        }
        catch (Exception e)
        {
            Console.WriteLine($"Fehler beim Hochladen der Datei: {e.Message}");
        }
    }
}