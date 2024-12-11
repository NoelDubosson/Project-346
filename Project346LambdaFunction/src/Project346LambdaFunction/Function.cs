using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

public class Function
{
    private static readonly string InputBucket = "josiaschweizer-input-bucket";
    private static readonly string OutputBucket = "josiaschweizer-output-bucket";
    private static readonly IAmazonS3 S3Client = new AmazonS3Client();

    public async void FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        foreach (var record in evnt.Records)
        {
            string inputKey = record.S3.Object.Key;
            context.Logger.LogLine($"Processing file: {inputKey}");

            try
            {
                await ConvertCsvToJsonAsync(InputBucket, OutputBucket, inputKey);
            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"Error processing file {inputKey}: {ex.Message}");
            }
        }
    }

    private static async System.Threading.Tasks.Task ConvertCsvToJsonAsync(string inputBucket, string outputBucket, string inputKey)
    {
        string localCsvPath = "/tmp/input.csv";
        string localJsonPath = "/tmp/output.json";

        try
        {
            var downloadRequest = new GetObjectRequest
            {
                BucketName = inputBucket,
                Key = inputKey
            };

            using (var response = await S3Client.GetObjectAsync(downloadRequest))
            using (var responseStream = response.ResponseStream)
            using (var fileStream = new FileStream(localCsvPath, FileMode.Create, FileAccess.Write))
            {
                await responseStream.CopyToAsync(fileStream);
            }

            var jsonData = new List<Dictionary<string, string>>();

            using (var reader = new StreamReader(localCsvPath))
            {
                string[] headers = null;
                while (!reader.EndOfStream)
                {
                    string line = reader.ReadLine();
                    if (headers == null)
                    {
                        headers = line.Split(",");
                    }
                    else
                    {
                        var values = line.Split(",");
                        var record = new Dictionary<string, string>();
                        for (int i = 0; i < headers.Length; i++)
                        {
                            record[headers[i]] = values[i];
                        }
                        jsonData.Add(record);
                    }
                }
            }

            File.WriteAllText(localJsonPath, Newtonsoft.Json.JsonConvert.SerializeObject(jsonData, Newtonsoft.Json.Formatting.Indented));

            var outputKey = inputKey.Replace(".csv", ".json");
            var uploadRequest = new PutObjectRequest
            {
                BucketName = outputBucket,
                Key = outputKey,
                FilePath = localJsonPath
            };

            await S3Client.PutObjectAsync(uploadRequest);

            Console.WriteLine($"Successfully processed {inputKey} and saved as {outputKey} in {outputBucket}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing file {inputKey}: {ex.Message}");
            throw;
        }
        finally
        {
            if (File.Exists(localCsvPath)) File.Delete(localCsvPath);
            if (File.Exists(localJsonPath)) File.Delete(localJsonPath);
        }
    }
}
