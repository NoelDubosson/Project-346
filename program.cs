using System;
using System.IO;
using Amazon;
 
class Program
{
    static async System.Threading.Tasks.Task Main(string[] args)
    {
        // Bucket-Namen und Datei-Pfade
        const string inputBucketName = "mein-input-bucket";
        const string outputBucketName = "mein-output-bucket";
        const string localCsvFile = "input.csv";
        const string localJsonFile = "output.json";
        const string s3InputKey = "input.csv";
        const string s3OutputKey = "output.json";
 
        // Region für AWS (anpassen falls nötig)
        var region = RegionEndpoint.EUWest1;
 
        // Manager für Input- und Output-Buckets erstellen
        var inputBucketManager = new InputBucketManager(inputBucketName, region);
        var outputBucketManager = new OutputBucketManager(outputBucketName, region);
 
        try
        {
            // Schritt 1: Lokale CSV-Datei in den Input-Bucket hochladen
            Console.WriteLine("CSV-Datei wird in den Input-Bucket hochgeladen...");
            await inputBucketManager.UploadFileAsync(localCsvFile, s3InputKey);
 
            // Schritt 2: Auf Lambda-Verarbeitung warten (nur für Demo)
            Console.WriteLine("Warten auf die Verarbeitung durch Lambda...");
            await System.Threading.Tasks.Task.Delay(5000); // Simulierter Wartezeitraum
 
            // Schritt 3: JSON-Datei aus dem Output-Bucket herunterladen
            Console.WriteLine("JSON-Datei wird aus dem Output-Bucket heruntergeladen...");
            await outputBucketManager.DownloadFileAsync(s3OutputKey, localJsonFile);
 
            Console.WriteLine("Verarbeitung abgeschlossen. Die Datei wurde gespeichert unter:");
            Console.WriteLine(Path.GetFullPath(localJsonFile));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ein Fehler ist aufgetreten: {ex.Message}");
        }
    }
}