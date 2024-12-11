const AWS = require('aws-sdk');
const S3 = new AWS.S3();
const readline = require('readline');

const INPUT_BUCKET = "josiaschweizer-input-bucket";
const OUTPUT_BUCKET = "josiaschweizer-output-bucket";

exports.handler = async (event) => {
    for (const record of event.Records) {
        const inputKey = record.s3.object.key;
        console.log(`Processing file: ${inputKey}`);

        try {
            await convertCsvToJson(INPUT_BUCKET, OUTPUT_BUCKET, inputKey);
        } catch (error) {
            console.error(`Error processing file ${inputKey}:`, error);
        }
    }
};

async function convertCsvToJson(inputBucket, outputBucket, inputKey) {
    const localCsvPath = '/tmp/input.csv';
    const localJsonPath = '/tmp/output.json';

    try {
        const csvFile = await S3.getObject({
            Bucket: inputBucket,
            Key: inputKey,
        }).promise();

        const csvContent = csvFile.Body.toString('utf-8');
        const lines = csvContent.split('\n');

        const headers = lines[0].split(',');
        const jsonData = [];

        for (let i = 1; i < lines.length; i++) {
            if (!lines[i].trim()) continue;
            const values = lines[i].split(',');
            const record = {};

            headers.forEach((header, index) => {
                record[header.trim()] = values[index]?.trim();
            });

            jsonData.push(record);
        }

        const jsonContent = JSON.stringify(jsonData, null, 4);
        require('fs').writeFileSync(localJsonPath, jsonContent);


        const outputKey = inputKey.replace('.csv', '.json');
        await S3.upload({
            Bucket: outputBucket,
            Key: outputKey,
            Body: jsonContent,
            ContentType: 'application/json',
        }).promise();

        console.log(`Successfully processed ${inputKey} and saved as ${outputKey} in ${outputBucket}`);
    } catch (error) {
        console.error(`Error processing file ${inputKey}:`, error);
        throw error;
    }
}
