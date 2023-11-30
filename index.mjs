import { Storage } from "@google-cloud/storage";
import axios from "axios";
import Mailgun from "mailgun.js";
import FormData from "form-data";
import * as uuid from "uuid";
import AWS from "aws-sdk";
const mailgun = new Mailgun(FormData);
const dynamoDB = new AWS.DynamoDB.DocumentClient();

const mailgunApiKey = process.env.mailgunAPI;
const mailgunDomain = process.env.mailgunDomain;
const mg = mailgun.client({ username: "api", key: mailgunApiKey });
const storage = new Storage({
  credentials: JSON.parse(process.env.privateKey),
});

export const handler = async (event) => {
  if (event.Records && event.Records.length > 0 && event.Records[0].Sns) {
    const snsMessage = JSON.parse(event.Records[0].Sns.Message);
    const fileName = snsMessage.url.substring(
      snsMessage.url.lastIndexOf("/") + 1
    );
    const fileloc =
      "Assignment - " +
      snsMessage.assignment_id +
      " / " +
      snsMessage.email +
      " / " +
      snsMessage.num_of_attempts +
      " / " +
      fileName;
    let dynamoDBParams = {
      TableName: process.env.dynamodbName,
      Item: {
        id: uuid.v4(),
        assignment_id: snsMessage.assignment_id,
        email: snsMessage.email,
        num_attempts: snsMessage.num_attempts,
        file_location: fileloc,
        timestamp: new Date().toISOString(),
      },
    };
    const GCS_BUCKET = process.env.bucketName;
    const url = snsMessage.url;
    const bucketObj = storage.bucket(GCS_BUCKET);
    const file = bucketObj.file(fileloc);
    try {
      const fileCon = await downloadFile(url);
      await file.save(fileCon);
      mg.messages
        .create(mailgunDomain, {
          from: `CSYE6225 <shweta@${mailgunDomain}>`,
          to: [snsMessage.email],
          subject: "CSYE 6225 Cloud Computing - Assignment submission accepted",
          text: `We've successfully received and confirmed your submission at ${fileloc}. Appreciate it.`,
        })
        .then((msg) => console.log(msg))
        .catch((err) => console.error(err));
      const response = {
        statusCode: 200,
        body: JSON.stringify("Lambda function successfull"),
      };

      await dynamoDB.put(dynamoDBParams).promise();
      return response;
    } catch (error) {
      mg.messages
        .create(mailgunDomain, {
          from: `CSYE6225 <shweta@${mailgunDomain}>`,
          to: [snsMessage.email],
          subject: "CSYE 6225 Cloud Computing - Assignment submission failed",
          text: "The download of your submission failed. Please check the URL and submit it again.",
        })
        .then((msg) => console.log(msg))
        .catch((err) => console.error(err));
      const response = {
        statusCode: 400,
        body: JSON.stringify("Invalid event source"),
      };
      await dynamoDB.put(dynamoDBParams).promise();
      return response;
    }
  }
};

async function downloadFile(url) {
  const response = await axios.get(url, { responseType: "arraybuffer" });
  return Buffer.from(response.data, "binary");
}
