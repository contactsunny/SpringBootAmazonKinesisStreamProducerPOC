package com.contactsunny.poc.SpringBootAmazonKinesisStreamProducerPOC;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.ByteBuffer;

@SpringBootApplication
public class App implements CommandLineRunner {

    @Value("${aws.auth.accessKey}")
    private String awsAccessKey;

    @Value("${aws.auth.secretKey}")
    private String awsSecretKey;

    @Value("${aws.kinesis.dataStream.name}")
    private String kinesisDataStreamName;

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);

        AmazonKinesis amazonKinesis = AmazonKinesisClientBuilder
                .standard().withRegion(Regions.US_WEST_2)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

        JSONObject messageJson = new JSONObject();
        messageJson.put("key1", "We are testing Amazon Kinesis Firehose!");
        messageJson.put("integerKey", 123);
        messageJson.put("booleanKey", true);
        messageJson.put("anotherString", "This should work!");

        logger.info("Message to Data Stream: " + messageJson.toString());

        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(kinesisDataStreamName);
        putRecordRequest.setPartitionKey("myPartitionKey");
        putRecordRequest.withData(ByteBuffer.wrap(messageJson.toString().getBytes()));

        PutRecordResult putRecordResult = amazonKinesis.putRecord(putRecordRequest);

        logger.info("Record sequence number: " + putRecordResult.getSequenceNumber());
    }
}
