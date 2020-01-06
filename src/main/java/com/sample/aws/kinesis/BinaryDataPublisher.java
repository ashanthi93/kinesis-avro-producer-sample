package com.sample.aws.kinesis;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;

public class BinaryDataPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinaryDataPublisher.class);

    public static void main(String args[]) {

        Properties properties = new Properties();
        JSONParser jsonParser = new JSONParser();

        try {
            LOGGER.info("Loading properties file ...");
            InputStream producerConfigInputStream = DataPublisher.class.getResourceAsStream("/ProducerConfig.properties");
            properties.load(producerConfigInputStream);

            // Initiate SampleProducer object
            DataPublisher dataPublisher = new DataPublisher(properties);

            FileReader reader = new FileReader("/home/aabe1096/SYSCO/Kinesis Video/producer-sample/src/main/resources/payload.json");

            //Read JSON file
            Object obj = jsonParser.parse(reader);

            JSONArray jsonArray = (JSONArray) obj;

            String jsonPayload = jsonArray.toJSONString();

            ByteBuffer payloadBytes = ByteBuffer.wrap(jsonPayload.getBytes());

            System.out.println("No.of bytes: " + payloadBytes.remaining());

            // Futures to get the status
            FutureCallback<UserRecordResult> futureCallback = new FutureCallback<UserRecordResult>() {
                public void onSuccess(UserRecordResult userRecordResult) {
                    LOGGER.info("Data sent to stream ===== > Sequence id " + userRecordResult.getSequenceNumber());
                }

                public void onFailure(Throwable arg) {
                    LOGGER.info("Unable to send data to stream");
                }
            };

            // Submit data to Kinesis Stream
            dataPublisher.submitEvent(payloadBytes, futureCallback);

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOGGER.error("context", e);
            }

            // Shutdown the producer
            dataPublisher.shutdown();

        } catch (IOException e) {
            LOGGER.error("context", e);
        } catch (ParseException e) {
            LOGGER.error("context", e);
        }
    }

}
