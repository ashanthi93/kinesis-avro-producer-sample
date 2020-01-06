package com.sample.aws.kinesis;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.sample.aws.kinesis.avro.AvroSerializer;
import org.apache.avro.Schema;
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

public class SerializedDataPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(SerializedDataPublisher.class);

    public static void main(String args[]) {

        Properties properties = new Properties();
        JSONParser jsonParser = new JSONParser();

        try {
            LOGGER.info("Loading properties file ...");
            InputStream producerConfigInputStream = DataPublisher.class.getResourceAsStream("/ProducerConfig.properties");
            properties.load(producerConfigInputStream);

            // Initiate SampleProducer object
            DataPublisher dataPublisher = new DataPublisher(properties);

            LOGGER.info("Loading avro schema file ...");
            InputStream schemaInputStream = DataPublisher.class.getResourceAsStream("/payload.json.avro");

            // Parse schema
            final Schema payloadSchema = new Schema.Parser().parse(schemaInputStream);

            FileReader reader = new FileReader("/home/aabe1096/SYSCO/Kinesis Video/producer-sample/src/main/resources/payload.json");

            //Read JSON file
            Object obj = jsonParser.parse(reader);

            JSONArray jsonArray = (JSONArray) obj;

            String jsonPayload = jsonArray.toJSONString();

            // Serializing the record
            byte[] serializedBytes = AvroSerializer.serialize(jsonPayload, payloadSchema);

            ByteBuffer payloadBytes = ByteBuffer.wrap(serializedBytes);

            System.out.println("No.of serialized bytes: " + payloadBytes.remaining());

            LOGGER.info("Generated serialized data !!! ");

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
