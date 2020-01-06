package com.sample.aws.kinesis;

import com.amazonaws.services.kinesis.producer.DaemonException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.sample.aws.kinesis.configs.ProducerConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Properties;

public class DataPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisher.class);

    private com.amazonaws.services.kinesis.producer.KinesisProducer producer;
    private ProducerConfigs producerConfigs;

    public DataPublisher(Properties properties) {

        LOGGER.info("Creating the Kinesis Event Producer ... ");

        this.producerConfigs = new ProducerConfigs(properties);
        this.producer = new com.amazonaws.services.kinesis.producer.KinesisProducer(producerConfigs.getKinesisProducerConfig());
    }

    public void submitEvent(ByteBuffer payload, FutureCallback callback) {

        LOGGER.info("Putting data to stream ... ");
        FutureCallback<UserRecordResult> futureCallback = callback;
        ListenableFuture<UserRecordResult> f = producer.addUserRecord(
                producerConfigs.getStreamName(), "my-partition-key", payload);
        Futures.addCallback(f, futureCallback);
    }

    public void shutdown() {
        LOGGER.info("Terminating kinesis producer client");
        try {
            // To ensure all records are sent before killing the child, need to call flushSync()
            producer.flushSync();
            // Terminate the child process without exiting the JVM
            producer.destroy();
        } catch (DaemonException e) {
            LOGGER.warn("Kinesis producer child process is already dead");
        }
    }
}
