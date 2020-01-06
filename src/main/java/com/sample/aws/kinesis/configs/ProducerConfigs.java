package com.sample.aws.kinesis.configs;

import com.amazonaws.auth.*;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.sample.aws.kinesis.constants.EventConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerConfigs {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConfigs.class);

    private KinesisProducerConfiguration kinesisProducerConfig;
    private String streamName;

    public ProducerConfigs(Properties properties) {

        LOGGER.info("Setting Kinesis Producer Configurations ... ");

        this.kinesisProducerConfig = new KinesisProducerConfiguration();

        AWSCredentials awsCredentials = new BasicAWSCredentials(
                properties.getProperty(EventConstants.KinesisConstants.ACCESS_KEY),
                properties.getProperty(EventConstants.KinesisConstants.SECRET_KEY));
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        this.kinesisProducerConfig.setCredentialsProvider(new AWSCredentialsProviderChain(awsCredentialsProvider));

        this.kinesisProducerConfig.setRegion(properties.getProperty(EventConstants.KinesisConstants.AWS_REGION));

        this.kinesisProducerConfig.setMaxConnections(EventConstants.KinesisDefaultConfigConstants.MAX_CONNECTIONS);
        this.kinesisProducerConfig.setRecordMaxBufferedTime(EventConstants.KinesisDefaultConfigConstants.RECORD_MAX_BUFFERED_TIME);
        this.kinesisProducerConfig.setConnectTimeout(EventConstants.KinesisDefaultConfigConstants.CONNECTION_TIME_OUT);

        this.streamName = properties.getProperty(EventConstants.KinesisConstants.STREAM_NAME);
    }

    public KinesisProducerConfiguration getKinesisProducerConfig() {
        return kinesisProducerConfig;
    }

    public String getStreamName() {
        return streamName;
    }
}
