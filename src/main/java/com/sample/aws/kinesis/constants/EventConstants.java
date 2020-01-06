package com.sample.aws.kinesis.constants;

public class EventConstants {

    public static class KinesisConstants {

        public static final String STREAM_NAME = "stream.name";
        public static final String ACCESS_KEY = "access.key";
        public static final String SECRET_KEY = "secret.key";
        public static final String AWS_REGION = "aws.region";
    }

    // Inner class for default constant values related to Kinesis producer & consumer configurations
    public static class KinesisDefaultConfigConstants {

        // Producer specific default config values
        public static final int MAX_CONNECTIONS = 25;
        public static final int CONNECTION_TIME_OUT = 60000;
        public static final long RECORD_MAX_BUFFERED_TIME = 3000;
    }
}
