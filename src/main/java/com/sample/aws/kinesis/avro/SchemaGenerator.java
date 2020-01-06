package com.sample.aws.kinesis.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SchemaGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaGenerator.class);

    public static void generateAndSave() {

        LOGGER.info("Generating Avro Schema ... ");

        Schema schema = SchemaBuilder.record("Sample")
                .namespace("com.sample.aws.kinesis")
                .fields()
                .requiredString("id")
                .requiredString("content")
                .endRecord();

        LOGGER.info("Saving Schema to a file ... ");

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter("src/main/resources/sample-schema.avsc"));
            writer.write(schema.toString());
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
