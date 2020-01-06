package com.sample.aws.kinesis.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroSerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSerializer.class);

    public static byte[] serialize(String jsonPayload, Schema schema) throws IOException {

        LOGGER.info("Generating serialized data !!! ");

        DatumReader<Object> datumReader = new GenericDatumReader<Object>(schema);
        DatumWriter<Object> datumWriter = new GenericDatumWriter<Object>(schema);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);

        JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, jsonPayload);

        Object datum = datumReader.read(null, jsonDecoder);

        datumWriter.write(datum, binaryEncoder);
        binaryEncoder.flush();

        byte[] data = outputStream.toByteArray();
        outputStream.close();

        return data;
    }

}
