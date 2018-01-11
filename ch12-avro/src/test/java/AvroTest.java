import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class AvroTest {

    @Test
    public void testInt() throws IOException {
        Schema schema = new Schema.Parser().parse("\"int\"");

        int datum = 163;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<Integer> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();

        DatumReader<Integer> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get()
                .binaryDecoder(out.toByteArray(), null);
        Integer result = reader.read(null, decoder);
        assertThat(result, is(163));

        try {
            reader.read(null, decoder);
            fail("Expected EOFException");
        } catch (EOFException e) {
            // expected
        }
    }

    @Test
    public void testGenericString() throws IOException {
        Schema schema = new Schema.Parser().parse("{\n" +
                "  \"type\": \"string\",\n" +
                "  \"avro.java.string\": \"String\"\n" +
                "}");

        String datum = "foo";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<String> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();

        DatumReader<String> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        String result = reader.read(null, decoder);
        assertThat(result, equalTo("foo"));
    }

    @Test
    public void testPairGeneric() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(
                getClass().getResourceAsStream("StringPair.avsc")
        );

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer =
                new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();

        DatumReader<GenericRecord> reader =
                new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        GenericRecord result = reader.read(null, decoder);
        assertThat(result.get("left").toString(), is("L"));
        assertThat(result.get("right").toString(), is("R"));
    }

    @Test
    public void testPairSpecific() throws IOException {
        StringPair datum = new StringPair();
        datum.setLeft("L");
        datum.setRight("R");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<StringPair> writer =
                new SpecificDatumWriter<>(StringPair.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();

        DatumReader<StringPair> reader =
                new SpecificDatumReader<>(StringPair.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        StringPair result = reader.read(null, decoder);
        assertThat(result.getLeft(), is("L"));
        assertThat(result.getRight(), is("R"));
    }

    @Test
    public void testDataFile() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass()
                .getResourceAsStream("StringPair.avsc"));

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");

        File file = new File("data.avro");
        DatumWriter<GenericRecord> writer =
                new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter =
                new DataFileWriter<>(writer);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(datum);
        dataFileWriter.close();

        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(file, reader);
        assertThat("Scheme is the same", schema, is(dataFileReader.getSchema()));

        assertThat(dataFileReader.hasNext(), is(true));
        GenericRecord result = dataFileReader.next();
        assertThat(result.get("left").toString(), is("L"));
        assertThat(result.get("right").toString(), is("R"));
        assertThat(dataFileReader.hasNext(), is(false));

        file.delete();
    }

    @Test
    public void testDataFileIteration() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass()
                .getResourceAsStream("StringPair.avsc"));

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");

        File file = new File("data.avro");
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter =
                new DataFileWriter<>(writer);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(datum);
        datum.put("right", new Utf8("r"));
        dataFileWriter.append(datum);
        dataFileWriter.close();

        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(file, reader);
        assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));

        // 重用实例
        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
        }

        dataFileReader =
                new DataFileReader<>(file, reader);
        int count = 0;
        record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            count++;
            assertThat(record.get("left").toString(), is("L"));
            if (count == 1) {
                assertThat(record.get("right").toString(), is("R"));
            } else {
                assertThat(record.get("right").toString(), is("r"));
            }
        }
        assertThat(count, is(2));
        file.delete();
    }

    @Test
    public void testDataFileIterationShort() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass()
                .getResourceAsStream("StringPair.avsc"));

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");

        File file = new File("data.avro");
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter =
                new DataFileWriter<>(writer);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(datum);
        datum.put("right", new Utf8("r"));
        dataFileWriter.append(datum);
        dataFileWriter.close();

        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<GenericRecord>(file, reader);
        assertThat("Scheme is the same", schema, is(dataFileReader.getSchema()));

        // 每次遍历都创建一个新对象
        for (GenericRecord record : dataFileReader) {
            // process record
        }


        dataFileReader =
                new DataFileReader<>(file, reader);
        int count = 0;
        for (GenericRecord record : dataFileReader) {
            count++;
            assertThat(record.get("left").toString(), is("L"));
            if (count == 1) {
                assertThat(record.get("right").toString(), is("R"));
            } else {
                assertThat(record.get("right").toString(), is("r"));
            }
        }

        assertThat(count, is(2));
        file.delete();
    }

    @Test
    public void testSchemaResolution() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass()
        .getResourceAsStream("StringPair.avsc"));
        Schema newSchema = new Schema.Parser().parse(getClass()
        .getResourceAsStream("NewStringPair.avsc"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");
        writer.write(datum, encoder);
        encoder.flush();

        DatumReader<GenericRecord> reader =
                new GenericDatumReader<>(schema, newSchema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        GenericRecord result = reader.read(null, decoder);
        assertThat(result.get("left").toString(), is("L"));
        assertThat(result.get("right").toString(), is("R"));
        assertThat(result.get("description").toString(),is(""));
    }

    @Test
    public void testSchemaResolutionWithAliases() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass()
        .getResourceAsStream("StringPair.avsc"));
        Schema newSchema = new Schema.Parser().parse(getClass()
        .getResourceAsStream("AliasedStringPair.avsc"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");
        writer.write(datum, encoder);
        encoder.flush();

        DatumReader<GenericRecord> reader =
                new GenericDatumReader<>(schema, newSchema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        GenericRecord result = reader.read(null, decoder);
        assertThat(result.get("first").toString(), is("L"));
        assertThat(result.get("second").toString(), is("R"));

//        旧的域名不再支持
        assertThat(result.get("left"), nullValue());
        assertThat(result.get("right"), nullValue());
    }

    @Test(expected = AvroTypeException.class)
    public void testIncompatibleSchemaResolution() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass()
        .getResourceAsStream("StringPair.avsc"));
        Schema newSchema = new Schema.Parser().parse("{\n" +
                "  \"type\":\"array\",\n" +
                "  \"items\":\"string\"\n" +
                "}");

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");
        writer.write(datum, encoder);
        encoder.flush();

        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, newSchema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        reader.read(null, decoder);
    }

    @Test
    public void testSchemaResolutionWithDataFile() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass()
        .getResourceAsStream("StringPair.avsc"));
        Schema newSchema = new Schema.Parser().parse(getClass()
        .getResourceAsStream("NewStringPair.avsc"));

        File file = new File("data.avro");

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter =
                new DataFileWriter<>(writer);
        dataFileWriter.create(schema,file);
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");
        dataFileWriter.append(datum);
        dataFileWriter.close();

        DatumReader<GenericRecord> reader =
                new GenericDatumReader<>(null, newSchema);
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(file, reader);
        assertThat(schema, is(dataFileReader.getSchema()));

        assertThat(dataFileReader.hasNext(), is(true));
        GenericRecord result = dataFileReader.next();
        assertThat(result.get("left").toString(), is("L"));
        assertThat(result.get("right").toString(), is("R"));
        assertThat(result.get("description").toString(), is(""));
        assertThat(dataFileReader.hasNext(), is(false));

        file.delete();
    }
}
