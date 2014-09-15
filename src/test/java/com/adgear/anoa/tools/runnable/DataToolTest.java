package com.adgear.anoa.tools.runnable;


import com.adgear.anoa.codec.schemaless.AvroSpecificToStringList;
import com.adgear.anoa.codec.schemaless.AvroSpecificToValue;
import com.adgear.anoa.codec.serialized.ValueToJsonBytes;
import com.adgear.anoa.provider.IteratorProvider;
import com.adgear.anoa.sink.schemaless.CsvSink;
import com.adgear.anoa.sink.serialized.BytesLineSink;
import com.adgear.generated.avro.RecordNested;
import com.adgear.generated.avro.flat.Simple;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

public class DataToolTest extends TestBase {

  static final protected Properties jdbcProperties;

  static {
    jdbcProperties = new Properties();
    jdbcProperties
        .setProperty("url", "jdbc:h2:file:target/db/testdb;USER=usertest;PASSWORD=pwtest");
  }

  @BeforeClass
  static public void init() throws Exception {
    Class.forName("org.h2.Driver");
  }

  @Test
  public void testJsonToAvro() throws IOException {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream outSchema = new ByteArrayOutputStream();
    DataTool.streamRun(RecordNested.class,
                       DataTool.Format.JSON,
                       DataTool.Format.AVRO,
                       getClass().getResourceAsStream("/multirecord.json"),
                       out,
                       outSchema);

    Assert.assertEquals(outSchema.toString(), RecordNested.getClassSchema().toString(true));

    byte[] actual = toJson(fromAvro(out.toByteArray()));
    byte[] expected = toJson(open());
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testAvroToJson() throws IOException {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream outSchema = new ByteArrayOutputStream();
    DataTool.streamRun(null,
                       DataTool.Format.AVRO,
                       DataTool.Format.JSON,
                       toAvroStream(open()),
                       out,
                       outSchema);

    Assert.assertEquals(outSchema.toString(), RecordNested.getClassSchema().toString(true));

    byte[] actual = out.toByteArray();
    byte[] expected = toJson(open());
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testJdbcInduced() throws IOException, SQLException, ClassNotFoundException {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataTool.jdbcRun(null,
                     DataTool.Format.CSV_NO_HEADER,
                     out,
                     null,
                     jdbcProperties.getProperty("url"),
                     null,
                     "SELECT foo, bar, baz FROM simple",
                     jdbcProperties);
    byte[] actual = out.toByteArray();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new CsvSink(baos)
        .appendAll(
            new AvroSpecificToStringList<>(
                new IteratorProvider<>(Arrays.asList(
                    Simple.newBuilder().setFoo(101).setBar("brick").setBaz(789.1).build(),
                    Simple.newBuilder().setFoo(-102).setBar("mortar").setBaz(543.2).build()
                ).iterator()),
                Simple.class));
    byte[] expected = baos.toByteArray();

    Assert.assertArrayEquals(expected, actual);
  }


  @Test
  public void testJdbc() throws IOException, SQLException, ClassNotFoundException {

    Class.forName("org.h2.Driver");

    String initStatements = "ALTER TABLE simple RENAME TO simple_renamed_once;\n"
                            + "ALTER TABLE simple_renamed_once RENAME TO simple;";

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream outSchema = new ByteArrayOutputStream();
    DataTool.jdbcRun(Simple.class,
                     DataTool.Format.JSON,
                     out,
                     outSchema,
                     jdbcProperties.getProperty("url"),
                     new ByteArrayInputStream(initStatements.getBytes()),
                     "SELECT * FROM simple;",
                     jdbcProperties);
    byte[] actual = out.toByteArray();

    Assert.assertEquals(outSchema.toString(), Simple.getClassSchema().toString(true));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new BytesLineSink(baos)
        .appendAll(
            new ValueToJsonBytes(
                new AvroSpecificToValue<>(
                    new IteratorProvider<>(Arrays.asList(
                        Simple.newBuilder().setFoo(101).setBar("brick").setBaz(789.1).build(),
                        Simple.newBuilder().setFoo(-102).setBar("mortar").setBaz(543.2).build()
                    ).iterator()),
                    Simple.class)));
    byte[] expected = baos.toByteArray();

    Assert.assertArrayEquals(expected, actual);
  }

}
