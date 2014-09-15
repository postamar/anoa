package com.adgear.anoa.sink.schemaless;

import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.sink.Sink;

import org.apache.avro.Schema;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.quote.AlwaysQuoteMode;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * Collects String list records into a comma-separated-values file. <p> Separator is ',', fields are
 * always quoted. If provided a matching Avro Schema, will generate a column headers row.
 *
 * @see com.adgear.anoa.sink.Sink
 * @see com.adgear.anoa.source.schemaless.CsvSource
 * @see com.adgear.anoa.source.schemaless.CsvWithHeaderSource
 * @see com.adgear.anoa.source.schemaless.TsvSource
 * @see com.adgear.anoa.source.schemaless.TsvWithHeaderSource
 * @see com.adgear.anoa.codec.schemaless.AvroGenericToStringList
 * @see com.adgear.anoa.codec.schemaless.AvroSpecificToStringList
 */
public class CsvSink implements Sink<List<String>, CsvSink> {

  static final private CsvPreference ALWAYS_QUOTE =
      new CsvPreference.Builder(CsvPreference.STANDARD_PREFERENCE)
          .useQuoteMode(new AlwaysQuoteMode())
          .build();


  final protected CsvListWriter writer;

  /**
   * @param out Stream to write to, assumes UTF-8 encoding.
   */
  public CsvSink(OutputStream out) throws IOException {
    this(out, null);
  }

  /**
   * @param out    Stream to write to, assumes UTF-8 encoding.
   * @param schema Used to generate column headers.
   */
  public CsvSink(OutputStream out, Schema schema) throws IOException {
    this(new OutputStreamWriter(out, "UTF-8"), schema);
  }

  public CsvSink(Writer writer) throws IOException {
    this(writer, null);
  }

  /**
   * @param schema Used to generate column headers.
   */
  public CsvSink(Writer writer, Schema schema) throws IOException {
    this(new CsvListWriter(writer, ALWAYS_QUOTE), schema);
  }

  protected CsvSink(CsvListWriter writer, Schema schema) throws IOException {
    this.writer = writer;
    if (schema != null) {
      List<String> names = new ArrayList<>();
      for (Schema.Field field : schema.getFields()) {
        names.add(field.name());
      }
      writer.write(names);
    }
  }

  @Override
  public CsvSink append(List<String> record) throws IOException {
    if (record != null) {
      writer.write(record);
    }
    return this;
  }

  @Override
  public CsvSink appendAll(Provider<List<String>> provider) throws IOException {
    for (List<String> element : provider) {
      append(element);
    }
    flush();
    return this;
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }
}
