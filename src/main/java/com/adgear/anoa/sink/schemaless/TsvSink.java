package com.adgear.anoa.sink.schemaless;

import com.adgear.anoa.provider.Provider;

import org.apache.avro.Schema;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

/**
 * Collects String list records into a tab-separated-values file. <p> Separator is '\\t'. If
 * provided a matching Avro Schema, will generate a column headers row.
 *
 * @see com.adgear.anoa.sink.Sink
 * @see com.adgear.anoa.source.schemaless.CsvSource
 * @see com.adgear.anoa.source.schemaless.CsvWithHeaderSource
 * @see com.adgear.anoa.source.schemaless.TsvSource
 * @see com.adgear.anoa.source.schemaless.TsvWithHeaderSource
 * @see com.adgear.anoa.codec.schemaless.AvroGenericToStringList
 * @see com.adgear.anoa.codec.schemaless.AvroSpecificToStringList
 */
public class TsvSink extends CsvSink {

  /**
   * @param out Stream to write to, assumes UTF-8 encoding.
   */
  public TsvSink(OutputStream out) throws IOException {
    this(out, null);
  }

  /**
   * @param out    Stream to write to, assumes UTF-8 encoding.
   * @param schema Used to generate column headers.
   */
  public TsvSink(OutputStream out, Schema schema) throws IOException {
    this(new OutputStreamWriter(out, "UTF-8"), schema);
  }

  public TsvSink(Writer writer) throws IOException {
    this(writer, null);
  }

  /**
   * @param schema Used to generate column headers.
   */
  public TsvSink(Writer writer, Schema schema) throws IOException {
    super(new CsvListWriter(writer, CsvPreference.TAB_PREFERENCE), schema);
  }

  @Override
  public TsvSink append(List<String> record) throws IOException {
    super.append(record);
    return this;
  }

  @Override
  public TsvSink appendAll(Provider<List<String>> provider) throws IOException {
    super.appendAll(provider);
    return this;
  }
}