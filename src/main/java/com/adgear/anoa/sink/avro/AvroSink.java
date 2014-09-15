package com.adgear.anoa.sink.avro;

import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.sink.Sink;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Collects Avro records into an Avro data file. <p> Data file is compressed using the
 * <code>deflate</code> codec, level 9. Sync interval is approx. 8 MB.
 *
 * @param <R> The type of the Avro records consumed by the Sink.
 * @see com.adgear.anoa.sink.Sink
 * @see com.adgear.anoa.source.avro.AvroSource
 * @see com.adgear.anoa.codec.avro.BytesToAvroGeneric
 * @see com.adgear.anoa.codec.avro.BytesToAvroSpecific
 * @see com.adgear.anoa.codec.avro.JsonNodeToAvro
 * @see com.adgear.anoa.codec.avro.StringListToAvro
 * @see com.adgear.anoa.codec.avro.ValueToAvro
 */
public class AvroSink<R extends IndexedRecord> implements Sink<R, AvroSink<R>> {

  final protected DataFileWriter<R> writer;

  /**
   * @param out         An output stream to a new Avro data file
   * @param recordClass The class object corresponding to the SpecificRecords written to out
   */
  public AvroSink(OutputStream out, Class<R> recordClass) throws IOException {
    this(out, SpecificData.get().getSchema(recordClass), new SpecificDatumWriter<>(recordClass));
  }


  /**
   * @param out    An output stream to a new Avro data file
   * @param schema The writer Schema to use when writing GenericRecords to out.
   */
  public AvroSink(OutputStream out, Schema schema) throws IOException {
    this(out, schema, new GenericDatumWriter<R>(schema));
  }

  protected AvroSink(OutputStream out, Schema schema, DatumWriter<R> datumWriter)
      throws IOException {
    this.writer = new DataFileWriter<>(datumWriter);
    writer.setCodec(CodecFactory.deflateCodec(9));
    writer.setSyncInterval(1 << 23); // 8M bytes
    writer.create(schema, out);
  }

  @Override
  public AvroSink<R> append(R record) throws IOException {
    if (record != null) {
      writer.append(record);
    }
    return this;
  }

  @Override
  public AvroSink<R> appendAll(Provider<R> provider) throws IOException {
    for (R record : provider) {
      append(record);
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
