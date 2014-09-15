package com.adgear.anoa.tools.runnable;


import com.adgear.anoa.codec.avro.JsonNodeToAvro;
import com.adgear.anoa.codec.schemaless.AvroSpecificToValue;
import com.adgear.anoa.codec.serialized.ValueToJsonBytes;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;
import com.adgear.anoa.sink.avro.AvroSink;
import com.adgear.anoa.sink.serialized.BytesLineSink;
import com.adgear.anoa.source.avro.AvroSpecificSource;
import com.adgear.anoa.source.schemaless.JsonNodeSource;
import com.adgear.generated.avro.RecordNested;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestBase {

  protected AvroProvider<RecordNested> open() {
    return open("/multirecord.json");
  }

  protected AvroProvider<RecordNested> open(String name) {
    InputStream stream = TestBase.class.getResourceAsStream(name);
    return new JsonNodeToAvro<>(new JsonNodeSource(stream),
                                RecordNested.class);
  }

  protected byte[] toJson(Provider<RecordNested> provider) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new BytesLineSink(baos)
        .appendAll(
            new ValueToJsonBytes(
                new AvroSpecificToValue<>(provider, RecordNested.class)));
    return baos.toByteArray();
  }

  protected ByteArrayInputStream toAvroStream(AvroProvider<RecordNested> provider)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new AvroSink<>(baos, RecordNested.class)
        .appendAll(provider);
    return new ByteArrayInputStream(baos.toByteArray());
  }

  protected AvroProvider<RecordNested> fromAvro(byte[] avro) throws IOException {
    return new AvroSpecificSource<>(new ByteArrayInputStream(avro), RecordNested.class);
  }

}
