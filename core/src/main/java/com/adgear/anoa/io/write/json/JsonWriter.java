package com.adgear.anoa.io.write.json;

import com.adgear.anoa.ThrowingFunction;
import com.adgear.anoa.io.write.Writer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.StructMetaData;

import java.io.IOException;
import java.io.OutputStream;

abstract public class JsonWriter<IN> implements Writer<IN,JsonGenerator> {

  static protected ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public void writeToStream(IN element, OutputStream out) throws IOException {
    try (JsonGenerator jsonGenerator = OBJECT_MAPPER.getFactory().createGenerator(out)) {
      write(element, jsonGenerator);
      jsonGenerator.flush();
    }
  }

  public TokenBuffer write(IN element) throws IOException {
    TokenBuffer tokenBuffer = new TokenBuffer(OBJECT_MAPPER, false);
    write(element, tokenBuffer);
    tokenBuffer.flush();
    return tokenBuffer;
  }

  @SuppressWarnings("unchecked")
  static public <IN> ThrowingFunction<IN, TokenBuffer> lambda(Class<IN> klazz) {
    final JsonWriter<IN> jsonWriter;
    if (TBase.class.isAssignableFrom(klazz)) {
      jsonWriter = new ThriftWriter(klazz);
    } else if (SpecificRecord.class.isAssignableFrom(klazz)) {
      jsonWriter = new AvroWriter(klazz);
    } else {
      throw new IllegalArgumentException("Class is not a Thrift or an Avro record: " + klazz);
    }
    return jsonWriter::write;
  }


  static public ThrowingFunction<GenericRecord, TokenBuffer> lambda(Schema schema) {
    return (new AvroWriter<>(schema))::write;
  }

  @SuppressWarnings("unchecked")
  static public <T extends TBase<T,? extends TFieldIdEnum>> ThrowingFunction<T, TokenBuffer>
  lambda(StructMetaData structMetaData) {
    return lambda((Class<T>) structMetaData.structClass);
  }
}
