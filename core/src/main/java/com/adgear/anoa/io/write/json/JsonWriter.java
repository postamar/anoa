package com.adgear.anoa.io.write.json;

import com.adgear.anoa.io.write.Writer;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.StructMetaData;

import java.io.IOException;
import java.io.OutputStream;

abstract public class JsonWriter<IN> implements Writer<IN,JsonGenerator> {

  static protected JsonFactory JSON_FACTORY = new JsonFactory();

  @Override
  public void writeToStream(IN element, OutputStream out) throws IOException {
    try (JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(out)) {
      write(element, jsonGenerator);
      jsonGenerator.flush();
    }
  }

  @SuppressWarnings("unchecked")
  static public <IN> JsonWriter<IN> create(Class<IN> klazz) {
    if (TBase.class.isAssignableFrom(klazz)) {
      return new ThriftWriter(klazz);
    } else if (SpecificRecord.class.isAssignableFrom(klazz)) {
      return new AvroWriter(klazz);
    }
    throw new IllegalArgumentException("Class is not a Thrift or an Avro record: " + klazz);
  }

  static public JsonWriter<GenericRecord> create(Schema schema) {
    return new AvroWriter<>(schema);
  }

  @SuppressWarnings("unchecked")
  static public <T extends TBase<T,? extends TFieldIdEnum>> JsonWriter<T> create(StructMetaData s) {
    return new ThriftWriter(s.structClass);
  }
}
