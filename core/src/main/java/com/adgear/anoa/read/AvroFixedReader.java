package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificFixed;

import java.io.IOException;
import java.lang.reflect.Constructor;

abstract class AvroFixedReader<F extends GenericData.Fixed> extends JacksonReader<F> {

  abstract protected F newInstance(byte[] bytes) throws Exception;

  static private ByteArrayReader byteArrayReader = new ByteArrayReader();

  @Override
  public F read(JsonParser jp) throws IOException {
    final byte[] array = byteArrayReader.read(jp);
    if (array == null) {
      return null;
    }
    try {
      return newInstance(array);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public F readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    final byte[] array = byteArrayReader.readStrict(jp);
    if (array == null) {
      return null;
    }
    try {
      return newInstance(array);
    } catch (Exception e) {
      return null;
    }
  }

  static class AvroSpecificFixedReader<F extends SpecificFixed> extends AvroFixedReader<F> {

    final Constructor<F> constructor;

    AvroSpecificFixedReader(Class<F> fixedClass) {
      try {
        this.constructor = fixedClass.getDeclaredConstructor();
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected F newInstance(byte[] bytes) throws Exception {
      F instance = constructor.newInstance();
      instance.bytes(bytes);
      return instance;
    }
  }

  static class AvroGenericFixedReader extends AvroFixedReader<GenericData.Fixed> {

    final Schema schema;

    AvroGenericFixedReader(Schema schema) {
      this.schema = schema;
    }

    @Override
    protected GenericData.Fixed newInstance(byte[] bytes) throws Exception {
      return new GenericData.Fixed(schema, bytes);
    }
  }
}