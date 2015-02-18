package com.adgear.anoa.io.read.json;

import com.adgear.anoa.ThrowingFunction;
import com.adgear.anoa.io.read.Reader;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.StructMetaData;

import java.io.IOException;

abstract public class JsonReader<OUT> implements Reader<JsonParser,OUT> {

  static private JsonFactory JSON_FACTORY = new JsonFactory();

  static public JsonParser createParser(byte[] jsonBytes) throws IOException {
    JsonParser jsonParser = JSON_FACTORY.createParser(jsonBytes);
    JsonToken jsonToken = jsonParser.nextToken();
    if (jsonToken == null || jsonToken != JsonToken.START_OBJECT) {
      throw new IOException("JsonParser.nextToken() returned " + jsonToken);
    }
    return jsonParser;
  }

  @SuppressWarnings("unchecked")
  static public <OUT> ThrowingFunction<JsonParser,OUT> lambda(Class<OUT> klazz, boolean strict) {
    final JsonReader<OUT> jsonReader;
    if (TBase.class.isAssignableFrom(klazz)) {
      jsonReader = new ThriftReader(klazz);
    } else if (SpecificRecord.class.isAssignableFrom(klazz)) {
      jsonReader = new AvroReader.AvroSpecificReader(klazz);
    } else {
      throw new IllegalArgumentException("Class is not a Thrift or an Avro record: " + klazz);
    }
    return strict ? jsonReader::readStrict : jsonReader::read;
  }

  static public ThrowingFunction<JsonParser,GenericRecord> lambda(Schema schema, boolean strict) {
    final JsonReader<GenericRecord> jsonReader = new AvroReader.AvroGenericReader(schema);
    return strict ? jsonReader::readStrict : jsonReader::read;
  }

  @SuppressWarnings("unchecked")
  static public <T extends TBase<T,? extends TFieldIdEnum>> ThrowingFunction<JsonParser,T>
  lambda(StructMetaData structMetaData, boolean strict) {
    return JsonReader.lambda((Class<T>) structMetaData.structClass, strict);
  }

  static protected interface ValueConsumer<E extends Exception> {
    public void accept(JsonParser jp) throws IOException, E;
  }

  static protected interface FieldValueConsumer<E extends Exception> {
    public void accept(String fieldName, JsonParser jp) throws IOException, E;
  }

  static protected <E extends Exception> void doArray(JsonParser jp,
                                                      ValueConsumer<E> valueConsumer)
      throws IOException, E {
    while (jp.nextToken() != JsonToken.END_ARRAY) {
      switch (jp.getCurrentToken()) {
        case END_OBJECT:
        case FIELD_NAME:
        case NOT_AVAILABLE:
          throw new IOException("Expected JSON object value, not " + jp.getCurrentToken());
      }
      valueConsumer.accept(jp);
    }
  }

  static protected <E extends Exception> void doMap(JsonParser jp,
                                                    FieldValueConsumer<E> fieldValueConsumer)
      throws IOException, E {
    while (jp.nextToken() != JsonToken.END_OBJECT) {
      if (jp.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw new IOException("Expected JSON object field name, not " + jp.getCurrentToken());
      }
      final String key = jp.getCurrentName();
      switch (jp.nextToken()) {
        case END_ARRAY:
        case END_OBJECT:
        case FIELD_NAME:
        case NOT_AVAILABLE:
          throw new IOException("Expected JSON object value, not " + jp.getCurrentToken());
      }
      fieldValueConsumer.accept(key, jp);
    }
  }

  static protected void gobbleValue(JsonParser jp) throws IOException {
    switch (jp.getCurrentToken()) {
      case START_ARRAY:
        doArray(jp, JsonReader::gobbleValue);
        break;
      case START_OBJECT:
        doMap(jp, (k, p) -> gobbleValue(p));
        break;
      case END_ARRAY:
      case END_OBJECT:
      case NOT_AVAILABLE:
      case FIELD_NAME:
        throw new IOException("Expected '[', '{', or JSON value, not " + jp.getCurrentToken());
    }
  }

}
