package com.adgear.anoa.io.read.json;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

abstract class AvroReader<R extends IndexedRecord> extends JsonReader<R> {

  static protected class Field {

    @SuppressWarnings("unchecked")
    protected Field(Schema.Field field, JsonReader<?> reader) {
      if (field.defaultValue() == null) {
        defaultValue = null;
      } else {
        try {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
          ResolvingGrammarGenerator.encode(encoder, field.schema(), field.defaultValue());
          encoder.flush();
          BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
          this.defaultValue =
              SpecificData.get().createDatumReader(field.schema()).read(null, decoder);
        } catch (IOException e) {
          throw new AvroRuntimeException(e);
        }
      }
      this.pos = field.pos();
      this.schema = field.schema();
      this.reader = reader;
    }

    final protected int pos;
    final protected Schema schema;
    final protected Object defaultValue;
    final protected JsonReader<?> reader;

    final protected Object valueOrDefault(Object value) {
      return (value != null || defaultValue == null)
             ? value
             : SpecificData.get().deepCopy(schema, defaultValue);
    }
  }

  final private Map<String,Field> fieldLookUp;

  abstract protected R newInstance() throws Exception;

  @SuppressWarnings("unchecked")
  private AvroReader(Schema schema) {
    this.fieldLookUp = new HashMap<>();
    for (Schema.Field field : schema.getFields()) {
      final Field lookUpValue = new Field(field, createReader(field.schema()));
      fieldLookUp.put(field.name(), lookUpValue);
      for (String alias : field.aliases()) {
        fieldLookUp.put(alias, lookUpValue);
      }
    }
  }

  @Override
  public R read(JsonParser jp) throws IOException {
    if (jp.getCurrentToken() == JsonToken.START_OBJECT) {
      final R record;
      try {
        record = newInstance();
      } catch (Exception e) {
        return null;
      }
      doMap(jp, (fieldName, p) -> {
        Field field = fieldLookUp.get(fieldName);
        if (field != null) {
          record.put(field.pos, field.valueOrDefault(field.reader.read(jp)));
        } else {
          gobbleValue(jp);
        }
      });
      return record;
    } else {
      gobbleValue(jp);
      return null;
    }
  }

  @Override
  public R readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_OBJECT:
        final R record;
        try {
          record = newInstance();
        } catch (Exception e) {
          throw new AnoaTypeException(e);
        }
        doMap(jp, (fieldName, p) -> {
          Field field = fieldLookUp.get(jp.getCurrentName());
          if (field != null) {
            record.put(field.pos, field.valueOrDefault(field.reader.readStrict(jp)));
          } else {
            gobbleValue(jp);
          }
        });
        return record;
      default:
        throw new AnoaTypeException("Token is not '{': " + jp.getCurrentToken());
    }  }

  @SuppressWarnings("unchecked")
  static protected JsonReader<?> createReader(Schema schema) {
    switch (schema.getType()) {
      case ARRAY:
        return new ListReader(createReader(schema.getElementType()));
      case BOOLEAN:
        return new BooleanReader();
      case BYTES:
        return new ByteBufferReader();
      case DOUBLE:
        return new DoubleReader();
      case ENUM:
        return new EnumReader(SpecificData.get().getClass(schema));
      case FIXED:
        final Class<? extends SpecificFixed> fixedClass = SpecificData.get().getClass(schema);
        return (fixedClass == null)
               ? new AvroFixedReader.AvroGenericFixedReader(schema)
               : new AvroFixedReader.AvroSpecificFixedReader<>(fixedClass);
      case FLOAT:
        return new FloatReader();
      case INT:
        return new IntegerReader();
      case LONG:
        return new LongReader();
      case MAP:
        return new MapReader(createReader(schema.getValueType()));
      case RECORD:
        final Class<? extends SpecificRecord> recordClass = SpecificData.get().getClass(schema);
        return (recordClass == null)
               ? new AvroGenericReader(schema)
               : new AvroSpecificReader<>(recordClass);
      case STRING:
        return new StringReader();
      case UNION:
        if (schema.getTypes().size() == 2) {
          return createReader(schema.getTypes().get(
              (schema.getTypes().get(0).getType() == Schema.Type.NULL) ? 1 : 0));
        }
    }
    throw new RuntimeException("Unsupported Avro schema: " + schema);
  }

  static class AvroGenericReader extends AvroReader<GenericRecord> {

    final Schema schema;

    AvroGenericReader(Schema schema) {
      super(schema);
      this.schema = schema;
    }

    @Override
    protected GenericRecord newInstance() throws Exception {
      return new GenericData.Record(schema);
    }
  }

  static class AvroSpecificReader<R extends SpecificRecord> extends AvroReader<R> {

    final Constructor<R> constructor;

    AvroSpecificReader(Class<R> recordClass) {
      super(SpecificData.get().getSchema(recordClass));
      try {
        this.constructor = recordClass.getDeclaredConstructor();
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected R newInstance() throws Exception {
      return constructor.newInstance();
    }
  }
}
