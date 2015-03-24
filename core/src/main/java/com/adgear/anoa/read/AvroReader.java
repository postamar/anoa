package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
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
import org.codehaus.jackson.node.NullNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

abstract class AvroReader<R extends IndexedRecord> extends AbstractReader<R> {

  static protected class Field {

    @SuppressWarnings("unchecked")
    protected Field(Schema.Field field, Object defaultValue, AbstractReader<?> reader) {
      this.pos = field.pos();
      this.schema = field.schema();
      this.reader = reader;
      this.defaultValue = defaultValue;
    }

    final protected int pos;
    final protected Schema schema;
    final protected Object defaultValue;
    final protected AbstractReader<?> reader;

    final protected Object valueOrDefault(Object value) {
      return (value != null || defaultValue == null)
             ? value
             : SpecificData.get().deepCopy(schema, defaultValue);
    }
  }

  final private Map<Integer, Object> defaultValues;
  final private Map<String, Optional<Field>> fieldLookUp;

  abstract protected R newInstance() throws Exception;

  @SuppressWarnings("unchecked")
  private AvroReader(Schema schema) {
    this.fieldLookUp = new HashMap<>();
    this.defaultValues = new HashMap<>();
    for (Schema.Field f : schema.getFields()) {
      final Object v = defaultValue(f);
      if (v != null) {
        defaultValues.put(f.pos(), v);
      }
      Optional<Field> cached = Optional.of(new Field(f, v, createReader(f.schema())));
      fieldLookUp.put(f.name(), cached);
      for (String alias : f.aliases()) {
        fieldLookUp.put(alias, cached);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Object defaultValue(Schema.Field field) {
    if (field.defaultValue() == null || NullNode.getInstance().equals(field.defaultValue())) {
        return null;
    }
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      ResolvingGrammarGenerator.encode(encoder, field.schema(), field.defaultValue());
      encoder.flush();
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
      return SpecificData.get().createDatumReader(field.schema()).read(null, decoder);
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  public R read(JsonParser jacksonParser) throws IOException {
    if (jacksonParser.getCurrentToken() == JsonToken.START_OBJECT) {
      final R record;
      try {
        record = newInstance();
      } catch (Exception e) {
        return null;
      }
      defaultValues.entrySet().stream().forEach(e -> record.put(e.getKey(), e.getValue()));
      doMap(jacksonParser, (fieldName, p) -> {
        Optional<Field> cacheValue = fieldLookUp.get(fieldName);
        if (cacheValue == null) {
          Optional<Map.Entry<String, Optional<Field>>> found = fieldLookUp.entrySet().stream()
              .filter(e -> (0 == fieldName.compareToIgnoreCase(e.getKey())))
              .findAny();
          cacheValue = found.isPresent() ? found.get().getValue() : Optional.<Field>empty();
          fieldLookUp.put(fieldName, cacheValue);
        }
        if (cacheValue.isPresent()) {
          final Field field = cacheValue.get();
          record.put(field.pos, field.valueOrDefault(field.reader.read(p)));
        } else {
          gobbleValue(p);
        }
      });
      return record;
    } else {
      gobbleValue(jacksonParser);
      return null;
    }
  }

  @Override
  public R readStrict(JsonParser jacksonParser) throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_OBJECT:
        final R record;
        try {
          record = newInstance();
        } catch (Exception e) {
          throw new AnoaJacksonTypeException(e);
        }
        defaultValues.entrySet().stream().forEach(e -> record.put(e.getKey(), e.getValue()));
        doMap(jacksonParser, (fieldName, p) -> {
          final Optional<Field> cacheValue =
              fieldLookUp.computeIfAbsent(fieldName, __ -> Optional.<Field>empty());
          if (cacheValue.isPresent()) {
            final Field field = cacheValue.get();
            record.put(field.pos, field.valueOrDefault(field.reader.readStrict(p)));
          } else {
            gobbleValue(p);
          }
        });
        SpecificData.get().validate(record.getSchema(), record);
        return record;
      default:
        throw new AnoaJacksonTypeException("Token is not '{': " + jacksonParser.getCurrentToken());
    }  }

  @SuppressWarnings("unchecked")
  static protected AbstractReader<?> createReader(Schema schema) {
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
               ? new GenericReader(schema)
               : new SpecificReader<>(recordClass);
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

  static class GenericReader extends AvroReader<GenericRecord> {

    final Schema schema;

    GenericReader(Schema schema) {
      super(schema);
      this.schema = schema;
    }

    @Override
    protected GenericRecord newInstance() throws Exception {
      return new GenericData.Record(schema);
    }
  }

  static class SpecificReader<R extends SpecificRecord> extends AvroReader<R> {

    final Constructor<R> constructor;

    SpecificReader(Class<R> recordClass) {
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
