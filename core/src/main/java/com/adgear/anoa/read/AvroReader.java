package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

abstract class AvroReader<R extends IndexedRecord> extends AbstractReader<R> {

  final protected List<AvroFieldWrapper> fieldWrappers;
  final private Map<String, Optional<AvroFieldWrapper>> fieldLookUp;

  @SuppressWarnings("unchecked")
  private AvroReader(Schema schema) {
    this.fieldLookUp = new HashMap<>();
    this.fieldWrappers = new ArrayList<>();
    int index = 0;
    for (Schema.Field field : schema.getFields()) {
      AvroFieldWrapper fieldWrapper = new AvroFieldWrapper(index++, field);
      fieldWrappers.add(fieldWrapper);
      fieldLookUp.put(field.name(), Optional.of(fieldWrapper));
      field.aliases().stream().forEach(alias -> fieldLookUp.put(alias, Optional.of(fieldWrapper)));
    }
  }

  abstract protected R newInstance() throws Exception;

  @Override
  protected R validateTopLevel(R record) {
    if (record instanceof SpecificRecord) {
      SpecificData.get().validate(record.getSchema(), record);
    } else {
      GenericData.get().validate(record.getSchema(), record);
    }
    return record;
  }

  @Override
  public R read(JsonParser jacksonParser) throws IOException {
    if (jacksonParser.getCurrentToken() == JsonToken.START_OBJECT) {
      final AvroRecordWrapper<R> recordWrapper;
      try {
        recordWrapper = new AvroRecordWrapper<>(newInstance(), fieldWrappers);
      } catch (Exception e) {
        return null;
      }
      doMap(jacksonParser, (fieldName, p) -> {
        Optional<AvroFieldWrapper> cacheValue = fieldLookUp.get(fieldName);
        if (cacheValue == null) {
          cacheValue = fieldLookUp.entrySet().stream()
              .filter(e -> (0 == fieldName.compareToIgnoreCase(e.getKey())))
              .findAny()
              .flatMap(Map.Entry::getValue);
          fieldLookUp.put(fieldName, cacheValue);
        }
        if (cacheValue.isPresent()) {
          recordWrapper.put(cacheValue.get(), cacheValue.get().reader.read(p));
        } else {
          gobbleValue(p);
        }
      });
      return recordWrapper.get();
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
        final AvroRecordWrapper<R> recordWrapper;
        try {
          recordWrapper = new AvroRecordWrapper<>(newInstance(), fieldWrappers);
        } catch (Exception e) {
          throw new AnoaJacksonTypeException(e);
        }
        doMap(jacksonParser, (fieldName, p) -> {
          final Optional<AvroFieldWrapper> cacheValue =
              fieldLookUp.computeIfAbsent(fieldName, __ -> Optional.<AvroFieldWrapper>empty());
          if (cacheValue.isPresent()) {
            recordWrapper.put(cacheValue.get(), cacheValue.get().reader.readStrict(p));
          } else {
            gobbleValue(p);
          }
        });
        return recordWrapper.get();
      default:
        throw new AnoaJacksonTypeException("Token is not '{': " + jacksonParser.getCurrentToken());
    }
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
