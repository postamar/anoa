package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.adgear.anoa.factory.util.ReflectionUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


class ThriftReader<F extends TFieldIdEnum, T extends TBase<T,F>> extends JacksonReader<T> {

  static protected class Field<F extends TFieldIdEnum> {

    protected Field(F tFieldIdEnum, boolean isRequired, JacksonReader<?> reader) {
      this.tFieldIdEnum = tFieldIdEnum;
      this.isRequired = isRequired;
      this.reader = reader;
    }

    final protected F tFieldIdEnum;
    final protected boolean isRequired;
    final protected JacksonReader<?> reader;
  }

  final private Map<String,Optional<Field<F>>> fieldLookUp;
  final private T instance;
  final private int nRequired;

  ThriftReader(Class<T> thriftClass) {
    this(new StructMetaData(TType.STRUCT, thriftClass));
  }

  @SuppressWarnings("unchecked")
  private ThriftReader(StructMetaData metaData) {
    Class<T> thriftClass = (Class<T>) metaData.structClass;
    try {
      instance = thriftClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    instance.clear();
    fieldLookUp = new HashMap<>();
    int n = 0;
    for (Map.Entry<F,FieldMetaData> entry :
        ReflectionUtils.getThriftMetaDataMap(thriftClass).entrySet()) {
      final boolean required = (entry.getValue().requirementType == TFieldRequirementType.REQUIRED);
      n += required ? 1 : 0;
      fieldLookUp.put(entry.getKey().getFieldName(), Optional.of(
          new Field<>(entry.getKey(), required, createReader(entry.getValue().valueMetaData))));
    }
    nRequired = n;
  }

  @Override
  public T read(JsonParser jp) throws IOException {
    if (jp.getCurrentToken() == JsonToken.START_OBJECT) {
      final T result = newInstance();
      doMap(jp, (fieldName, p) -> {
        Optional<Field<F>> cacheValue = fieldLookUp.get(fieldName);
        if (cacheValue == null) {
          Optional<Map.Entry<String, Optional<Field<F>>>> found = fieldLookUp.entrySet().stream()
              .filter(e -> (0 == fieldName.compareToIgnoreCase(e.getKey())))
              .findAny();
          cacheValue = found.isPresent() ? found.get().getValue() : Optional.empty();
          fieldLookUp.put(fieldName, cacheValue);
        }
        if (cacheValue.isPresent()) {
          final Field<F> field = cacheValue.get();
          result.setFieldValue(field.tFieldIdEnum, field.reader.read(p));
        } else {
          gobbleValue(p);
        }
      });
      return result;
    } else {
      gobbleValue(jp);
      return null;
    }
  }

  static private class Counter {
    long n = 0;
  }

  @Override
  public T readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_OBJECT:
        final T result = newInstance();
        final Counter countRequired = new Counter();
        doMap(jp, (fieldName, p) -> {
          final Optional<Field<F>> cacheValue =
              fieldLookUp.computeIfAbsent(fieldName, __ -> Optional.<Field<F>>empty());
          if (cacheValue.isPresent()) {
            final Field<F> field = cacheValue.get();
            result.setFieldValue(field.tFieldIdEnum, field.reader.readStrict(p));
            if (field.isRequired) {
              ++countRequired.n;
            }
          } else {
            gobbleValue(p);
          }
        });
        if (countRequired.n < nRequired) {
          for (Optional<Field<F>> cacheValue : fieldLookUp.values()) {
            if (!result.isSet(cacheValue.get().tFieldIdEnum)) {
              throw new AnoaTypeException("Required field not set: "
                                          + cacheValue.get().tFieldIdEnum.getFieldName());
            }
          }
        }
        return result;
      default:
        throw new AnoaTypeException("Token is not '{': " + jp.getCurrentToken());
    }
  }

  @SuppressWarnings("unchecked")
  protected T newInstance() {
    return (T) instance.deepCopy();
  }

  static protected JacksonReader<?> createReader(FieldValueMetaData metaData) {
    switch (metaData.type) {
      case TType.BOOL:
        return new BooleanReader();
      case TType.BYTE:
        return new ByteReader();
      case TType.DOUBLE:
        return new DoubleReader();
      case TType.ENUM:
        return new EnumReader(((EnumMetaData) metaData).enumClass);
      case TType.I16:
        return new ShortReader();
      case TType.I32:
        return new IntegerReader();
      case TType.I64:
        return new LongReader();
      case TType.LIST:
        return new ListReader(createReader(((ListMetaData) metaData).elemMetaData));
      case TType.MAP:
        MapMetaData mapMetaData = (MapMetaData) metaData;
        if (mapMetaData.keyMetaData.type != TType.STRING) {
          throw new RuntimeException("Map key type is not string.");
        }
        return new MapReader(createReader(mapMetaData.valueMetaData));
      case TType.SET:
        return new SetReader(createReader((SetMetaData) metaData));
      case TType.STRUCT:
        return new ThriftReader((StructMetaData) metaData);
      case TType.STRING:
        return metaData.isBinary() ? new ByteBufferReader() : new StringReader();
    }
    throw new RuntimeException("Unknown type in metadata " + metaData);
  }
}
