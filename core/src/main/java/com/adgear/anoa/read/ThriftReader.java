package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.adgear.anoa.AnoaReflectionUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class ThriftReader<F extends TFieldIdEnum, T extends TBase<?, F>> extends AbstractReader<T> {

  final private List<ThriftFieldWrapper<F>> fieldWrappers;
  final private Map<String, Optional<ThriftFieldWrapper<F>>> fieldLookUp;
  final private T instance;
  final private int nRequired;

  ThriftReader(Class<T> thriftClass) {
    try {
      this.instance = thriftClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    instance.clear();
    this.fieldWrappers = new ArrayList<>();
    AnoaReflectionUtils.getThriftMetaDataMap(thriftClass).forEach(
        (k, v) -> fieldWrappers.add(new ThriftFieldWrapper<>(k, v)));
    this.fieldLookUp = new HashMap<>(fieldWrappers.size());
    fieldWrappers.stream().forEach(
        w -> fieldLookUp.put(w.tFieldIdEnum.getFieldName(), Optional.of(w)));
    this.nRequired = (int) fieldWrappers.stream().filter(w -> w.isRequired).count();
  }

  @Override
  protected T read(JsonParser jacksonParser) throws IOException {
    if (jacksonParser.getCurrentToken() == JsonToken.START_OBJECT) {
      final ThriftRecordWrapper<F, T> recordWrapper = newWrappedInstance();
      doMap(jacksonParser, (fieldName, p) -> {
        Optional<ThriftFieldWrapper<F>> cacheValue = fieldLookUp.get(fieldName);
        if (cacheValue == null) {
          cacheValue = fieldLookUp.entrySet().stream()
              .filter(e -> (0 == fieldName.compareToIgnoreCase(e.getKey())))
              .findAny()
              .flatMap(Map.Entry::getValue);
          fieldLookUp.put(fieldName, cacheValue);
        }
        if (cacheValue.isPresent()) {
          final ThriftFieldWrapper<F> fieldWrapper = cacheValue.get();
          recordWrapper.put(fieldWrapper, fieldWrapper.reader.read(p));
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
  protected T readStrict(JsonParser jacksonParser) throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_OBJECT:
        final ThriftRecordWrapper<F, T> recordWrapper = newWrappedInstance();
        doMap(jacksonParser, (fieldName, p) -> {
          final Optional<ThriftFieldWrapper<F>> cacheValue =
              fieldLookUp.computeIfAbsent(fieldName, __ -> Optional.<ThriftFieldWrapper<F>>empty());
          if (cacheValue.isPresent()) {
            final ThriftFieldWrapper<F> fieldWrapper = cacheValue.get();
            recordWrapper.put(fieldWrapper, fieldWrapper.reader.readStrict(p));
          } else {
            gobbleValue(p);
          }
        });
        return recordWrapper.get();
      default:
        throw new AnoaJacksonTypeException("Token is not '{': " + jacksonParser.getCurrentToken());
    }
  }

  @SuppressWarnings("unchecked")
  protected ThriftRecordWrapper<F, T> newWrappedInstance() {
    return new ThriftRecordWrapper<>((T) instance.deepCopy(), fieldWrappers, nRequired);
  }
}
