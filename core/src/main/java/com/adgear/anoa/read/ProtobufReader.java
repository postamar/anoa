package com.adgear.anoa.read;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.adgear.anoa.AnoaReflectionUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

class ProtobufReader<R extends Message> extends AbstractReader<R> {

  final private Message.Builder clearBuilderInstance;
  final private Map<String, Optional<ProtobufFieldWrapper>> fieldLookUp;

  ProtobufReader(Message.Builder builder) {
    this.clearBuilderInstance = builder.clone().clear();
    this.fieldLookUp = new HashMap<>();
    Descriptors.Descriptor descriptor = builder.getDescriptorForType();
    Stream.concat(descriptor.getFields().stream(), descriptor.getExtensions().stream())
        .map(field -> new ProtobufFieldWrapper(field, builder))
        .forEach(wrapper -> fieldLookUp.put(wrapper.field.getName(), Optional.of(wrapper)));
  }

  ProtobufReader(Class<R> recordClass) {
    this((Message.Builder) AnoaReflectionUtils.getProtobufBuilder(recordClass));
  }

  @Override
  @SuppressWarnings("unchecked")
  protected R read(JsonParser jacksonParser) throws IOException {
    if (jacksonParser.getCurrentToken() == JsonToken.START_OBJECT) {
      final Message.Builder builder = clearBuilderInstance.clone();
      doMap(jacksonParser, (fieldName, p) -> {
        Optional<ProtobufFieldWrapper> cacheValue = fieldLookUp.get(fieldName);
        if (cacheValue == null) {
          cacheValue = fieldLookUp.entrySet().stream()
              .filter(e -> (0 == fieldName.compareToIgnoreCase(e.getKey())))
              .findAny()
              .flatMap(Map.Entry::getValue);
          fieldLookUp.put(fieldName, cacheValue);
        }
        if (cacheValue.isPresent()) {
          final ProtobufFieldWrapper fieldWrapper = cacheValue.get();
          if (fieldWrapper.field.isRepeated()) {
            for (Object e : fieldWrapper.listReader.read(p)) {
              builder.addRepeatedField(fieldWrapper.field, e);
            }
          } else {
            builder.setField(fieldWrapper.field, fieldWrapper.reader.read(p));
          }
        } else {
          gobbleValue(p);
        }
      });
      return (R) builder.buildPartial();
    } else {
      gobbleValue(jacksonParser);
      return null;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected R readStrict(JsonParser jacksonParser) throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_OBJECT:
        final Message.Builder builder = clearBuilderInstance.clone();
        doMap(jacksonParser, (fieldName, p) -> {
          final Optional<ProtobufFieldWrapper> cacheValue =
              fieldLookUp.computeIfAbsent(fieldName,
                                          __ -> Optional.<ProtobufFieldWrapper>empty());
          if (cacheValue.isPresent()) {
            final ProtobufFieldWrapper fieldWrapper = cacheValue.get();
            if (fieldWrapper.field.isRepeated()) {
              for (Object e : fieldWrapper.listReader.readStrict(p)) {
                builder.addRepeatedField(fieldWrapper.field, e);
              }
            } else {
              builder.setField(fieldWrapper.field, fieldWrapper.reader.readStrict(p));
            }
          } else {
            gobbleValue(p);
          }
        });
        return (R) builder.build();
      default:
        throw new AnoaJacksonTypeException("Token is not '{': " + jacksonParser.getCurrentToken());
    }
  }
}
