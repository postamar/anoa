package com.adgear.anoa.read;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;

import com.adgear.anoa.AnoaJacksonTypeException;

class ProtobufRecordWrapper<R extends Message> implements RecordWrapper<R, ProtobufFieldWrapper> {

  final private Message.Builder builder;

  public ProtobufRecordWrapper(Message.Builder clearBuilderInstance) {
    builder = clearBuilderInstance.clone();
  }

  @Override
  @SuppressWarnings("unchecked")
  public R get() {
    try {
      return (R) builder.build();
    } catch (UninitializedMessageException e) {
      throw new AnoaJacksonTypeException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(ProtobufFieldWrapper fieldWrapper, Object value) {
    Descriptors.FieldDescriptor field = fieldWrapper.field;
    if (field.isRepeated()) {
      if (value != null) {
        for (Object element : (Iterable<Object>) value) {
          builder.addRepeatedField(field, element);
        }
      }
    } else  {
      builder.setField(field, (value == null) ? builder.getField(field) : value);
    }
  }
}
