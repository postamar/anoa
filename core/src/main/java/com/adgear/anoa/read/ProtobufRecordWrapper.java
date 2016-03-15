package com.adgear.anoa.read;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

class ProtobufRecordWrapper<R extends Message> implements RecordWrapper<R, ProtobufFieldWrapper> {

  final private Message.Builder builder;

  public ProtobufRecordWrapper(Message.Builder clearBuilderInstance) {
    builder = clearBuilderInstance.clone();
  }

  @Override
  @SuppressWarnings("unchecked")
  public R get() {
    return (R) builder.build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(ProtobufFieldWrapper fieldWrapper, Object value) {
    Descriptors.FieldDescriptor field = fieldWrapper.field;
    if (field.isRepeated()) {
      for (Object element : (Iterable<Object>) value) {
        builder.addRepeatedField(field, nonNullValue(field, element));
      }
    } else {
      builder.setField(field, nonNullValue(field, value));
    }
  }

  private Object nonNullValue(Descriptors.FieldDescriptor field, Object value) {
    if (value != null) {
      return value;
    }
    return builder.newBuilderForField(field).getDefaultInstanceForType();
  }

}
