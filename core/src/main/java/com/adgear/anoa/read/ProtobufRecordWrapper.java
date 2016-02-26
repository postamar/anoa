package com.adgear.anoa.read;

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
    if (fieldWrapper.field.isRepeated()) {
      for (Object e : (Iterable<Object>) value) {
        builder.addRepeatedField(fieldWrapper.field, e);
      }
    } else {
      builder.setField(fieldWrapper.field, value);
    }
  }
}
