package com.adgear.anoa.read;

import com.google.protobuf.Message;

import com.adgear.anoa.AnoaReflectionUtils;

class ProtobufReader<R extends Message> extends AbstractRecordReader<R, ProtobufFieldWrapper> {

  final private Message.Builder clearBuilderInstance;

  ProtobufReader(Message.Builder builder) {
    super(builder.clone().clear().getDescriptorForType().getFields().stream()
              .map(field -> new ProtobufFieldWrapper(field, builder)));
    this.clearBuilderInstance = builder.clone().clear();
  }

  ProtobufReader(Class<R> recordClass) {
    this((Message.Builder) AnoaReflectionUtils.getProtobufBuilder(recordClass));
  }

  @Override
  protected RecordWrapper<R, ProtobufFieldWrapper> newWrappedInstance() {
    return new ProtobufRecordWrapper<>(clearBuilderInstance);
  }
}
