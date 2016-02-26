package com.adgear.anoa.read;

import com.adgear.anoa.AnoaReflectionUtils;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

class ThriftReader<F extends TFieldIdEnum, T extends TBase<?, F>>
    extends AbstractRecordReader<T, ThriftFieldWrapper<F>> {

  final private T instance;
  final private int nRequired;

  ThriftReader(Class<T> thriftClass) {
    super(AnoaReflectionUtils.getThriftMetaDataMap(thriftClass).entrySet().stream()
              .map(e -> new ThriftFieldWrapper<>(e.getKey(), e.getValue())));
    try {
      this.instance = thriftClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    instance.clear();
    this.nRequired = (int) fieldWrappers.stream().filter(w -> w.isRequired).count();
  }

  @SuppressWarnings("unchecked")
  protected ThriftRecordWrapper<F, T> newWrappedInstance() {
    return new ThriftRecordWrapper<>((T) instance.deepCopy(), fieldWrappers, nRequired);
  }
}
