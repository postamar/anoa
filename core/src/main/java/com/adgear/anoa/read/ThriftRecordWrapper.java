package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import java.util.List;

class ThriftRecordWrapper<F extends TFieldIdEnum, T extends TBase<?, F>> {

  final protected T record;
  final protected List<ThriftFieldWrapper<F>> fieldWrappers;
  final protected int nRequired;
  private int n = 0;

  ThriftRecordWrapper(T record, List<ThriftFieldWrapper<F>> fieldWrappers, int nRequired) {
    this.record = record;
    this.fieldWrappers = fieldWrappers;
    this.nRequired = nRequired;
  }

  void put(ThriftFieldWrapper<F> fieldWrapper, Object value) {
    if (fieldWrapper.isRequired) {
      ++n;
    }
    record.setFieldValue(fieldWrapper.tFieldIdEnum, value);
  }

  T get() {
    if (n < nRequired) {
      for (ThriftFieldWrapper<F> fieldWrapper : fieldWrappers) {
        if (fieldWrapper.isRequired && !record.isSet(fieldWrapper.tFieldIdEnum)) {
          throw new AnoaJacksonTypeException(
              "Required field not set: " + fieldWrapper.tFieldIdEnum.getFieldName());
        }
      }
    }
    return record;
  }
}
