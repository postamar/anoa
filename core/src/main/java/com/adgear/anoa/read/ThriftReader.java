package com.adgear.anoa.read;

import com.adgear.anoa.AnoaReflectionUtils;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;
import org.jooq.lambda.Unchecked;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

class ThriftReader<F extends TFieldIdEnum, T extends TBase<?, F>>
    extends AbstractRecordReader<T, ThriftFieldWrapper<F>> {

  final private Supplier<T> constructor;
  final private int nRequired;

  ThriftReader(Class<T> thriftClass) {
    super(buildFieldWrappers(thriftClass).values().stream());
    this.constructor = getConstructor(thriftClass);
    this.nRequired = (int) fieldWrappers.stream().filter(w -> w.isRequired).count();
  }

  static private <F extends TFieldIdEnum, T extends TBase<?, F>>
  Map<F, ThriftFieldWrapper<F>> buildFieldWrappers(Class<T> thriftClass) {
    Map<F, ThriftFieldWrapper<F>> map = new LinkedHashMap<>();
    AnoaReflectionUtils.getThriftMetaDataMap(thriftClass).forEach((f, md) -> {
      Supplier<Object> supplier = buildDefaultValueSupplier(thriftClass, f, md.valueMetaData);
      map.put(f, new ThriftFieldWrapper<>(f, md, supplier));
    });
    return map;
  }

  static private <F extends TFieldIdEnum, T extends TBase<?, F>>
  Supplier<T> getConstructor(Class<T> thriftClass) {
    return Unchecked.supplier(thriftClass::newInstance);
  }

  @SuppressWarnings("unchecked")
  static private <F extends TFieldIdEnum, T extends TBase<?, F>>
  Supplier<Object> buildDefaultValueSupplier(Class<T> thriftClass, F f, FieldValueMetaData vmd) {
    Supplier<T> constructor = getConstructor(thriftClass);
    if (vmd.isBinary()) {
      return () -> ByteBuffer.wrap((byte[]) constructor.get().getFieldValue(f));
    } else if (vmd.isStruct()) {
      final Class<? extends TBase> subClass = ((StructMetaData) vmd).structClass;
      return Unchecked.supplier(subClass::newInstance);
    } else if (vmd.isContainer()) {
      switch (vmd.type) {
        case TType.LIST:
          return ArrayList::new;
        case TType.SET:
          return HashSet::new;
        case TType.MAP:
          return HashMap::new;
      }
    }
    return () -> constructor.get().getFieldValue(f);
  }

  @SuppressWarnings("unchecked")
  protected ThriftRecordWrapper<F, T> newWrappedInstance() {
    return new ThriftRecordWrapper<>(constructor.get(), fieldWrappers, nRequired);
  }
}
