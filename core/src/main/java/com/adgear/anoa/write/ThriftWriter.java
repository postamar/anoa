package com.adgear.anoa.write;

import com.adgear.anoa.AnoaReflectionUtils;
import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;
import org.jooq.lambda.Unchecked;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

class ThriftWriter<F extends TFieldIdEnum, T extends TBase<?, F>> extends AbstractRecordWriter<T> {

  final Map<F, AbstractWriter<Object>> fieldWriters;
  final Map<F, Object> fieldDefaults;

  @SuppressWarnings("unchecked")
  ThriftWriter(Class<T> thriftClass) {
    fieldWriters = new LinkedHashMap<>();
    fieldDefaults = new HashMap<>();
    T originalInstance = Unchecked.supplier(thriftClass::newInstance).get();
    T defaultInstance = createDefaultValue(thriftClass);
    AnoaReflectionUtils.getThriftMetaDataMap(thriftClass).forEach((f, md) -> {
      fieldWriters.put(f, (AbstractWriter<Object>) createWriter(md.valueMetaData));
      if (originalInstance.getFieldValue(f) == null) {
        fieldDefaults.put(f, defaultInstance.getFieldValue(f));
      }
    });

  }

  static private <F extends TFieldIdEnum, T extends TBase<?, F>>
  T createDefaultValue(Class<T> thriftClass) {
    final T instance = Unchecked.supplier(thriftClass::newInstance).get();
    AnoaReflectionUtils.getThriftMetaDataMap(thriftClass).forEach((f, md) -> {
      if (instance.getFieldValue(f) == null) {
        switch (md.valueMetaData.type) {
          case TType.LIST:
            instance.setFieldValue(f, new ArrayList<>());
            break;
          case TType.MAP:
            instance.setFieldValue(f, new HashMap<>());
            break;
          case TType.SET:
            instance.setFieldValue(f, new HashSet<>());
            break;
          case TType.STRUCT:
            Object subStruct = createDefaultValue(((StructMetaData) md.valueMetaData).structClass);
            instance.setFieldValue(f, subStruct);
            break;
        }
      }
    });
    return instance;
  }

  static private AbstractWriter<?> createWriter(FieldValueMetaData metaData) {
    switch (metaData.type) {
      case TType.BOOL:
        return new BooleanWriter();
      case TType.BYTE:
        return new ByteWriter();
      case TType.DOUBLE:
        return new DoubleWriter();
      case TType.ENUM:
        return new EnumWriter();
      case TType.I16:
        return new ShortWriter();
      case TType.I32:
        return new IntegerWriter();
      case TType.I64:
        return new LongWriter();
      case TType.LIST:
        return new CollectionWriter<>(createWriter(((ListMetaData) metaData).elemMetaData));
      case TType.MAP:
        MapMetaData mapMetaData = (MapMetaData) metaData;
        if (mapMetaData.keyMetaData.type != TType.STRING) {
          throw new RuntimeException("Map key type is not string.");
        }
        return new MapWriter<>(createWriter(mapMetaData.valueMetaData));
      case TType.SET:
        return new CollectionWriter<>(createWriter(((SetMetaData) metaData).elemMetaData));
      case TType.STRUCT:
        return new ThriftWriter<>(((StructMetaData) metaData).structClass);
      case TType.STRING:
        return metaData.isBinary() ? new ByteArrayWriter() : new StringWriter();
    }
    throw new RuntimeException("Unknown type in metadata " + metaData);
  }

  @Override
  protected void write(T t, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeStartObject();
    for (Map.Entry<F, AbstractWriter<Object>> entry : fieldWriters.entrySet()) {
      F f = entry.getKey();
      t.getFieldValue(f);
      if (t.isSet(f)) {
        Object value = t.getFieldValue(f);
        Object defaultValue = fieldDefaults.get(f);
        if (value != null && !value.equals(defaultValue)) {
          jacksonGenerator.writeFieldName(f.getFieldName());
          entry.getValue().write(value, jacksonGenerator);
        }
      }
    }
    jacksonGenerator.writeEndObject();
  }
}
