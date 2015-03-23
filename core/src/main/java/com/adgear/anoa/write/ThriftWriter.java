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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

class ThriftWriter<F extends TFieldIdEnum, T extends TBase<?, F>> extends AbstractWriter<T> {

  final private LinkedHashMap<F, AbstractWriter<Object>> fieldMap;

  @SuppressWarnings("unchecked")
  ThriftWriter(Class<T> thriftClass) {
    fieldMap = new LinkedHashMap<>();
    AnoaReflectionUtils.getThriftMetaDataMap(thriftClass).entrySet().stream()
        .forEach(e -> fieldMap.put(
            (F) e.getKey(),
            (AbstractWriter<Object>) createWriter(e.getValue().valueMetaData)));
  }

  @Override
  protected void writeChecked(T t, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeStartObject();
    for (Map.Entry<F,AbstractWriter<Object>> entry : fieldMap.entrySet()) {
      F f = entry.getKey();
      if (t.isSet(f)) {
        jacksonGenerator.writeFieldName(f.getFieldName());
        entry.getValue().writeChecked(t.getFieldValue(f), jacksonGenerator);
      }
    }
    jacksonGenerator.writeEndObject();
  }

  static protected AbstractWriter<?> createWriter(FieldValueMetaData metaData) {
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
}
