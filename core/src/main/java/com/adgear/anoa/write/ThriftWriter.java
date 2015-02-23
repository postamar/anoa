package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

class ThriftWriter<F extends TFieldIdEnum, T extends TBase<T,F>> extends JacksonWriter<T> {

  final private LinkedHashMap<F,JacksonWriter<Object>> fieldMap;

  @SuppressWarnings("unchecked")
  ThriftWriter(Class<T> thriftClass) {
    fieldMap = new LinkedHashMap<>();
    FieldMetaData.getStructMetaDataMap(thriftClass).entrySet().stream()
        .sorted(new Comparator<Map.Entry<? extends TFieldIdEnum, FieldMetaData>>() {
          @Override
          public int compare(Map.Entry<? extends TFieldIdEnum, FieldMetaData> o1,
                             Map.Entry<? extends TFieldIdEnum, FieldMetaData> o2) {
            return o1.getKey().getThriftFieldId() - o2.getKey().getThriftFieldId();
          }
        })
        .forEach(e -> fieldMap.put((F) e.getKey(),
                                   (JacksonWriter<Object>) createWriter(e.getValue().valueMetaData)));
  }

  @Override
  public void write(T t, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartObject();
    for (Map.Entry<F,JacksonWriter<Object>> entry : fieldMap.entrySet()) {
      F f = entry.getKey();
      if (t.isSet(f)) {
        jsonGenerator.writeFieldName(f.getFieldName());
        entry.getValue().write(t.getFieldValue(f), jsonGenerator);
      }
    }
    jsonGenerator.writeEndObject();
  }

  static protected JacksonWriter<?> createWriter(FieldValueMetaData metaData) {
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
