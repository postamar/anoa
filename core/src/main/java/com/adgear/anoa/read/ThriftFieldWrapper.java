package com.adgear.anoa.read;

import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;

class ThriftFieldWrapper<F extends TFieldIdEnum> {

  final F tFieldIdEnum;
  final boolean isRequired;
  final AbstractReader<?> reader;

  ThriftFieldWrapper(F tFieldIdEnum, FieldMetaData fieldMetaData) {
    this.tFieldIdEnum = tFieldIdEnum;
    this.isRequired = (fieldMetaData.requirementType == TFieldRequirementType.REQUIRED);
    this.reader = createReader(fieldMetaData.valueMetaData);
  }

  @SuppressWarnings("unchecked")
  static private AbstractReader<?> createReader(FieldValueMetaData metaData) {
    switch (metaData.type) {
      case TType.BOOL:
        return new BooleanReader();
      case TType.BYTE:
        return new ByteReader();
      case TType.DOUBLE:
        return new DoubleReader();
      case TType.ENUM:
        return new EnumReader(((EnumMetaData) metaData).enumClass);
      case TType.I16:
        return new ShortReader();
      case TType.I32:
        return new IntegerReader();
      case TType.I64:
        return new LongReader();
      case TType.LIST:
        return new ListReader(createReader(((ListMetaData) metaData).elemMetaData));
      case TType.MAP:
        MapMetaData mapMetaData = (MapMetaData) metaData;
        if (mapMetaData.keyMetaData.type != TType.STRING) {
          throw new RuntimeException("Map key type is not string.");
        }
        return new MapReader(createReader(mapMetaData.valueMetaData));
      case TType.SET:
        return new SetReader(createReader((SetMetaData) metaData));
      case TType.STRUCT:
        return new ThriftReader(((StructMetaData) metaData).structClass);
      case TType.STRING:
        return metaData.isBinary() ? new ByteBufferReader() : new StringReader();
    }
    throw new RuntimeException("Unknown type in metadata " + metaData);
  }
}
