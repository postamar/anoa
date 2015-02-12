package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.StructMetaData;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


class StructParser<F extends TFieldIdEnum, T extends TBase<T,F>> extends JsonNodeParser<T> {

  static protected class Field<F extends TFieldIdEnum> {

    public Field(F tFieldIdEnum, boolean isRequired, JsonNodeParser parser) {
      this.tFieldIdEnum = tFieldIdEnum;
      this.isRequired = isRequired;
      this.parser = parser;
    }

    final protected F tFieldIdEnum;
    final protected boolean isRequired;
    final protected JsonNodeParser parser;
  }

  final private Map<String,Field<F>> fieldLookUp;
  final private T instance;
  final private int nRequired;

  @SuppressWarnings("unchecked")
  protected T newInstance() {
    return (T) instance.deepCopy();
  }

  @SuppressWarnings("unchecked")
  StructParser(StructMetaData metaData) {
    try {
      instance = (T) metaData.structClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    instance.clear();
    fieldLookUp = new HashMap<>();
    int n = 0;
    final Map<F,FieldMetaData> metaDataMap = (Map<F,FieldMetaData>)
        FieldMetaData.getStructMetaDataMap(metaData.structClass);
    for (Map.Entry<F,FieldMetaData> entry : metaDataMap.entrySet()) {
      final FieldMetaData fieldMetaData = entry.getValue();
      final boolean required = (fieldMetaData.requirementType == TFieldRequirementType.REQUIRED);
      n += required ? 1 : 0;
      fieldLookUp.put(entry.getKey().getFieldName(),
                      new Field<>(entry.getKey(),
                                  required,
                                  create(entry.getValue().valueMetaData)));
    }
    nRequired = n;
  }

  @Override
  T parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isObject()) {
      return null;
    }
    T result = newInstance();
    Iterator<Map.Entry<String,JsonNode>> iterator = node.fields();
    while (iterator.hasNext()) {
      Map.Entry<String,JsonNode> entry = iterator.next();
      Field<F> field = fieldLookUp.get(entry.getKey());
      if (field != null) {
        result.setFieldValue(field.tFieldIdEnum, field.parser.parse(entry.getValue()));
      }
    }
    return result;
  }

  @Override
  T parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isObject()) {
      return null;
    }
    T result = newInstance();
    Iterator<Map.Entry<String,JsonNode>> iterator = node.fields();
    int n = nRequired;
    while (iterator.hasNext()) {
      Map.Entry<String,JsonNode> entry = iterator.next();
      Field<F> field = fieldLookUp.get(entry.getKey());
      if (field != null) {
        result.setFieldValue(field.tFieldIdEnum, field.parser.parseStrict(entry.getValue()));
        n -= field.isRequired ? 1 : 0;
      }
    }
    if (n > 0) {
      for (Field<F> field : fieldLookUp.values()) {
        if (!result.isSet(field.tFieldIdEnum)) {
          throw new TException("Required field not set: " + field.tFieldIdEnum.getFieldName());
        }
      }
    }
    return result;
  }
}
