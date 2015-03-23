package com.adgear.anoa;

import org.apache.avro.specific.SpecificRecord;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

public class AnoaReflectionUtils {

  @SuppressWarnings("unchecked")
  static public Class<? extends SpecificRecord> getAvroClass(String className)
      throws ClassNotFoundException {
    if (className == null) {
      throw new ClassNotFoundException("Class name must not be null.");
    }
    Class recordClass = Class.forName(className);
    if (!SpecificRecord.class.isAssignableFrom(recordClass)) {
      throw new ClassCastException(className + " does not implement SpecificRecord.");
    }
    return (Class<? extends SpecificRecord>) recordClass;
  }


  @SuppressWarnings("unchecked")
  static public <T extends TBase> Class<T> getThriftClass(
      String className) throws ClassNotFoundException{
    if (className == null) {
      throw new ClassNotFoundException("Class name must not be null.");
    }
    Class recordClass = Class.forName(className);
    if (!TBase.class.isAssignableFrom(recordClass)) {
      throw new ClassCastException(className + " does not implement SpecificRecord.");
    }
    return (Class<T>) recordClass;
  }

  @SuppressWarnings("unchecked")
  public static <F extends TFieldIdEnum>
  LinkedHashMap<F, FieldMetaData> getThriftMetaDataMap(Class<? extends TBase<?,F>> thriftClass) {
    LinkedHashMap<F, FieldMetaData> result = new LinkedHashMap<>();
    ((Map<F, FieldMetaData>) FieldMetaData.getStructMetaDataMap(thriftClass)).entrySet().stream()
        .sorted(comparator)
        .forEach(e -> result.put(e.getKey(), e.getValue()));
    return result;
  }

  static private Comparator<Map.Entry<? extends TFieldIdEnum, FieldMetaData>> comparator
      = new Comparator<Map.Entry<? extends TFieldIdEnum, FieldMetaData>>() {
    @Override
    public int compare(Map.Entry<? extends TFieldIdEnum, FieldMetaData> o1,
                       Map.Entry<? extends TFieldIdEnum, FieldMetaData> o2) {
      return o1.getKey().getThriftFieldId() - o2.getKey().getThriftFieldId();
    }
  };
}
