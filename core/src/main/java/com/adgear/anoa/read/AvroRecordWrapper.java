package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;

import org.apache.avro.generic.IndexedRecord;

import java.util.List;

class AvroRecordWrapper<R extends IndexedRecord> {

  final protected R record;
  final protected List<AvroFieldWrapper> fieldWrappers;
  final protected boolean[] flag;

  AvroRecordWrapper(R record, List<AvroFieldWrapper> fieldWrappers) {
    this.record = record;
    this.fieldWrappers = fieldWrappers;
    flag = new boolean[fieldWrappers.size()];
  }

  void put(AvroFieldWrapper fieldWrapper, Object value) {
    flag[fieldWrapper.index] = true;
    if (value != null) {
      record.put(fieldWrapper.field.pos(), value);
    } else if (fieldWrapper.defaultValue == null) {
      if (fieldWrapper.unboxed) {
        throw new AnoaJacksonTypeException(
            "Cannot set unboxed field to null: " + fieldWrapper.field.name());
      }
      record.put(fieldWrapper.field.pos(), null);
    } else {
      record.put(fieldWrapper.field.pos(), fieldWrapper.defaultValueCopy());
    }
  }

  R get() {
    for (AvroFieldWrapper fieldWrapper : fieldWrappers) {
      if (!flag[fieldWrapper.index]) {
        if (fieldWrapper.defaultValue != null) {
          record.put(fieldWrapper.field.pos(), fieldWrapper.defaultValueCopy());
        } else if (fieldWrapper.unboxed) {
          throw new AnoaJacksonTypeException(
              "Cannot leave unboxed field unset: " + fieldWrapper.field.name());
        }
      }
    }
    return record;
  }
}
