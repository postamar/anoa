package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.util.List;

class AvroRecordWrapper<R extends IndexedRecord> implements RecordWrapper<R, AvroFieldWrapper> {

  final protected R record;
  final protected List<AvroFieldWrapper> fieldWrappers;
  final protected boolean[] flag;

  AvroRecordWrapper(R record, List<AvroFieldWrapper> fieldWrappers) {
    this.record = record;
    this.fieldWrappers = fieldWrappers;
    flag = new boolean[fieldWrappers.size()];
  }

  @Override
  public void put(AvroFieldWrapper fieldWrapper, Object value) {
    flag[fieldWrapper.index] = true;
    if (value != null) {
      record.put(fieldWrapper.field.pos(), value);
    } else if (fieldWrapper.hasDefaultValue()) {
      record.put(fieldWrapper.field.pos(), fieldWrapper.defaultValueCopy());
    } else {
      if (fieldWrapper.unboxed) {
        throw new AnoaJacksonTypeException(
            "Cannot set unboxed field to null: " + fieldWrapper.field.name());
      }
      record.put(fieldWrapper.field.pos(), null);
    }
  }

  @Override
  public R get() {
    for (AvroFieldWrapper fieldWrapper : fieldWrappers) {
      if (!flag[fieldWrapper.index]) {
        if (fieldWrapper.hasDefaultValue()) {
          record.put(fieldWrapper.field.pos(), fieldWrapper.defaultValueCopy());
        } else if (fieldWrapper.unboxed) {
          throw new AnoaJacksonTypeException(
              "Cannot leave unboxed field unset: " + fieldWrapper.field.name());
        }
      }
    }
    if (record instanceof SpecificRecord) {
      SpecificData.get().validate(record.getSchema(), record);
    } else {
      GenericData.get().validate(record.getSchema(), record);
    }
    return record;
  }
}
