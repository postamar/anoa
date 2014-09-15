package com.adgear.anoa.avro.decode;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

public class SpecificDatumTextReader<T> extends GenericDatumTextReader<T> {

  public SpecificDatumTextReader(Class<T> c) {
    this(c, new SpecificData(c.getClassLoader()));
  }

  protected SpecificDatumTextReader(Class<T> c, SpecificData data) {
    super(data.getSchema(c), data);
  }

  public SpecificData getSpecificData() {
    return (SpecificData) data;
  }

  @Override
  public void setSchema(Schema actual) {
    // if expected is unset and actual is a specific record,
    // then default expected to schema of currently loaded class
    if (schema == null && actual != null && actual.getType() == Schema.Type.RECORD) {
      SpecificData data = getSpecificData();
      Class c = data.getClass(actual);
      if (c != null && SpecificRecord.class.isAssignableFrom(c)) {
        schema = data.getSchema(c);
      }
    }
    if (schema == null) {
      schema = actual;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Object createEnum(String symbol, Schema schema) {
    Class c = getSpecificData().getClass(schema);
    if (c == null) {
      return super.createEnum(symbol, schema); // punt to generic
    }
    return Enum.valueOf(c, symbol);
  }
}
