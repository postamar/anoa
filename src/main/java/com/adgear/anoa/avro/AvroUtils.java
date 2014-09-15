package com.adgear.anoa.avro;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.thrift.TBase;

/**
 * A collection of static functions for mapping SpecificRecords to Schemas.
 */
public class AvroUtils {

  /**
   * @param recordClass A class object for an Avro SpecificRecord or a Thrift TBase class.
   * @return The corresponding Avro Schema, if exists.
   * @throws java.lang.IllegalArgumentException If no schema exists.
   */
  public static Schema getSchema(Class<?> recordClass) throws IllegalArgumentException {
    if (SpecificRecord.class.isAssignableFrom(recordClass)) {
      return SpecificData.get().getSchema(recordClass);
    } else if (TBase.class.isAssignableFrom(recordClass)) {
      return ThriftDataModified.getModified().getSchema(recordClass);
    }
    throw new IllegalArgumentException("Class " + recordClass.getCanonicalName()
                                       + " is not an Avro Specific Record or a Thrift class");
  }

  /**
   * @param schema An Avro Schema for a SpecificRecord.
   * @return A class object for the corresponding SpecificRecord implementation.
   */
  @SuppressWarnings("unchecked")
  public static Class<? extends SpecificRecord> getSpecificClass(Schema schema) {
    return (Class<? extends SpecificRecord>) SpecificData.get().getClass(schema);
  }
}
