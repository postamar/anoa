package com.adgear.anoa.avro.encode;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificData;

import java.io.IOException;


public class SpecificDatumTextWriter<T> extends GenericDatumTextWriter<T> {

  public SpecificDatumTextWriter() {
    super(SpecificData.get());
  }

  public SpecificDatumTextWriter(Class<T> c) {
    super(SpecificData.get().getSchema(c), SpecificData.get());
  }

  public SpecificDatumTextWriter(Schema schema) {
    super(schema, SpecificData.get());
  }

  public SpecificDatumTextWriter(Schema root, SpecificData specificData) {
    super(root, specificData);
  }

  protected SpecificDatumTextWriter(SpecificData specificData) {
    super(specificData);
  }

  @Override
  protected void writeEnum(Schema schema, Object datum, Encoder out) throws IOException {
    if (datum instanceof Enum) {
      if (enumsAsString) {
        writeString(datum.toString(), out);
      } else {
        out.writeEnum(((Enum) datum).ordinal());
      }
    } else {
      super.writeEnum(schema, datum, out);  // punt to generic
    }
  }
}
