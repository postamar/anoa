package com.adgear.anoa.io.write.json;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.generic.GenericData;

import java.io.IOException;

class AvroFixedWriter extends JsonWriter<GenericData.Fixed> {

  @Override
  public void write(GenericData.Fixed fixed, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeBinary(fixed.bytes());
  }
}
