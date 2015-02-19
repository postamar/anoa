package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.generic.GenericData;

import java.io.IOException;

class AvroFixedWriter extends JacksonWriter<GenericData.Fixed> {

  @Override
  public void write(GenericData.Fixed fixed, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeBinary(fixed.bytes());
  }
}
