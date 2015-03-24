package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.generic.GenericData;

import java.io.IOException;

class AvroFixedWriter extends AbstractWriter<GenericData.Fixed> {

  @Override
  protected void writeChecked(GenericData.Fixed fixed, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeBinary(fixed.bytes());
  }
}
