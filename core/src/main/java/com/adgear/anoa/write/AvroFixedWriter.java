package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.generic.GenericFixed;

import java.io.IOException;

class AvroFixedWriter extends AbstractWriter<GenericFixed> {

  @Override
  protected void write(GenericFixed fixed, JsonGenerator jacksonGenerator)
      throws IOException {
    jacksonGenerator.writeBinary(fixed.bytes());
  }
}
