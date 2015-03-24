package com.adgear.anoa.write;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;

/**
 * Utility class for writing Jackson records in the CBOR format.
 */
public class CborConsumers extends JacksonConsumersBase<
    ObjectMapper,
    CBORFactory,
    FormatSchema,
    CBORGenerator> {

  public CborConsumers() {
    super(new ObjectMapper(new CBORFactory()));
  }
}
