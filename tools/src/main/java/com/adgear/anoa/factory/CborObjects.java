package com.adgear.anoa.factory;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

public class CborObjects extends JacksonObjects<
    ObjectMapper,
    CBORFactory,
    FormatSchema,
    CBORParser,
    CBORGenerator> {

  public CborObjects() {
    super(new ObjectMapper(new CBORFactory()));
  }
}
