package com.adgear.anoa.read;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

/**
 * Utility class for streaming Jackson records from CBOR serializations.
 */
public class CborStreams extends JacksonStreamsBase<
    ObjectMapper,
    CBORFactory,
    FormatSchema,
    CBORParser> {

  public CborStreams() {
    super(new ObjectMapper(new CBORFactory()));
  }
}
