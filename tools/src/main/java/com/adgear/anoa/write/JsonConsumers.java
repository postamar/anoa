package com.adgear.anoa.write;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

/**
 * Utility class for writing Jackson records in the JSON format.
 */
public class JsonConsumers extends JacksonConsumersBase<
      ObjectMapper,
      JsonFactory,
      FormatSchema,
      JsonGenerator> {

  public JsonConsumers() {
    super(new ObjectMapper());
  }

  public /*@NonNull*/ TokenBuffer generator() {
    return (TokenBuffer) with(new TokenBuffer(objectCodec, false));
  }

  public /*@NonNull*/ TokenBufferWriteConsumer to() {
    return new TokenBufferWriteConsumer(generator());
  }
}
