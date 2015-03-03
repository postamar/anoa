package com.adgear.anoa.factory;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.factory.util.JacksonFactory;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.function.Supplier;

abstract public class JacksonObjects<
    C extends ObjectMapper,
    F extends JsonFactory,
    S extends FormatSchema,
    P extends JsonParser,
    G extends JsonGenerator>
    extends JacksonFactory<ObjectNode, C, F, S, P, G> {

  JacksonObjects(C objectMapper) {
    this(objectMapper, Optional.<S>empty());
  }

  JacksonObjects(C objectMapper, S schema) {
    this(objectMapper, Optional.of(schema));
  }

  JacksonObjects(C objectMapper, Optional<S> schema) {
    super(((Supplier<C>) (() -> {
      objectMapper.findAndRegisterModules();
      return objectMapper;
    })).get(), schema);
  }

  public @NonNull TokenBuffer buffer() {
    return new TokenBuffer(objectCodec, false);
  }

  public @NonNull ObjectNode readValueAsTree(@NonNull TokenBuffer tokenBuffer) {
    try {
      return tokenBuffer.asParser(objectCodec).readValueAsTree();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
