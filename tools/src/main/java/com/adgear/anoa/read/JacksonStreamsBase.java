package com.adgear.anoa.read;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Optional;

class JacksonStreamsBase<
    M extends ObjectMapper,
    F extends JsonFactory,
    S extends FormatSchema,
    P extends JsonParser>
    extends JacksonStreams<ObjectNode, M, F, S, P> {

  JacksonStreamsBase(/*@NonNull*/ M objectMapper) {
    this(objectMapper, Optional.<S>empty());
  }

  JacksonStreamsBase(/*@NonNull*/ M objectMapper, /*@NonNull*/ Optional<S> schema) {
    super(objectMapper, schema);
    objectMapper.findAndRegisterModules();
  }
}
