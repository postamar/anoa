package com.adgear.anoa.read;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

/**
 * Utility class for streaming Jackson records from JSON serializations.
 */
public class JsonStreams extends JacksonStreamsBase<
    ObjectMapper,
    JsonFactory,
    FormatSchema,
    JsonParser> {

  public JsonStreams() {
    super(new ObjectMapper());
  }

  public JsonParser parser(TokenBuffer tokenBuffer) {
    return tokenBuffer.asParser(objectCodec);
  }

  /**
   * @return Object-mapping function appliable to TokenBuffer instances
   */
  public Function<TokenBuffer, ObjectNode> tokenBuffer() {
    return (TokenBuffer tb) -> {
      try {
        return parser(tb).readValueAsTree();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param <M> Metadata type
   * @return Object-mapping function appliable to TokenBuffer instances
   */
  public <M> @NonNull Function<Anoa<TokenBuffer, M>, Anoa<ObjectNode, M>> tokenBuffer(
      @NonNull AnoaFactory<M> anoaFactory) {
    return anoaFactory.functionChecked((TokenBuffer tb) -> parser(tb).readValueAsTree());
  }
}
