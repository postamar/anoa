package com.adgear.anoa.read;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Utility class for deserializing Jackson {@link com.fasterxml.jackson.core.TreeNode} instances as
 * a {@link java.util.stream.Stream}.
 *
 * Intended to be used as a base class, subclasses should wrap appropriate Jackson databinding types
 * together; see the anoa-tools module for examples.
 *
 * @param <N> Record type
 * @param <C> Mapper type
 * @param <F> Factory type
 * @param <S> Schema type
 * @param <P> Parser type
 */
public class JacksonStreams<
    N extends TreeNode,
    C extends ObjectCodec,
    F extends JsonFactory,
    S extends FormatSchema,
    P extends JsonParser> {

  /**
   * The object mapper used by this instance
   */
  final public C objectCodec;

  /**
   * The factory used by this instance
   */
  final public F factory;

  /**
   * The (optional) format schema used by this instance
   */
  final public Optional<S> schema;

  /**
   * @param objectCodec Jackson object mapper instance
   * @param schema Jackson format schema (optional)
   */
  @SuppressWarnings("unchecked")
  public JacksonStreams(@NonNull C objectCodec, @NonNull Optional<S> schema) {
    this.objectCodec = objectCodec;
    this.factory = (F) objectCodec.getFactory();
    this.factory.setCodec(this.objectCodec);
    this.schema = schema;
  }

  @SuppressWarnings("unchecked")
  public @NonNull P parser(@NonNull InputStream inputStream) {
    try {
      return with((P) factory.createParser(new BufferedInputStream(inputStream)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull P parser(@NonNull Reader reader) {
    try {
      return with((P) factory.createParser(new BufferedReader(reader)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull P parser(@NonNull byte[] bytes) {
    try {
      return with((P) factory.createParser(bytes));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull P parser(@NonNull String string) {
    try {
      return with((P) factory.createParser(string));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull P parser(@NonNull File file) {
    try {
      return with((P) factory.createParser(file));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull P parser(@NonNull URL url) {
    try {
      return with((P) factory.createParser(url));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @return the parser passed as argument, after setting the current object mapper and schema
   */
  public @NonNull P with(@NonNull P parser) {
    parser.setCodec(objectCodec);
    schema.ifPresent(parser::setSchema);
    return parser;
  }

  public @NonNull Stream<N> from(@NonNull P parser) {
    return ReadIteratorUtils.<N>jackson(parser).stream();
  }

  public @NonNull Stream<N> from(@NonNull InputStream inputStream) {
    return from(parser(inputStream));
  }

  public @NonNull Stream<N> from(@NonNull Reader reader) {
    return from(parser(reader));
  }

  public @NonNull Stream<N> from(@NonNull byte[] bytes) {
    return from(parser(bytes));
  }

  public @NonNull Stream<N> from(@NonNull String string) {
    return from(parser(string));
  }

  public @NonNull Stream<N> from(@NonNull File file) {
    return from(parser(file));
  }

  public @NonNull Stream<N> from(@NonNull URL url) {
    return from(parser(url));
  }

  public <M> @NonNull Stream<Anoa<N, M>> from(@NonNull AnoaHandler<M> anoaHandler,
                                              @NonNull P parser) {
    return ReadIteratorUtils.<N, M>jackson(anoaHandler, parser).stream();
  }

  public <M> @NonNull Stream<Anoa<N, M>> from(@NonNull AnoaHandler<M> anoaHandler,
                                              @NonNull InputStream inputStream) {
    return from(anoaHandler, parser(inputStream));
  }

  public <M> @NonNull Stream<Anoa<N, M>> from(@NonNull AnoaHandler<M> anoaHandler,
                                              @NonNull Reader reader) {
    return from(anoaHandler, parser(reader));
  }

  public <M> @NonNull Stream<Anoa<N, M>> from(@NonNull AnoaHandler<M> anoaHandler,
                                              @NonNull byte[] bytes) {
    return from(anoaHandler, parser(bytes));
  }

  public <M> @NonNull Stream<Anoa<N, M>> from(@NonNull AnoaHandler<M> anoaHandler,
                                              @NonNull String string) {
    return from(anoaHandler, parser(string));
  }

  public <M> @NonNull Stream<Anoa<N, M>> from(@NonNull AnoaHandler<M> anoaHandler,
                                              @NonNull File file) {
    return from(anoaHandler, parser(file));
  }

  public <M> @NonNull Stream<Anoa<N, M>> from(@NonNull AnoaHandler<M> anoaHandler,
                                              @NonNull URL url) {
    return from(anoaHandler, parser(url));
  }
}