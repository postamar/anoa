package com.adgear.anoa.read;

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
  public JacksonStreams(C objectCodec, Optional<S> schema) {
    this.objectCodec = objectCodec;
    this.factory = (F) objectCodec.getFactory();
    this.factory.setCodec(this.objectCodec);
    this.schema = schema;
  }

  @SuppressWarnings("unchecked")
  public P parser(InputStream inputStream) {
    try {
      return with((P) factory.createParser(new BufferedInputStream(inputStream)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public P parser(Reader reader) {
    try {
      return with((P) factory.createParser(new BufferedReader(reader)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public P parser(byte[] bytes) {
    try {
      return with((P) factory.createParser(bytes));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public P parser(String string) {
    try {
      return with((P) factory.createParser(string));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public P parser(File file) {
    try {
      return with((P) factory.createParser(file));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public P parser(URL url) {
    try {
      return with((P) factory.createParser(url));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @return the parser passed as argument, after setting the current object mapper and schema
   */
  public P with(P parser) {
    parser.setCodec(objectCodec);
    schema.ifPresent(parser::setSchema);
    return parser;
  }

  public Stream<N> from(P parser) {
    return LookAheadIteratorFactory.<N>jackson(parser).asStream();
  }

  public Stream<N> from(InputStream inputStream) {
    return from(parser(inputStream));
  }

  public Stream<N> from(Reader reader) {
    return from(parser(reader));
  }

  public Stream<N> from(byte[] bytes) {
    return from(parser(bytes));
  }

  public Stream<N> from(String string) {
    return from(parser(string));
  }

  public Stream<N> from(File file) {
    return from(parser(file));
  }

  public Stream<N> from(URL url) {
    return from(parser(url));
  }

  public <M> Stream<Anoa<N, M>> from(AnoaHandler<M> anoaHandler, P parser) {
    return LookAheadIteratorFactory.<N, M>jackson(anoaHandler, parser).asStream();
  }

  public <M> Stream<Anoa<N, M>> from(AnoaHandler<M> anoaHandler, InputStream inputStream) {
    return from(anoaHandler, parser(inputStream));
  }

  public <M> Stream<Anoa<N, M>> from(AnoaHandler<M> anoaHandler, Reader reader) {
    return from(anoaHandler, parser(reader));
  }

  public <M> Stream<Anoa<N, M>> from(AnoaHandler<M> anoaHandler, byte[] bytes) {
    return from(anoaHandler, parser(bytes));
  }

  public <M> Stream<Anoa<N, M>> from(AnoaHandler<M> anoaHandler, String string) {
    return from(anoaHandler, parser(string));
  }

  public <M> Stream<Anoa<N, M>> from(AnoaHandler<M> anoaHandler, File file) {
    return from(anoaHandler, parser(file));
  }

  public <M> Stream<Anoa<N, M>> from(AnoaHandler<M> anoaHandler, URL url) {
    return from(anoaHandler, parser(url));
  }
}