package com.adgear.anoa.factory;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.factory.util.JacksonReadIterator;
import com.adgear.anoa.factory.util.JacksonWriteConsumer;
import com.adgear.anoa.factory.util.WriteConsumer;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.net.URL;
import java.util.Optional;
import java.util.stream.Stream;

public class JacksonFactory<
    N extends TreeNode,
    C extends ObjectCodec,
    F extends JsonFactory,
    S extends FormatSchema,
    P extends JsonParser,
    G extends JsonGenerator> {

  final public C objectCodec;
  final public F factory;
  final public Optional<S> schema;

  @SuppressWarnings("unchecked")
  public JacksonFactory(@NonNull C objectCodec, @NonNull Optional<S> schema) {
    this.objectCodec = objectCodec;
    this.factory = (F) objectCodec.getFactory();
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

  public @NonNull P with(@NonNull P parser) {
    parser.setCodec(objectCodec);
    schema.ifPresent(parser::setSchema);
    return parser;
  }

  public @NonNull Stream<N> from(@NonNull P parser) {
    return new JacksonReadIterator<N,P>(parser).stream();
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

  @SuppressWarnings("unchecked")
  public @NonNull G generator(@NonNull Writer writer) {
    try {
      return with((G) factory.createGenerator(new BufferedWriter(writer)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull G generator(@NonNull OutputStream outputStream) {
    try {
      return with((G) factory.createGenerator(new BufferedOutputStream(outputStream), JsonEncoding.UTF8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull G generator(@NonNull File file) {
    try {
      return with((G) factory.createGenerator(file, JsonEncoding.UTF8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public @NonNull G with(@NonNull G generator) {
    schema.ifPresent(generator::setSchema);
    return generator;
  }

  public @NonNull WriteConsumer<N> to(@NonNull G generator) {
    return new JacksonWriteConsumer<>(generator);
  }

  public @NonNull WriteConsumer<N> to(@NonNull Writer writer) {
    return to(generator(writer));
  }

  public @NonNull WriteConsumer<N> to(@NonNull OutputStream outputStream) {
    return to(generator(outputStream));
  }

  public @NonNull WriteConsumer<N> to(@NonNull File file) {
    return to(generator(file));
  }
}
