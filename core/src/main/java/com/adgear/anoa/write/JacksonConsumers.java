package com.adgear.anoa.write;

import checkers.nullness.quals.NonNull;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.Optional;

/**
 * Utility class for generating {@code WriteConsumer} instances to write Jackson
 * {@link com.fasterxml.jackson.core.TreeNode} instances.
 *
 * Intended to be used as a base class, subclasses should wrap appropriate Jackson databinding types
 * together; see the anoa-tools module for examples.
 *
 * @param <N> Record type
 * @param <C> Mapper type
 * @param <F> Factory type
 * @param <S> Schema type
 * @param <G> JsonGenerator type
 *
 */
public class JacksonConsumers<
    N extends TreeNode,
    C extends ObjectCodec,
    F extends JsonFactory,
    S extends FormatSchema,
    G extends JsonGenerator> {

  /**
   * The object mapper used by this instance
   */
  final public C objectCodec;

  /**
   * The factory used by this instance
   */
  final public F factory;

  /**
   * The format schema used by this instance, if present
   */
  final public Optional<S> schema;

  /**
   *
   * @param objectCodec Jackson object mapper instance
   * @param schema Jackson format schema (optional)
   */
  @SuppressWarnings("unchecked")
  public JacksonConsumers(@NonNull C objectCodec, @NonNull Optional<S> schema) {
    this.objectCodec = objectCodec;
    this.factory = (F) objectCodec.getFactory();
    this.factory.setCodec(objectCodec);
    this.schema = schema;
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
      return with((G) factory.createGenerator(new BufferedOutputStream(outputStream),
                                              JsonEncoding.UTF8));
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

  /**
   * @param generator
   * @return the same generator, with the format schema set
   */
  public @NonNull G with(@NonNull G generator) {
    schema.ifPresent(generator::setSchema);
    return generator;
  }

  public @NonNull WriteConsumer<N> to(@NonNull G generator) {
    return new TreeNodeWriteConsumer<>(generator);
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
