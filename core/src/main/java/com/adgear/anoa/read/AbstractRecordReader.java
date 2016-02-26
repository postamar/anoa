package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class AbstractRecordReader<R, F extends FieldWrapper>
    extends AbstractReader<R> {

  final private Map<String, Optional<F>> fieldLookUp;
  final protected List<F> fieldWrappers;

  protected AbstractRecordReader(Stream<F> fieldWrappers) {
    this.fieldLookUp = new HashMap<>();
    this.fieldWrappers = fieldWrappers.collect(Collectors.toList());
    this.fieldWrappers.forEach(
        fw -> fw.getNames().flatMap(AbstractRecordReader::fieldNames).forEach(
            name -> fieldLookUp.put(name, Optional.of(fw))));
  }

  <P extends JsonParser> Function<P, R> decoder(boolean strict) {
    if (Boolean.TRUE.equals(strict)) {
      return (P jp) -> {
        try {
          return readRecordStrict(jp);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };
    } else {
      return (P jp) -> {
        try {
          return readRecord(jp);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };
    }
  }

  <P extends JsonParser, M> Function<Anoa<P, M>, Anoa<R, M>> decoder(
      AnoaHandler<M> anoaHandler,
      boolean strict) {
    if (Boolean.TRUE.equals(strict)) {
      return anoaHandler.functionChecked(this::readRecordStrict);
    } else {
      return anoaHandler.functionChecked(this::readRecord);
    }
  }

  Stream<R> stream(
      boolean strict,
      JsonParser jacksonParser) {
    return LookAheadIteratorFactory.jackson(jacksonParser).asStream()
        .map(TreeNode::traverse)
        .map(decoder(strict));
  }

  <M> Stream<Anoa<R, M>> stream(
      AnoaHandler<M> anoaHandler,
      boolean strict,
      JsonParser jacksonParser) {
    return LookAheadIteratorFactory.jackson(anoaHandler, jacksonParser).asStream()
        .map(anoaHandler.function(TreeNode::traverse))
        .map(decoder(anoaHandler, strict));
  }

  private R readRecord(JsonParser jacksonParser) throws IOException {
    jacksonParser.nextToken();
    return read(jacksonParser);
  }

  private R readRecordStrict(JsonParser jacksonParser) throws IOException {
    jacksonParser.nextToken();
    return readStrict(jacksonParser);
  }

  abstract protected RecordWrapper<R, F> newWrappedInstance();

  @Override
  protected R read(JsonParser jacksonParser) throws IOException {
    if (jacksonParser.getCurrentToken() == JsonToken.START_OBJECT) {
      final RecordWrapper<R, F> recordWrapper = newWrappedInstance();
      doMap(jacksonParser, (fieldName, p) -> {
        final Optional<F> fieldWrapper = fieldLookUp.get(fieldName);
        if (fieldWrapper == null) {
          fieldLookUp.put(fieldName, Optional.<F>empty());
        } else if (fieldWrapper.isPresent()) {
          recordWrapper.put(fieldWrapper.get(), fieldWrapper.get().getReader().read(p));
        } else {
          gobbleValue(p);
        }
      });
      return recordWrapper.get();
    } else {
      gobbleValue(jacksonParser);
      return null;
    }
  }

  @Override
  protected R readStrict(JsonParser jacksonParser) throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_OBJECT:
        final RecordWrapper<R, F> recordWrapper = newWrappedInstance();
        doMap(jacksonParser, (fieldName, p) -> {
          final Optional<F> fieldWrapper = fieldLookUp.get(fieldName);
          if (fieldWrapper == null) {
            fieldLookUp.put(fieldName, Optional.<F>empty());
          } else if (fieldWrapper.isPresent()) {
            recordWrapper.put(fieldWrapper.get(), fieldWrapper.get().getReader().readStrict(p));
          } else {
            gobbleValue(p);
          }
        });
        return recordWrapper.get();
      default:
        throw new AnoaJacksonTypeException("Token is not '{': " + jacksonParser.getCurrentToken());
    }
  }

  static private Stream<String> fieldNames(String name) {
    Pattern camelCasePattern = Pattern.compile("_(.)");
    Matcher camelCaseMatcher = camelCasePattern.matcher(name);
    StringBuffer camelCaseBuffer = new StringBuffer();
    while (camelCaseMatcher.find()) {
      camelCaseMatcher.appendReplacement(camelCaseBuffer, camelCaseMatcher.group(1).toUpperCase());
    }
    camelCaseMatcher.appendTail(camelCaseBuffer);
    String camelCase = camelCaseBuffer.toString();


    Pattern lowercase_pattern = Pattern.compile("[a-z0-9]([A-Z])");
    Matcher lowercase_matcher = lowercase_pattern.matcher(name);
    StringBuffer lowercase_buffer = new StringBuffer();
    while (lowercase_matcher.find()) {
      lowercase_matcher.appendReplacement(lowercase_buffer,
                                          "_" + lowercase_matcher.group(1).toLowerCase());
    }
    lowercase_matcher.appendTail(lowercase_buffer);
    String lowercase_underscore = lowercase_buffer.toString();

    return Stream.of(camelCase,
                     lowercase_underscore.toLowerCase(),
                     lowercase_underscore.toUpperCase(),
                     name);
  }
}
