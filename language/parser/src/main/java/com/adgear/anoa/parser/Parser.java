package com.adgear.anoa.parser;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

final public class Parser implements Function<Stream<String>, Optional<Stream<SchemaGenerator>>> {

  static final public Pattern NAME_PATTERN = Pattern.compile(
      "((?:[a-z][a-z0-9]*\\.)*[a-z][a-z0-9]*(?:_[a-z][a-z0-9]*)*)\\.(?:struct|enum)");

  final private Consumer<String> logError;
  final private Function<String, Stream<String>> reader;

  private LinkedHashMap<String, Declaration<?>> declarations;
  private boolean hasErrors;

  public Parser(Consumer<String> logError, Function<String, Stream<String>> reader) {
    this.logError = logError;
    this.reader = reader;
  }

  private void error(Exception e) {
    logError.accept(e.getMessage());
    hasErrors = true;
  }

  @Override
  public Optional<Stream<SchemaGenerator>> apply(Stream<String> names) {
    // initialize state
    declarations = new LinkedHashMap<>();
    hasErrors = false;
    // tokenize input
    names.forEach(this::tokenize);
    // resolve dependencies
    List<Declaration<?>> ordered = orderedDeclarations();
    // parse
    ordered.forEach(this::parse);
    // return results
    return hasErrors
           ? Optional.<Stream<SchemaGenerator>>empty()
           : Optional.of(ordered.stream().map(Declaration::parse));
  }

  private void tokenize(String name) {
    try {
      if (!NAME_PATTERN.matcher(name).matches()) {
        throw new AnoaSyntaxException(name, "invalid name");
      }
      if (declarations.containsKey(name)) {
        throw new AnoaSyntaxException(name, "a declaration with that name already exists");
      }
      declarations.put(name, Declaration.tokenize(name, reader.apply(name)));
    } catch (AnoaSyntaxException | UncheckedIOException e) {
      error(e);
    }
  }

  private List<Declaration<?>> orderedDeclarations() {
    List<Declaration<?>> resolved = new ArrayList<>();
    int oldSize = -1;
    while (oldSize < resolved.size()) {
      oldSize = resolved.size();
      declarations.values().forEach(declaration -> {
        try {
          declaration.resolveDependencies(resolved);
        } catch (AnoaDependencyException e) {
          error(e);
        }
      });
    }
    declarations.values().forEach(declaration -> {
      try {
        declaration.validateDependencies();
      } catch (AnoaDependencyException e) {
        error(e);
      }
    });
    assert (resolved.size() == declarations.size());
    return resolved;
  }

  private void parse(Declaration<?> declaration) {
    try {
      declaration.parse();
    } catch (AnoaParseException e) {
      error(e);
    }
  }

}
