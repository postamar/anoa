package com.adgear.anoa.parser;

import com.adgear.anoa.parser.state.EnumState;
import com.adgear.anoa.parser.state.State;
import com.adgear.anoa.parser.state.StructState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class Declaration<R extends State> {

  final String name;

  final protected List<Statement> statements = new ArrayList<>();
  final protected Map<Statement, Declaration<?>> dependencies = new HashMap<>();

  private boolean tokenized = false;
  private boolean resolved = false;
  private boolean parsed = false;
  private R result = null;

  private Declaration(String name) {
    this.name = name;
  }

  static Declaration<?> tokenize(String name, Stream<String> input)
      throws AnoaSyntaxException {
    final Declaration<?> declaration;
    if (name.endsWith(".enum")) {
      declaration = new EnumDeclaration(name);
    } else if (name.endsWith(".struct")) {
      declaration = new StructDeclaration(name);
    } else {
      throw new AnoaSyntaxException(name, "name must end with '.enum' or '.struct'");
    }
    declaration.tokenize(input);
    return declaration;
  }

  private void tokenize(Stream<String> input) throws AnoaSyntaxException {
    assert (!tokenized);
    int lineCounter = 0;
    Iterator<String> iterator = input.iterator();
    while (iterator.hasNext()) {
      lineCounter++;
      final String line = iterator.next().trim();
      if (!line.isEmpty()) {
        Statement statement = Statement.tokenize(name, lineCounter, line);
        if (statement.dependsOnEnum || statement.dependsOnStruct) {
          dependencies.put(statement, null);
        }
        statements.add(statement);
      }
    }
    tokenized = true;
  }

  boolean resolveDependencies(Collection<Declaration<?>> declarations)
      throws AnoaDependencyException {
    if (tokenized && !resolved) {
      resolved = dependencies.entrySet().stream()
          .filter(e -> e.getValue() == null)
          .peek(e -> resolveDependency(e.getKey(), declarations).ifPresent(e::setValue))
          .allMatch(e -> e.getValue() != null);
      if (resolved) {
        declarations.add(this);
      }
    }
    return resolved;
  }

  void validateDependencies() {
    dependencies.forEach((statement, dependency) -> {
      if (dependency == null) {
        throw new AnoaDependencyException(statement, "dependency is unresolved or circular");
      }
    });
  }

  private Optional<Declaration<?>> resolveDependency(Statement statement,
                                                     Collection<Declaration<?>> declarations) {
    assert (statement.dependsOnEnum || statement.dependsOnStruct);
    final String reference = statement.structFieldType.get();
    List<Declaration<?>> resolved = declarations.stream()
        .filter(d -> d.name.endsWith(reference))
        .collect(Collectors.toList());
    if (resolved.size() > 1) {
      String message = resolved.stream()
          .map(d -> d.name)
          .sorted()
          .collect(Collectors.joining(", ", "ambiguous reference, could be any of ", "."));
      throw new AnoaDependencyException(statement, message);
    }
    return (resolved.isEmpty()) ? Optional.empty() : Optional.of(resolved.get(0));
  }

  SchemaGenerator parse() {
    if (resolved && !parsed) {
      result = parseOnce();
      parsed = true;
    }
    return result;
  }

  abstract protected R parseOnce();

  abstract protected UnaryOperator<StructState> parseStructWithDependency(Statement statement);

  protected R getResult() {
    return result;
  }

  static private class EnumDeclaration extends Declaration<EnumState> {

    private EnumDeclaration(String name) {
      super(name);
    }

    @Override
    protected EnumState parseOnce() {
      EnumState root = new EnumState(name);
      for (Statement statement : statements) {
        root = statement.parseEnum().apply(root);
      }
      root.validate();
      return root;
    }

    @Override
    protected UnaryOperator<StructState> parseStructWithDependency(Statement statement) {
      return statement.parseStruct(getResult(), null);
    }
  }

  static private class StructDeclaration extends Declaration<StructState> {

    private StructDeclaration(String name) {
      super(name);
    }

    @Override
    protected StructState parseOnce() {
      StructState root = new StructState(name);
      for (Statement statement : statements) {
        Declaration<?> dependency = dependencies.get(statement);
        UnaryOperator<StructState> fn = (dependency == null)
                                       ? statement.parseStruct(null, null)
                                       : dependency.parseStructWithDependency(statement);
        root = fn.apply(root);
      }
      root.validate();
      return root;
    }

    @Override
    protected UnaryOperator<StructState> parseStructWithDependency(Statement statement) {
      return statement.parseStruct(null, getResult());
    }
  }

}
