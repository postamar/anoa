package com.adgear.anoa.parser.state;

import com.adgear.anoa.parser.AnoaParseException;
import com.adgear.anoa.parser.Statement;

import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class Node<N extends Node<N>> {

  final protected String docString;
  final protected boolean deprecated;
  final private ArrayList<String> names;

  protected Node(String name, String docString) {
    names = new ArrayList<>();
    names.add(name);
    this.docString = docString;
    this.deprecated = false;
  }

  protected Node(Node<N> old,
                 Optional<String> newName,
                 Optional<String> newDocString,
                 Optional<Boolean> deprecated) {
    this.names = new ArrayList<>(old.names);
    newName.ifPresent(names::add);
    this.docString = newDocString.orElse(old.docString);
    this.deprecated = deprecated.orElse(old.deprecated);
  }

  final String getCurrentName() {
    return names.get(names.size() - 1);
  }

  final Stream<String> getOldNames() {
    return names.stream().sequential().skip(1);
  }

  protected void validateNewName(Statement origin, String newName) {
    if (newName.equals(getCurrentName())) {
      throw new AnoaParseException(origin, "new name is identical to current name");
    }
    if (getOldNames().anyMatch(newName::equals)) {
      throw new AnoaParseException(origin, "new name is identical to an old name");
    }
  }

  protected void validateNewDocString(Statement origin, String newDocString) {
    if (newDocString.equals(docString)) {
      throw new AnoaParseException(origin, "new docstring is identical to current docstring");
    }
  }

  protected void validateDeprecation(Statement origin, boolean newState) {
    if (newState == deprecated) {
      throw new AnoaParseException(origin,
                                   "new deprecation state is identical to current: " + deprecated);
    }
  }

  abstract public N doName(Statement origin, String newName);

  abstract public N doDoc(Statement origin, String newDoc);

  abstract public N doDeprecate(Statement origin);

  abstract public N doRemove(Statement origin);

  abstract public N doRestore(Statement origin);

  @Override
  public String toString() {
    return getClass().getSimpleName() + names.stream().collect(Collectors.joining(",", "[", "]"));
  }
}
