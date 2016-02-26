package com.adgear.anoa.parser.state;

import com.adgear.anoa.parser.AnoaParseException;
import com.adgear.anoa.parser.Statement;

import java.util.Optional;

abstract public class Field<N extends Field<N>> extends Node<N> implements Comparable<Field> {

  final protected int ordinal;
  final protected boolean removed;

  protected Field(String name, String docString, int ordinal) {
    super(name, docString);
    this.ordinal = ordinal;
    this.removed = false;
  }

  protected Field(Field<N> old,
                  Optional<String> newName,
                  Optional<String> newDocstring,
                  Optional<Boolean> deprecated,
                  Optional<Boolean> removed) {
    super(old, newName.map(String::toUpperCase), newDocstring, deprecated);
    this.ordinal = old.ordinal;
    this.removed = removed.orElse(old.removed);
  }

  boolean isActive() {
    return !removed;
  }

  protected void validateRemoval(Statement origin, boolean newState) {
    if (newState == removed) {
      throw new AnoaParseException(origin, "new removal state is identical to current: " + removed);
    }
  }

  @Override
  public int compareTo(Field o) {
    return ordinal - o.ordinal;
  }
}
