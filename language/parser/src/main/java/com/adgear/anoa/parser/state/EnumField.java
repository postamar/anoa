package com.adgear.anoa.parser.state;

import com.adgear.anoa.parser.AnoaParseException;
import com.adgear.anoa.parser.Statement;

import java.util.Optional;

final class EnumField extends Field<EnumField> {

  EnumField(String name, String docString, int ordinal) {
    super(name.toUpperCase(), docString, ordinal);
  }

  private EnumField(EnumField old,
                    Optional<String> newDocString,
                    Optional<Boolean> removed) {
    super(old, Optional.<String>empty(), newDocString, Optional.<Boolean>empty(), removed);
  }

  boolean isDefault() {
    return ordinal == 0;
  }

  @Override
  public EnumField doName(Statement origin, String newName) {
    throw new AnoaParseException(origin, "cannot change name of enum field");
  }

  @Override
  public EnumField doDoc(Statement origin, String newDoc) {
    validateNewDocString(origin, newDoc);
    return new EnumField(this, Optional.of(newDoc), Optional.<Boolean>empty());
  }

  @Override
  public EnumField doDeprecate(Statement origin) {
    throw new AnoaParseException(origin, "cannot deprecate enum field");
  }

  @Override
  public EnumField doRemove(Statement origin) {
    validateRemoval(origin, true);
    if (isDefault()) {
      throw new AnoaParseException(origin, "cannot remove first enum field");
    }
    return new EnumField(this, Optional.<String>empty(), Optional.of(true));
  }

  @Override
  public EnumField doRestore(Statement origin) {
    validateRemoval(origin, false);
    return new EnumField(this, Optional.<String>empty(), Optional.of(false));
  }
}
