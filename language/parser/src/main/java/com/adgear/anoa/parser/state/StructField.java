package com.adgear.anoa.parser.state;


import com.adgear.anoa.parser.Statement;
import com.adgear.anoa.parser.type.FieldType;

import org.apache.avro.Schema;
import org.codehaus.jackson.node.BooleanNode;

import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Collectors;

final class StructField extends Field<StructField> {

  final FieldType fieldType;

  StructField(String name,
              String docString,
              int ordinal,
              FieldType fieldType) {
    super(name.toLowerCase(), docString, ordinal);
    this.fieldType = fieldType;
  }

  private StructField(StructField old,
                      Optional<String> newName,
                      Optional<String> newDocString,
                      Optional<Boolean> deprecated,
                      Optional<Boolean> removed) {
    super(old, newName, newDocString, deprecated, removed);
    this.fieldType = old.fieldType;
  }

  @Override
  public StructField doName(Statement origin, String newName) {
    validateNewName(origin, newName);
    return new StructField(this,
                           Optional.of(newName),
                           Optional.<String>empty(),
                           Optional.<Boolean>empty(),
                           Optional.<Boolean>empty());
  }

  @Override
  public StructField doDoc(Statement origin, String newDoc) {
    validateNewDocString(origin, newDoc);
    return new StructField(this,
                           Optional.<String>empty(),
                           Optional.of(newDoc),
                           Optional.<Boolean>empty(),
                           Optional.<Boolean>empty());
  }

  @Override
  public StructField doDeprecate(Statement origin) {
    validateDeprecation(origin, true);
    return new StructField(this,
                           Optional.<String>empty(),
                           Optional.<String>empty(),
                           Optional.of(true),
                           Optional.<Boolean>empty());
  }

  @Override
  public StructField doRemove(Statement origin) {
    validateRemoval(origin, true);
    return new StructField(this,
                           Optional.<String>empty(),
                           Optional.<String>empty(),
                           Optional.<Boolean>empty(),
                           Optional.of(true));
  }

  @Override
  public StructField doRestore(Statement origin) {
    validateDeprecation(origin, false);
    validateRemoval(origin, false);
    return new StructField(this,
                           Optional.<String>empty(),
                           Optional.<String>empty(),
                           Optional.of(false),
                           Optional.of(false));
  }

  Schema.Field getAvroField() {
    Schema.Field field = new Schema.Field(getCurrentName(),
                                          fieldType.avroSchema(),
                                          docString,
                                          fieldType.avroDefault().orElse(null));
    getOldNames().forEach(field::addAlias);
    if (deprecated) {
      field.addProp("deprecated", BooleanNode.TRUE);
    }
    return field;
  }

  String protoStatement() {
    String statement = fieldType.protoType() + " " + getCurrentName() + " = " + (ordinal + 1);
    ArrayList<String> options = new ArrayList<>();
    fieldType.protoOptions().ifPresent(options::add);
    if (deprecated) {
      options.add("deprecated=true");
    }
    if (options.isEmpty()) {
      return statement + ";";
    } else {
      return options.stream().collect(Collectors.joining(",", statement + " [", "];"));
    }
  }

  String thriftStatement() {
    String statement = "" + (ordinal + 1) + ": " + fieldType.thriftType() + " " + getCurrentName();
    if (fieldType.thriftDefault().isPresent()) {
      return statement + " = " + fieldType.thriftDefault().get() + ";";
    } else {
      return statement + ";";
    }
  }
}
