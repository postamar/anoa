package com.adgear.anoa.parser.state;

import com.adgear.anoa.parser.AnoaParseException;
import com.adgear.anoa.parser.Statement;

import org.apache.avro.Schema;
import org.codehaus.jackson.node.BooleanNode;

import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

final public class EnumState extends State<EnumField, EnumState> {

  public EnumState(String name) {
    super(name, "enum");
  }

  private EnumState(EnumState old,
                    Optional<String> newDocString,
                    Optional<Boolean> deprecated,
                    Optional<EnumField> newChild) {
    super(old, newDocString, deprecated, newChild);
  }

  private EnumState(EnumState old, Optional<EnumField> field, UnaryOperator<EnumField> fn) {
    super(old, field, fn);
  }

  @Override
  public EnumState doDoc(Statement origin, String newDoc) {
    validateNewDocString(origin, newDoc);
    return new EnumState(this,
                        Optional.of(newDoc),
                        Optional.<Boolean>empty(),
                        Optional.<EnumField>empty());
  }

  @Override
  public EnumState doDeprecate(Statement origin) {
    validateDeprecation(origin, true);
    return new EnumState(this,
                        Optional.<String>empty(),
                        Optional.of(true),
                        Optional.<EnumField>empty());
  }

  @Override
  public EnumState doRemove(Statement origin) {
    throw new AnoaParseException(origin, "cannot remove enum declaration");
  }

  @Override
  public EnumState doRestore(Statement origin) {
    validateDeprecation(origin, false);
    return new EnumState(this,
                        Optional.<String>empty(),
                        Optional.of(false),
                        Optional.<EnumField>empty());
  }

  @Override
  public EnumState doChild(Statement origin, String name, UnaryOperator<EnumField> fn) {
    return new EnumState(this, Optional.of(findChild(origin, name)), fn);
  }

  public EnumState addEnumField(String name, String docString) {
    return new EnumState(this,
                        Optional.<String>empty(),
                        Optional.<Boolean>empty(),
                        Optional.of(new EnumField(name, docString, getNextOrdinal())));
  }

  EnumField getDefaultField() {
    return children().filter(c -> !c.removed).findFirst().get();
  }

  public EnumField getDefault(Optional<String> label) {
    return getDefaultField();
  }

  @Override
  public String protoSchema() {
    StringBuilder header = new StringBuilder();
    header.append("package ").append(getCurrentName()).append(";\n");
    header.append("\nenum ").append(toCamelCase(getSimpleName())).append(" {");
    return children()
        .filter(Field::isActive)
        .map(c -> c.getCurrentName() + " = " + c.ordinal + ";")
        .collect(Collectors.joining("\n  ", header.append("\n  ").toString(), "\n}\n"));
  }

  @Override
  public String thriftSchema() {
    StringBuilder header = new StringBuilder();
    header.append("namespace * ").append(getCurrentName()).append("\n");
    header.append("\nenum ").append(toCamelCase(getSimpleName())).append("Thrift {");
    return children()
        .filter(Field::isActive)
        .map(c -> c.getCurrentName() + " = " + c.ordinal)
        .collect(Collectors.joining(",\n  ", header.append("\n  ").toString(), ";\n}\n"));
  }

  @Override
  public Optional<String> csvSchema() {
    return Optional.of(children()
                           .filter(Field::isActive)
                           .map(f -> "" + f.ordinal + "," + f.getCurrentName())
                           .collect(Collectors.joining("\n")));
  }

  @Override
  public Schema avroSchema() {
    Schema schema = Schema.createEnum(
        toCamelCase(getSimpleName()) + "Avro",
        docString,
        getCurrentName(),
        children()
            .filter(Field::isActive)
            .map(Node::getCurrentName)
            .collect(Collectors.toList()));
    getOldNames().map(name -> toCamelCase(name) + "Avro").forEach(schema::addAlias);
    if (deprecated) {
      schema.addProp("deprecated", BooleanNode.TRUE);
    }
    return schema;
  }
}
