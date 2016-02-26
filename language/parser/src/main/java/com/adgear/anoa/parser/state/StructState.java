package com.adgear.anoa.parser.state;

import com.adgear.anoa.parser.AnoaParseException;
import com.adgear.anoa.parser.Statement;
import com.adgear.anoa.parser.type.FieldType;

import org.apache.avro.Schema;
import org.codehaus.jackson.node.BooleanNode;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final public class StructState extends State<StructField, StructState> {

  public StructState(String name) {
    super(name, "struct");
  }

  private StructState(StructState old,
                      Optional<String> newDocString,
                      Optional<Boolean> deprecated,
                      Optional<StructField> newChild) {
    super(old, newDocString, deprecated, newChild);
  }

  private StructState(StructState old, Optional<StructField> field, UnaryOperator<StructField> fn) {
    super(old, field, fn);
  }

  @Override
  public StructState doDoc(Statement origin, String newDoc) {
    validateNewDocString(origin, newDoc);
    return new StructState(this,
                          Optional.of(newDoc),
                          Optional.<Boolean>empty(),
                          Optional.<StructField>empty());
  }

  @Override
  public StructState doDeprecate(Statement origin) {
    validateDeprecation(origin, true);
    return new StructState(this,
                          Optional.<String>empty(),
                          Optional.of(true),
                          Optional.<StructField>empty());
  }

  @Override
  public StructState doRemove(Statement origin) {
    throw new AnoaParseException(origin, "cannot remove struct declaration");
  }

  @Override
  public StructState doRestore(Statement origin) {
    validateDeprecation(origin, false);
    return new StructState(this,
                          Optional.<String>empty(),
                          Optional.of(false),
                          Optional.<StructField>empty());
  }

  @Override
  public StructState doChild(Statement origin, String name, UnaryOperator<StructField> fn) {
    return new StructState(this, Optional.of(findChild(origin, name)), fn);
  }

  public StructState addStructField(String name,
                                   String docString,
                                   FieldType fieldType) {
    StructField field = new StructField(name, docString, getNextOrdinal(), fieldType);
    return new StructState(this,
                          Optional.<String>empty(),
                          Optional.<Boolean>empty(),
                          Optional.of(field));
  }

  @Override
  public String protoSchema() {
    String joinedRemovedOrdinals = children()
        .filter(f -> f.removed)
        .map(f -> String.format("%d", f.ordinal + 1))
        .collect(Collectors.joining(", "));
    String joinedRemovedTags = Stream.concat(
        children().filter(f -> f.removed).map(Node::getCurrentName),
        children().flatMap(StructField::getOldNames))
        .sorted()
        .collect(Collectors.joining("\", \""));

    StringBuilder header = new StringBuilder();
    children().forEach(field -> field.fieldType.getDependency().ifPresent(
        dep -> header.append("import \"").append(dep.protoFileName()).append("\";\n")));
    header.append("\npackage ").append(getCurrentName()).append(";\n");
    header.append("\nmessage ").append(toCamelCase(getSimpleName())).append(" {");
    if (!joinedRemovedOrdinals.isEmpty()) {
      header.append("\n  reserved ").append(joinedRemovedOrdinals).append(";");
    }
    if (!joinedRemovedTags.isEmpty()) {
      header.append("\n  reserved \"").append(joinedRemovedTags).append("\";");
    }
    return children()
        .filter(Field::isActive)
        .map(StructField::protoStatement)
        .collect(Collectors.joining("\n  ", header.append("\n  ").toString(), "\n}\n"));
  }

  @Override
  public String thriftSchema() {
    StringBuilder header = new StringBuilder();
    Optional<Path> base = Optional.ofNullable(new File(thriftFileName()).getParentFile())
        .map(File::toPath);
    children().forEach(field -> field.fieldType.getDependency()
        .map(dep -> new File(dep.thriftFileName()).toPath())
        .map(path -> base.map(p -> p.relativize(path)).orElse(path))
        .ifPresent(path -> header.append("include \"").append(path).append("\"\n")));
    header.append("\nnamespace * ").append(getCurrentName()).append("\n");
    header.append("\nstruct ").append(toCamelCase(getSimpleName())).append("Thrift {");
    return children()
        .filter(Field::isActive)
        .map(StructField::thriftStatement)
        .collect(Collectors.joining("\n  ", header.append("\n  ").toString(), "\n}\n"));
  }

  @Override
  public Optional<String> csvSchema() {
    return Optional.empty();
  }

  @Override
  public Schema avroSchema() {
    Schema schema = Schema.createRecord(toCamelCase(getSimpleName()) + "Avro",
                                        docString,
                                        getCurrentName(),
                                        false);
    schema.setFields(children()
                         .filter(f -> !f.removed)
                         .map(StructField::getAvroField)
                         .collect(Collectors.toList()));
    getOldNames().map(name -> toCamelCase(name) + "Avro").forEach(schema::addAlias);
    if (deprecated) {
      schema.addProp("deprecated", BooleanNode.TRUE);
    }
    return schema;
  }
}
