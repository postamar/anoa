package com.adgear.anoa.parser.state;

import com.adgear.anoa.parser.AnoaParseException;
import com.adgear.anoa.parser.AnoaValidationException;
import com.adgear.anoa.parser.SchemaGenerator;
import com.adgear.anoa.parser.Statement;
import com.adgear.anoa.parser.Type;

import org.apache.commons.lang3.text.WordUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract public class State<F extends Field<F>, N extends State<F, N>>
    extends Node<N> implements SchemaGenerator, Type {

  final private List<F> children;
  final private String extension;

  protected State(String name, String extension) {
    super(getPackage(name).get(), "");
    this.extension = extension;
    this.children = new ArrayList<>();
  }

  static private Optional<String> getPackage(String qualifiedName) {
    int lastDot = qualifiedName.lastIndexOf('.');
    return (lastDot < 0)
           ? Optional.<String>empty()
           : Optional.of(qualifiedName.substring(0, lastDot));
  }

  protected State(State<F, N> old,
                  Optional<String> newDocString,
                  Optional<Boolean> deprecated,
                  Optional<F> newChild) {
    super(old, Optional.<String>empty(), newDocString, deprecated);
    extension = old.extension;
    children = new ArrayList<>(old.children);
    newChild.ifPresent(children::add);
  }

  protected State(State<F, N> old, Optional<F> child, UnaryOperator<F> fn) {
    super(old, Optional.<String>empty(), Optional.<String>empty(), Optional.<Boolean>empty());
    extension = old.extension;
    children = old.children.stream()
        .map(c -> child.map(target -> target == c).orElse(true) ? fn.apply(c) : c)
        .collect(Collectors.toList());
  }

  protected Optional<String> getPackage() {
    return getPackage(getCurrentName());
  }

  protected String getSimpleName() {
    return getCurrentName().substring(
        getPackage(getCurrentName()).map(String::length).orElse(-1) + 1);
  }

  static String toCamelCase(String lcus) {
    Optional<String> pkgMaybe = getPackage(lcus);
    int idx = pkgMaybe.map(String::length).orElse(-1) + 1;
    String cc = WordUtils.capitalizeFully(lcus.substring(idx), '_').replace("_", "");
    return pkgMaybe.map(pkg -> pkg + "." + cc).orElse(cc);
  }

  private String getFileName(String name, String extension) {
    return (getCurrentName() + ".").replace('.', File.separatorChar) + name + "." + extension;
  }

  @Override
  public String anoaFileName() {
    return getFileName(getSimpleName(), extension);
  }

  @Override
  public String avroFileName() {
    return getFileName(getSimpleName(), "avsc");
  }

  @Override
  public String protoFileName() {
    return getFileName(getSimpleName() + "_protobuf", "proto");
  }

  @Override
  public String thriftFileName() {
    return getFileName(getCurrentName().replace('.', '_'), "thrift");
  }

  @Override
  public String csvFileName() {
    return getFileName(getSimpleName(), "csv");
  }

  protected int getNextOrdinal() {
    return children.size();
  }

  @Override
  public N doName(Statement origin, String newName) {
    throw new AnoaParseException(origin, "cannot rename enums or structs");
  }

  abstract public N doChild(Statement origin, String name, UnaryOperator<F> fn);

  protected F findChild(Statement origin, String name) {
    return children()
        .filter(c -> c.getCurrentName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AnoaParseException(origin,  "cannot find child '" + name + "'"));
  }

  protected Stream<F> children() {
    return children.stream().sorted();
  }

  public void validate() {
    if (!children.stream().anyMatch(c -> !c.removed)) {
      throw new AnoaValidationException(anoaFileName(), "declaration has no fields");
    }
  }

  @Override
  final public String protoType() {
    return getCurrentName() + "." + toCamelCase(getSimpleName());
  }

  @Override
  final public String thriftType() {
    return getCurrentName().replace('.', '_') + "." + toCamelCase(getSimpleName()) + "Thrift";
  }
}
