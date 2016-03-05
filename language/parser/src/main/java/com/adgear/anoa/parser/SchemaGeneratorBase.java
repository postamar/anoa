package com.adgear.anoa.parser;

import org.apache.avro.Protocol;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class SchemaGeneratorBase implements SchemaGenerator {

  final private File file;
  final private List<Path> imports;
  final protected Protocol protocol;

  public SchemaGeneratorBase(ProtocolFactory pg, String suffix, String extension) {
    this.protocol = pg.generate(suffix);
    this.file = pg.getFile(protocol.getNamespace(), extension);
    this.imports = pg.getImportedNamespaces()
        .map(ns -> pg.getFile(ns, extension).toPath())
        .collect(Collectors.toList());
  }

  protected Stream<Path> getImports() {
    return imports.stream().sequential();
  }

  @Override
  public File getFile() {
    return file;
  }
}
