package com.adgear.anoa.compiler;

import java.io.File;

public interface Generator {

  String generateSchema();

  File generateSchema(File schemaRootDir) throws SchemaGenerationException;

  void generateJava(File schemaRootDir, File javaRootDir) throws CodeGenerationException;
}

