package com.adgear.anoa.compiler;

import java.io.File;

public interface AnoaCodeGenerator {

  File getSchemaFile();

  String generateSchema();

  void generateSchema(File schemaRootDir) throws SchemaGenerationException;

  void generateJava(File schemaRootDir, File javaRootDir) throws JavaCodeGenerationException;
}
