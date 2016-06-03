package com.adgear.anoa.compiler;

import com.adgear.anoa.compiler.javagen.JavaCodeGenerationException;

import java.io.File;

public interface Generator {

  String generateSchema();

  void generateSchema(File schemaRootDir) throws SchemaGenerationException;

  void generateJava(File schemaRootDir, File javaRootDir) throws JavaCodeGenerationException;
}

