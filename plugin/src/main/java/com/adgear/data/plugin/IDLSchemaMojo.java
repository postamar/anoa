/**
 * Modified source code from the Apache Avro project, version 1.7.4 (http://avro.apache.org/)
 *
 *
 * LICENSE: Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 *
 * NOTICE: Apache Avro Copyright 2010 The Apache Software Foundation
 *
 * This product includes software developed at The Apache Software Foundation
 * (http://www.apache.org/).
 *
 * C JSON parsing provided by Jansson and written by Petri Lehtinen. The original software is
 * available from http://www.digip.org/jansson/.
 */

package com.adgear.data.plugin;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mojo.AbstractAvroMojo;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generate Java classes and interfaces from AvroIDL files (.avdl), modified for improved
 * annotation and dependency handling.
 *
 * @extendsPlugin avro-maven-plugin
 * @extendsGoal schema
 * @goal idl-schema
 * @phase generate-sources
 * @threadSafe
 */
public class IDLSchemaMojo extends AbstractAvroMojo {

  private static final String encoding = "UTF8";
  static public Pattern
      docstringPattern =
      Pattern.compile("(?:\\A|\\n)\\s*requires:\\s*((?:\\n\\s*\\S+\\s*)+)\\s*(?:\\n|\\Z)",
                      Pattern.MULTILINE);
  // the following are only ever mutated by the 'execute' and 'doCompile' methods.
  protected Map<String, IDLObject> mapSource;
  protected Map<String, IDLObject> mapTest;
  /**
   * A set of Ant-like inclusion patterns used to select files from the source directory for
   * processing. By default, the pattern <code>**&#47;*.avdl</code> is used to select IDL files.
   *
   * @parameter
   */
  private String[] includes = new String[]{"**/*.avdl", "**/*.avsc"};
  /**
   * A set of Ant-like inclusion patterns used to select files from the source directory for
   * processing. By default, the pattern <code>**&#47;*.avdl</code> is used to select IDL files.
   *
   * @parameter
   */
  private String[] testIncludes = new String[]{"**/*.avdl", "**/*.avsc"};
  private File superOutputDirectory;
  private File superTestOutputDirectory;


  public IDLSchemaMojo() {
    super();
    templateDirectory = "/com/adgear/avro/plugin/templates/";
  }

  @Override
  protected String[] getIncludes() {
    return includes;
  }

  @Override
  protected String[] getTestIncludes() {
    return testIncludes;
  }

  /**
   * Parses IDL source code for Avro Protocol.
   *
   * @param source IDL source code
   * @return Wrapping protocol object
   */

  protected Protocol parseProtocol(String source) throws MojoExecutionException {
    ByteArrayInputStream input;
    try {
      input = new ByteArrayInputStream(source.getBytes(encoding));
    } catch (UnsupportedEncodingException e) {
      throw new MojoExecutionException("Unsupported encoding: " + encoding, e);
    }
    Idl idl = new Idl(input, encoding);
    try {
      return idl.CompilationUnit();
    } catch (ParseException e) {
      throw new MojoExecutionException("Error compiling Avro wrapping protocol :\n" + source, e);
    }
  }

  protected File getDir(File outputDir, Schema s) {
    final String namespace = s.getNamespace();
    if (namespace != null && !namespace.isEmpty()) {
      String path = namespace.replace('.', File.separatorChar) + File.separatorChar;
      return new File(outputDir, path);
    } else {
      return outputDir;
    }
  }

  protected String getIncludePath(File file, File includeFile) {
    return file.toPath().normalize().relativize(includeFile.toPath().normalize()).toString();
  }

  protected File getThriftFile(File outputDir, Schema s) {
    String baseName = (s.getNamespace() + "." + Compiler.mangle(s.getName())).replace('.', '_');
    return new File(outputDir, baseName + ".thrift");
  }

  /**
   * Compile a schema in corresponding .java and .avsc files in the outputDir. Also compiles .csv
   * file for enumerations
   *
   * @param s Schema to be compiled
   */
  protected void compileSchema(Schema s, File outputDir) throws MojoExecutionException {
    /* compile .java output file */
    Compiler compiler = new Compiler(s);
    compiler.setStringType(GenericData.StringType.valueOf(stringType));
    compiler.setTemplateDir(templateDirectory);
    compiler.setFieldVisibility(getFieldVisibility());
    compiler.setCreateSetters(createSetters);
    try {
      compiler.compileToDestination(null, outputDir);
    } catch (IOException e) {
      throw new MojoExecutionException("Error compiling Avro schema " + s.getFullName(), e);
    }
    final String name = Compiler.mangle(s.getName());

    /* Write the schema */
    File schemaFile = new File(getDir(outputDir, s), name + ".avsc");
    try {
      FileUtils.write(schemaFile, s.toString(true), encoding);
    } catch (IOException e) {
      throw new MojoExecutionException("Could not write schema in: " + schemaFile.getPath(), e);
    }

    /* export other */
    switch (s.getType()) {
      case ENUM:
        try {
          FileUtils.write(new File(getDir(outputDir, s), name + ".csv"), toCSV(s), encoding);
        } catch (IOException e) {
          getLog().warn("Could not write " + name + " csv enumeration: " + e.getMessage());
        }
        try {
          FileUtils.write(getThriftFile(outputDir, s), ExportThrift.get().exportEnum(s), encoding);
        } catch (IOException e) {
          getLog().warn("Could not write " + name + " thrift enum schema: " + e.getMessage());
        }
        break;
      case RECORD:
        List<Schema>
            dependencies =
            ExportBase.collectDependencies(s, new ArrayList<Schema>(), false);
        Map<String, String> names = new HashMap<String, String>();
        Map<String, String> includePaths = new HashMap<String, String>();
        try {
          for (Schema dependentSchema : dependencies) {
            if (!dependentSchema.getFullName().equals(s.getFullName())) {
              File includeFile = getThriftFile(outputDir, dependentSchema);
              if (!includeFile.exists()) {
                throw new IOException("Missing include file " + includeFile.getName());
              }
              names.put(dependentSchema.getFullName(),
                        includeFile.getName().replaceAll("\\.thrift$", ""));
              includePaths.put(dependentSchema.getFullName(), includeFile.getName());
            }
          }
          FileUtils.write(getThriftFile(outputDir, s),
                          ExportThrift.get().exportRecord(s, includePaths, names), encoding);
        } catch (IOException e) {
          getLog().warn("Could not write " + name + " thrift struct schema: " + e.getMessage());
        }
    }
  }

  /*
   * The following methods depend on the shared state kept in the following properties.
   * This contrivance is not easily avoidable since this class inherits from AbstractAvroMojo.
   *
   */
  private String toCSV(Schema s) {
    StringBuilder sb = new StringBuilder();
    for (String symbol : s.getEnumSymbols()) {
      sb.append(s.getEnumOrdinal(symbol)).append(',').append(symbol).append('\n');
    }
    return sb.toString();
  }

  /**
   * Recursive function which collects all required dependencies
   *
   * @param o Root IDL object
   * @return Set of names of dependent IDL objects for 'name'
   */
  private Set<IDLObject> collect(Map<String, IDLObject> map, IDLObject o) {
    Set<IDLObject> set = new HashSet<IDLObject>();
    set.add(o);
    if (o.getRequired() != null) {
      for (String required : o.getRequired()) {
        set.addAll(collect(map, map.get(required)));
      }
    }
    return set;
  }

  /**
   * Recursive function which traverses the dependency graph adjacency matrix and computes the
   * length of the longest dependency chain. A exception is thrown when the graph is found to be
   * inconsistent, either through a circularity or an orphan.
   *
   * @param path Current dependency chain
   * @return Length of the longest subchain starting at the end of the current chain
   */
  private Integer getDependencyRank(Map<String, IDLObject> map, List<String> path)
      throws MojoExecutionException {
    String name = path.get(path.size() - 1);
    int maxRank = 0;

    if (!map.containsKey(name)) {
      throw new MojoExecutionException(
          "Unresolved dependency: " + Arrays.toString(path.toArray(new String[path.size()])));
    }

    Set<String> dependencies = map.get(name).getRequired();
    if (dependencies == null) {
      return 0;
    }
    for (String required : dependencies) {
      ArrayList<String> requiredPath = new ArrayList<String>(path);
      requiredPath.add(required);
      if (path.contains(required)) {
        throw new MojoExecutionException(
            "Circular dependency: "
            + Arrays.toString(requiredPath.toArray(new String[path.size()])));
      }
      int rank = 1 + getDependencyRank(map, requiredPath);
      maxRank = (rank > maxRank) ? rank : maxRank;
    }
    return maxRank;
  }

  /**
   * @param map       Dependency graph map
   * @param outputDir Directory in which to put the compiled files
   */
  void compileAll(Map<String, IDLObject> map, File outputDir) throws MojoExecutionException {

    for (IDLObject o : map.values()) {
      if (o.getRequired() == null) {
        Schema s = new Schema.Parser().parse(o.getCode());
        if (s == null) {
          throw new MojoExecutionException("Error parsing Avro schema " + o.getName());
        }
        String ns = o.getName().replaceFirst("\\.[^\\.]+$", "");
        String nsprop = s.getProp("namespace");
        if (nsprop == null) {
          String code = s.toString().replaceFirst("\\{", "{\"namespace\" : \"" + ns + "\",");
          s = new Schema.Parser().parse(code);
        } else {
          if (!nsprop.equals(ns)) {
            throw new MojoExecutionException(
                "Avro schema " + o.getName() + " declares as namespace " + nsprop
                + " which does not correspond to its location.");
          }
        }
        compileSchema(s, outputDir);
      }
    }

    Map<String, Integer> rankMap = new HashMap<String, Integer>();
    for (Map.Entry<String, IDLObject> entry : map.entrySet()) {
      if (entry.getValue().getRequired() != null) {
        List<String> path = new ArrayList<String>();
        path.add(entry.getKey());
        rankMap.put(entry.getKey(), getDependencyRank(map, path));
      }
    }

    ArrayList<IDLObject> list = new ArrayList<IDLObject>();
    for (int oldSize = -1, rank = 0; list.size() > oldSize; ++rank) {
      oldSize = list.size();
      for (Map.Entry<String, Integer> entry : rankMap.entrySet()) {
        if (entry.getValue() == rank) {
          list.add(map.get(entry.getKey()));
        }
      }
    }

    for (IDLObject o : list) {
      Set<IDLObject> set = collect(map, o);
      StringBuilder sb = new StringBuilder();
      sb.append("protocol ").append(o.getName().replaceAll("\\.", "_")).append("__protocol {");
      for (IDLObject obj : list) {
        if (set.contains(obj)) {
          sb.append('\n').append(obj.getCode()).append('\n');
        }
      }
      sb.append("}\n");
      Protocol p = parseProtocol(sb.toString());
      for (Schema s : p.getTypes()) {
        compileSchema(s, outputDir);
      }
    }
  }

  @Override
  public void execute() throws MojoExecutionException {
    superOutputDirectory = null;
    superTestOutputDirectory = null;
    mapSource = new HashMap<String, IDLObject>();
    mapTest = new HashMap<String, IDLObject>();
    super.execute();
    compileAll(mapSource, superOutputDirectory);
    compileAll(mapTest, superTestOutputDirectory);
  }

  /**
   * Inspect the source for dependency requirement statements
   *
   * @param source IDL source code
   * @return Collection of dependency names
   */
  protected Collection<String> collectDependencies(String source) {
    HashSet<String> found = new HashSet<String>();
    Matcher start = Pattern.compile("/\\*\\*", Pattern.MULTILINE).matcher(source);
    Matcher end = Pattern.compile("\\*/", Pattern.MULTILINE).matcher(source);
    if (start.find() && end.find()) {
      String doc = source.substring(start.start() + 3, end.end() - 2);
      Matcher dep = docstringPattern.matcher(doc);
      if (dep.find()) {
        Matcher
            required =
            Pattern.compile("([a-zA-Z0-9$_\\.]+)", Pattern.MULTILINE).matcher(dep.group(1));
        while (required.find()) {
          String name = required.group(1);
          found.add(name);
          getLog().debug("Registering dependency '" + name + "'");
        }
      }
    }
    return found;
  }

  /**
   * Performs transformations on IDL source code and stores it for later dependency resolution.
   *
   * @param filename        Name of the source file
   * @param sourceDirectory Source directory
   * @param outputDirectory Java output directory
   */
  @Override
  protected void doCompile(String filename, File sourceDirectory, File outputDirectory)
      throws IOException {

    if (superOutputDirectory == null) {
      superOutputDirectory = outputDirectory;
    } else if (superOutputDirectory != outputDirectory && superTestOutputDirectory == null) {
      superTestOutputDirectory = outputDirectory;
    }

    // read file contents
    File sourceFile = new File(sourceDirectory, filename);
    String source = FileUtils.readFileToString(sourceFile, "UTF8");
    String sourceName = filename.replaceAll("\\..*$", "").replaceAll("/", ".");

    IDLObject o = (filename.matches("^.*\\.avdl$"))
                  ? generateIdlObject(sourceName, source)
                  : new IDLObject(sourceName, source, null);

    if (outputDirectory == superOutputDirectory) {
      mapSource.put(o.getName(), o);
    } else if (outputDirectory == superTestOutputDirectory) {
      mapTest.put(o.getName(), o);
    }
  }

  /**
   * Factory method for generating instances of IDLObject
   *
   * @param name   File name
   * @param source IDL source code
   */
  protected IDLObject generateIdlObject(String name, String source) {
    getLog().debug("Preprocessing file " + name);
    String namespaceAnnotationRegex = "(?<!\\w)@namespace\\(\"[a-zA-Z0-9$_\\.]+\"\\)";
    Matcher m = Pattern.compile(namespaceAnnotationRegex, Pattern.MULTILINE).matcher(source);
    while (m.find()) {
      getLog().warn("Ignoring @namespace annotation '" + m.group(1) + "'");
      source = m.replaceFirst(" ");
    }
    String namespace = name.replaceFirst("\\.[^\\.]+$", "");
    return new IDLObject(name, "@namespace(\"" + namespace + "\")\n" + source.trim(),
                         collectDependencies(source));
  }

  protected static class IDLObject {

    private String name;
    private String code;
    private Set<String> required;

    public IDLObject(String name, String code, Collection<String> required) {
      this.name = name;
      this.code = code;
      if (required != null) {
        this.required = new HashSet<String>(required);
      }
    }

    public String getName() {
      return name;
    }

    public String getCode() {
      return code;
    }

    public Set<String> getRequired() {
      return required;
    }
  }
}