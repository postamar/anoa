/**
 *
 * Modified source code from the Apache Avro project, version 1.7.4 (http://avro.apache.org/)
 *
 *
 * LICENSE:
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *
 * NOTICE:
 *   Apache Avro
 *   Copyright 2010 The Apache Software Foundation
 *
 *   This product includes software developed at
 *   The Apache Software Foundation (http://www.apache.org/).
 *
 *   C JSON parsing provided by Jansson and
 *   written by Petri Lehtinen. The original software is
 *   available from http://www.digip.org/jansson/.
 */

package com.adgear.data.plugin;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A modified version of Avro's {@link org.apache.avro.compiler.specific.SpecificCompiler},
 * with improved annotation and dependency handling.
 *
 */
public class Compiler extends SpecificCompiler {

  /* List of Java reserved words from
   * http://java.sun.com/docs/books/jls/third_edition/html/lexical.html. */
  private static final Set<String> RESERVED_WORDS;
  /* Reserved words for accessor/mutator methods */
  private static final Set<String> ACCESSOR_MUTATOR_RESERVED_WORDS;
  /* Reserved words for error types */
  private static final Set<String> ERROR_RESERVED_WORDS;
  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);

  static {
    RESERVED_WORDS = new HashSet<String>(
        Arrays.asList(new String[]{
            "abstract", "assert", "boolean", "break", "byte", "case", "catch",
            "char", "class", "const", "continue", "default", "do", "double",
            "else", "enum", "extends", "false", "final", "finally", "float",
            "for", "goto", "if", "implements", "import", "instanceof", "int",
            "interface", "long", "native", "new", "null", "package", "private",
            "protected", "public", "return", "short", "static", "strictfp",
            "super", "switch", "synchronized", "this", "throw", "throws",
            "transient", "true", "try", "void", "volatile", "while"
        }));
    ACCESSOR_MUTATOR_RESERVED_WORDS =
        new HashSet<String>(Arrays.asList(new String[]{
            "class", "schema", "classSchema"
        }));
    // Add reserved words to accessor/mutator reserved words
    ACCESSOR_MUTATOR_RESERVED_WORDS.addAll(RESERVED_WORDS);
    ERROR_RESERVED_WORDS = new HashSet<String>(
        Arrays.asList(new String[]{"message", "cause"}));
    // Add accessor/mutator reserved words to error reserved words
    ERROR_RESERVED_WORDS.addAll(ACCESSOR_MUTATOR_RESERVED_WORDS);
  }

  public Compiler(Schema s) {
    super(s);
  }

  static public String generateAliasMethodName(Schema schema, String alias, String prefix) {
    // Check for the special case in which the schema defines two fields whose
    // names are identical except for the case of the first character:
    char firstChar = alias.charAt(0);
    String conflictingFieldName = (Character.isLowerCase(firstChar)
                                   ? Character.toUpperCase(firstChar)
                                   : Character.toLowerCase(firstChar)) +
                                  (alias.length() > 1 ? alias.substring(1) : "");
    boolean fieldNameConflict = schema.getField(conflictingFieldName) != null;
    StringBuilder methodBuilder = new StringBuilder(prefix);
    String fieldName = mangle(alias,
                              schema.isError()
                              ? ERROR_RESERVED_WORDS
                              : ACCESSOR_MUTATOR_RESERVED_WORDS,
                              true);

    boolean nextCharToUpper = true;
    for (int ii = 0; ii < fieldName.length(); ii++) {
      if (fieldName.charAt(ii) == '_') {
        nextCharToUpper = true;
      } else if (nextCharToUpper) {
        methodBuilder.append(Character.toUpperCase(fieldName.charAt(ii)));
        nextCharToUpper = false;
      } else {
        methodBuilder.append(fieldName.charAt(ii));
      }
    }
    // If there is a field name conflict append $0 or $1
    if (fieldNameConflict) {
      if (methodBuilder.charAt(methodBuilder.length() - 1) != '$') {
        methodBuilder.append('$');
      }
      methodBuilder.append(Character.isLowerCase(firstChar) ? '0' : '1');
    }
    return methodBuilder.toString();
  }

  public String[] javaAnnotations(JsonProperties props, String doc) {
    ArrayList<String> a = new ArrayList<String>(Arrays.asList(super.javaAnnotations(props)));
    String regex = "(?:\\A|\\n)\\s*java annotations:\\s*((?:\\n\\s*\\S.*)+)\\s*(?:\\n|\\Z)";
    Matcher m = Pattern.compile(regex, Pattern.MULTILINE).matcher(doc);
    if (m.find()) {
      for (String annotation : Arrays.asList(m.group(1).trim().split("\\n"))) {
        a.add(annotation.trim());
      }
    }
    return a.toArray(new String[a.size()]);
  }

  public String[] javaImports(String doc) {
    ArrayList<String> a = new ArrayList<String>();
    String regex = "(?:\\A|\\n)\\s*java imports:\\s*((?:\\n\\s*\\S+\\s*)+)\\s*(?:\\n|\\Z)";
    Matcher m = Pattern.compile(regex, Pattern.MULTILINE).matcher(doc);
    if (m.find()) {
      for (String imp : Arrays.asList(m.group(1).trim().split("\\n"))) {
        a.add(imp.trim());
      }
    }
    return a.toArray(new String[a.size()]);
  }

  public String getStringType() {
    return javaType(STRING_SCHEMA);
  }
}
