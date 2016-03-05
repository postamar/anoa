package com.adgear.anoa.plugin;

import com.adgear.anoa.parser2.SchemaGenerator;

import org.apache.avro.Schema;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ThriftPatcher implements Runnable {

  final File javaSource;
  final private Map<String, Pattern> map;

  public ThriftPatcher(File javaSource, List<Schema.Field> fields) {
    this.javaSource = javaSource;
    this.map = new LinkedHashMap<>();
    for (Schema.Field field : fields) {
      String name = field.name();
      String value = field.defaultValue().toString();
      String prefix = "^(\\s+this\\." + name + "\\s+=\\s+)";
      String suffix = "\\s*;\\s*$";
      String regex = prefix + value.replace("+", "\\+") + suffix;
      String newValue = "ByteBuffer.wrap(java.util.Base64.getDecoder().decode(" + value + "));";
      map.put(newValue, Pattern.compile(regex));
    }

  }

  static public Optional<ThriftPatcher> check(SchemaGenerator sg, File javaSourceDir) {
    Schema schema = sg.avroSchema();
    if (schema.getType() == Schema.Type.RECORD) {
      List<Schema.Field> fields = sg.avroSchema().getFields().stream()
              .filter(field -> field.schema().getType().equals(Schema.Type.BYTES))
              .filter(field -> !field.defaultValue().getTextValue().isEmpty())
              .collect(Collectors.toList());
      if (!fields.isEmpty()) {
        File packageDir = new File(javaSourceDir, sg.thriftFileName()).getParentFile();
        assert packageDir.exists();
        assert packageDir.isDirectory();
        File[] sourceFiles = packageDir.listFiles();
        assert sourceFiles != null;
        return Stream.of(sourceFiles)
            .filter(f -> f.getName().endsWith("Thrift.java"))
            .findFirst()
            .map(sourceFile -> new ThriftPatcher(sourceFile, fields));
      }
    }
    return Optional.empty();
  }

  @Override
  public void run() {
    assert javaSource.canRead();
    assert javaSource.canWrite();
    final List<String> sourceLines;
    try (BufferedReader reader = new BufferedReader(new FileReader(javaSource))) {
      sourceLines = reader.lines().collect(Collectors.toList());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    for (Map.Entry<String, Pattern> entry : map.entrySet()) {
      for (int i = 0; i < sourceLines.size(); i++) {
        Matcher matcher = entry.getValue().matcher(sourceLines.get(i));
        if (matcher.matches()) {
          sourceLines.set(i, matcher.group(1) + entry.getKey());
        }
      }
    }
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(javaSource))) {
      for (String line : sourceLines) {
        writer.write(line);
        writer.newLine();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
