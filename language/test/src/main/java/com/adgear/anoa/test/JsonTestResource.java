package com.adgear.anoa.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class JsonTestResource {

  final String resourcePath;
  final List<String> jsonStrings;
  final List<byte[]> jsonBytes;

  JsonTestResource(String resourcePath) {
    this.resourcePath = resourcePath;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream()))) {
      jsonStrings = reader.lines()
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    jsonBytes = jsonStrings.stream().sequential()
        .map(String::getBytes)
        .collect(Collectors.toList());
  }

  Stream<String> strings() {
    return jsonStrings.stream().sequential();
  }

  Stream<byte[]> bytes() {
    return jsonBytes.stream().sequential();
  }

  InputStream inputStream() {
    return getClass().getResourceAsStream(resourcePath);
  }
}
