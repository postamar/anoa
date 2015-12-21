package com.adgear.anoa;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TestResource {

  protected final String resourcePath;
  protected final List<String> jsonStrings;
  protected final List<byte[]> jsonBytes;

  protected TestResource(String resourcePath) {
    this.resourcePath = resourcePath;
    jsonStrings = new BufferedReader(new InputStreamReader(
        getClass().getResourceAsStream(resourcePath)))
        .lines()
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    jsonBytes = jsonStrings.stream().sequential()
        .map(String::getBytes)
        .collect(Collectors.toList());

  }

  public Stream<String> jsonStrings() {
    return jsonStrings.stream().sequential();
  }

  public Stream<byte[]> jsonBytes() {
    return jsonBytes.stream().sequential();
  }

  public InputStream jsonStream() {
    return getClass().getResourceAsStream(resourcePath);
  }
}
