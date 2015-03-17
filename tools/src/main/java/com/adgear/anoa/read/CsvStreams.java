package com.adgear.anoa.read;

import checkers.nullness.quals.NonNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.util.Optional;

public class CsvStreams extends JacksonStreamsBase<
    ObjectMapper,
    CsvFactory,
    CsvSchema,
    CsvParser> {

  public CsvStreams(@NonNull CsvSchema csvSchema) {
    super(new CsvMapper(new CsvFactory()), Optional.of(csvSchema));
  }

  static public CsvStreams csv() {
    return new CsvStreams(CsvSchema.emptySchema());
  }

  static public CsvStreams tsv() {
    return new CsvStreams(CsvSchema.builder()
                              .setColumnSeparator('\t')
                              .build());
  }

  static public CsvStreams csvWithHeader() {
    return new CsvStreams(CsvSchema.builder()
                              .setUseHeader(true)
                              .build());
  }

  static public CsvStreams tsvWithHeader() {
    return new CsvStreams(CsvSchema.builder()
                              .setUseHeader(true)
                              .setColumnSeparator('\t')
                              .build());
  }
}
