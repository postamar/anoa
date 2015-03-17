package com.adgear.anoa.write;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.util.Optional;

public class CsvConsumers extends JacksonConsumersBase<
    ObjectMapper,
    CsvFactory,
    CsvSchema,
    CsvGenerator> {

  public CsvConsumers(CsvSchema csvSchema) {
    super(new CsvMapper(new CsvFactory()), Optional.of(csvSchema));
  }

  static public CsvConsumers csv() {
    return new CsvConsumers(CsvSchema.emptySchema());
  }

  static public CsvConsumers tsv() {
    return new CsvConsumers(CsvSchema.builder()
                              .setColumnSeparator('\t')
                              .build());
  }

  static public CsvConsumers csvWithHeader() {
    return new CsvConsumers(CsvSchema.builder()
                              .setUseHeader(true)
                              .build());
  }

  static public CsvConsumers tsvWithHeader() {
    return new CsvConsumers(CsvSchema.builder()
                              .setUseHeader(true)
                              .setColumnSeparator('\t')
                              .build());
  }
}
