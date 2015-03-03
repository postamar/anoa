package com.adgear.anoa.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

public class CsvObjects extends JacksonObjects<
    ObjectMapper,
    CsvFactory,
    CsvSchema,
    CsvParser,
    CsvGenerator> {

  public CsvObjects(CsvSchema csvSchema) {
    super(new CsvMapper(new CsvFactory()), csvSchema);
  }

  static public CsvObjects csv() {
    return new CsvObjects(CsvSchema.emptySchema());
  }

  static public CsvObjects tsv() {
    return new CsvObjects(CsvSchema.builder()
                                  .setColumnSeparator('\t')
                                  .build());
  }

  static public CsvObjects csvWithHeader() {
    return new CsvObjects(CsvSchema.builder()
                                  .setUseHeader(true)
                                  .build());
  }

  static public CsvObjects tsvWithHeader() {
    return new CsvObjects(CsvSchema.builder()
                                  .setUseHeader(true)
                                  .setColumnSeparator('\t')
                                  .build());
  }
}