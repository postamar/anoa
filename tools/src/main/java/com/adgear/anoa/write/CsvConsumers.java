package com.adgear.anoa.write;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.util.Optional;

/**
 * Utility class for writing Jackson records as CSV documents.
 */
public class CsvConsumers extends JacksonConsumersBase<
    ObjectMapper,
    CsvFactory,
    CsvSchema,
    CsvGenerator> {

  /**
   * @param csvSchema format schema to use
   */
  public CsvConsumers(CsvSchema csvSchema) {
    super(new CsvMapper(new CsvFactory()), Optional.of(csvSchema));
  }

  /**
   * @return a CsvConsumers instance for writing CSV documents with no header
   */
  static public CsvConsumers csv() {
    return new CsvConsumers(CsvSchema.emptySchema());
  }

  /**
   * @return a CsvConsumers instance for writing TSV documents with no header
   */
  static public CsvConsumers tsv() {
    return new CsvConsumers(CsvSchema.builder()
                              .setColumnSeparator('\t')
                              .build());
  }

  /**
   * @return a CsvConsumers instance for writing CSV documents with a header row
   */
  static public CsvConsumers csvWithHeader() {
    return new CsvConsumers(CsvSchema.builder()
                              .setUseHeader(true)
                              .build());
  }

  /**
   * @return a CsvConsumers instance for writing TSV documents with a header row
   */
  static public CsvConsumers tsvWithHeader() {
    return new CsvConsumers(CsvSchema.builder()
                              .setUseHeader(true)
                              .setColumnSeparator('\t')
                              .build());
  }
}
