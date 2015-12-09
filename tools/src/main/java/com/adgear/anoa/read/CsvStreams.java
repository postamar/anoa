package com.adgear.anoa.read;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.util.Optional;


/**
 * Utility class for streaming Jackson records from CSV serializations.
 */
public class CsvStreams extends JacksonStreamsBase<
    ObjectMapper,
    CsvFactory,
    CsvSchema,
    CsvParser> {

  /**
   * @param csvSchema format schema to use
   */
  public CsvStreams(CsvSchema csvSchema) {
    super(new CsvMapper(new CsvFactory()), Optional.of(csvSchema));
  }

  /**
   * @return a CsvStreams instance for parsing CSV documents with no header
   */
  static public CsvStreams csv() {
    return new CsvStreams(CsvSchema.emptySchema());
  }

  /**
   * @return a CsvStreams instance for parsing TSV documents with no header
   */
  static public CsvStreams tsv() {
    return new CsvStreams(CsvSchema.builder()
                              .disableQuoteChar()
                              .setColumnSeparator('\t')
                              .build());
  }

  /**
   * @return a CsvStreams instance for parsing CSV documents with header row
   */
  static public CsvStreams csvWithHeader() {
    return new CsvStreams(CsvSchema.builder()
                              .setUseHeader(true)
                              .build());
  }

  /**
   * @return a CsvStreams instance for parsing CSV documents with header row
   */
  static public CsvStreams tsvWithHeader() {
    return new CsvStreams(CsvSchema.builder()
                              .setUseHeader(true)
                              .disableQuoteChar()
                              .setColumnSeparator('\t')
                              .build());
  }
}
