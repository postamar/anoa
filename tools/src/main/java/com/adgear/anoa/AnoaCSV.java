package com.adgear.anoa;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

public class AnoaCsv {

  static final CsvMapper CSV_MAPPER = new CsvMapper();

  static public CsvParser from(Reader reader, CsvSchema csvSchema) throws IOException {
    return with(CSV_MAPPER.getFactory().createParser(reader), csvSchema);
  }

  static public CsvParser from(File file, CsvSchema csvSchema) throws IOException {
    return with(CSV_MAPPER.getFactory().createParser(file), csvSchema);
  }

  static public CsvParser from(InputStream inputStream, CsvSchema csvSchema) throws IOException {
    return with(CSV_MAPPER.getFactory().createParser(inputStream), csvSchema);
  }

  static public CsvParser from(byte[] bytes, CsvSchema csvSchema) throws IOException {
    return with(CSV_MAPPER.getFactory().createParser(bytes), csvSchema);
  }

  static public CsvGenerator to(OutputStream outputStream, CsvSchema csvSchema)
      throws IOException {
    return with(CSV_MAPPER.getFactory().createGenerator(outputStream, JsonEncoding.UTF8),
                csvSchema);
  }

  static public CsvGenerator to(Writer writer, CsvSchema csvSchema)
      throws IOException {
    return with(CSV_MAPPER.getFactory().createGenerator(writer), csvSchema);
  }

  static public CsvGenerator to(File file, CsvSchema csvSchema) throws IOException {
    return with(CSV_MAPPER.getFactory().createGenerator(file, JsonEncoding.UTF8), csvSchema);
  }

  static protected CsvGenerator with(CsvGenerator generator, CsvSchema schema) throws IOException {
    generator.setSchema(schema);
    return generator;
  }

  static protected CsvParser with(CsvParser parser, CsvSchema schema) throws IOException {
    parser.setSchema(schema);
    return parser;
  }
}
