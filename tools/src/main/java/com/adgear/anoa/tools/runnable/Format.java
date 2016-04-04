package com.adgear.anoa.tools.runnable;

/**
 * Supported input/output serialization formats.
 */
public enum Format {
  /**
   * Avro batch file
   */
  AVRO(FormatCategory.AVRO),

  /**
   * Avro binary encoding (not batch file, just binary serializations)
   */
  AVRO_BINARY(FormatCategory.AVRO),

  /**
   * Avro JSON encoding (not batch file, just JSON serializations)
   */
  AVRO_JSON(FormatCategory.AVRO),

  /**
   * Thrift compact encoding
   */
  THRIFT_COMPACT(FormatCategory.THRIFT),

  /**
   * Thrift binary encoding
   */
  THRIFT_BINARY(FormatCategory.THRIFT),

  /**
   * Thrift JSON encoding
   */
  THRIFT_JSON(FormatCategory.THRIFT),

  /**
   * Protobuf encoding
   */
  PROTOBUF(FormatCategory.PROTOBUF),

  /**
   * JDBC result set (input only)
   */
  JDBC(FormatCategory.DB),

  /**
   * JSON objects, newline separated
   */
  JSON(FormatCategory.JACKSON, false),

  /**
   * CSV with column header row, separated by ',', escaping supported, no trimming.
   */
  CSV(FormatCategory.JACKSON),

  /**
   * same as {@link #CSV}, but without column header row.
   */
  CSV_NO_HEADER(FormatCategory.JACKSON),

  /**
   * TSV with column header row, separated by ',', escaping supported, no trimming.
   */
  TSV(FormatCategory.JACKSON),

  /**
   * same as {@link #TSV}, but without column header row.
   */
  TSV_NO_HEADER(FormatCategory.JACKSON),

  /**
   * CBOR encoding
   */
  CBOR(FormatCategory.JACKSON, false),

  /**
   * SMILE encoding
   */
  SMILE(FormatCategory.JACKSON, false);


  final public FormatCategory category;
  final public boolean writeStrict;


  Format(FormatCategory category) {
    this(category, true);
  }


  Format(FormatCategory category, boolean writeStrict) {
    this.category = category;
    this.writeStrict = writeStrict;
  }

  static public Format valueOfIgnoreCase(String fmt) {
    for (Format value : Format.values()) {
      if (value.toString().equals(fmt.toUpperCase())) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unknown format '" + fmt + "'.");
  }
}
