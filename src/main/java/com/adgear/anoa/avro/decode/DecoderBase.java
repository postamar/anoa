package com.adgear.anoa.avro.decode;

import org.apache.avro.io.Decoder;

/**
 * Extension to {@link org.apache.avro.io.Decoder}, intended for reading from text serializations.
 *
 * @see org.apache.avro.io.Decoder
 */
abstract public class DecoderBase extends Decoder {

  protected int parseInteger(String s) {
    return Integer.valueOf(s.trim());
  }

  protected long parseLong(String s) {
    return Long.valueOf(s.trim());
  }

  protected boolean parseBoolean(String s) {
    return Boolean.valueOf(s.trim().toLowerCase());
  }

  protected float parseFloat(String s) {
    return Float.valueOf(s.trim());
  }

  protected double parseDouble(String s) {
    return Double.valueOf(s.trim());
  }
}
