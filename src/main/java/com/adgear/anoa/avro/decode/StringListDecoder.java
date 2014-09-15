package com.adgear.anoa.avro.decode;

import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * {@link org.apache.avro.io.Decoder} implementation for deserializing Avro records from flat String
 * lists.
 */
public class StringListDecoder extends DecoderBase {

  protected String[] fields;
  protected int index;

  public StringListDecoder(List<String> list) {
    fields = list.toArray(new String[list.size()]);
    index = 0;
  }

  protected String next() throws IOException {
    if (index > fields.length) {
      throw new IOException("Decoder ran out of fields to read.");
    }
    return fields[index++];
  }

  @Override
  public void readNull() throws IOException {
    next();
  }

  @Override
  public boolean readBoolean() throws IOException {
    return parseBoolean(next());
  }

  @Override
  public int readInt() throws IOException {
    return parseInteger(next());
  }

  @Override
  public long readLong() throws IOException {
    return parseLong(next());
  }

  @Override
  public float readFloat() throws IOException {
    return parseFloat(next());
  }

  @Override
  public double readDouble() throws IOException {
    return parseDouble(next());
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    return (old == null) ? (new Utf8(next())) : old.set(next());
  }

  @Override
  public String readString() throws IOException {
    return next();
  }

  @Override
  public void skipString() throws IOException {
    next();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    throw new IOException("This method is not supported by this Decoder.");
  }

  @Override
  public void skipBytes() throws IOException {
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    throw new IOException("This method is not supported by this Decoder.");
  }

  @Override
  public void skipFixed(int length) throws IOException {
  }

  @Override
  public int readEnum() throws IOException {
    return readInt();
  }

  @Override
  public long readArrayStart() throws IOException {
    throw new IOException("This method is not supported by this Decoder.");
  }

  @Override
  public long arrayNext() throws IOException {
    throw new IOException("This method is not supported by this Decoder.");
  }

  @Override
  public long skipArray() throws IOException {
    return 0;
  }

  @Override
  public long readMapStart() throws IOException {
    throw new IOException("This method is not supported by this Decoder.");
  }

  @Override
  public long mapNext() throws IOException {
    throw new IOException("This method is not supported by this Decoder.");
  }

  @Override
  public long skipMap() throws IOException {
    return 0;
  }

  @Override
  public int readIndex() throws IOException {
    return (fields[index] == null) ? 0 : 1;
  }
}
