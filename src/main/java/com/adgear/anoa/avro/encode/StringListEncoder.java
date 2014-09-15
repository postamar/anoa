package com.adgear.anoa.avro.encode;

import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Encoder implementation for serializing Avro records as flat String lists.
 */
public class StringListEncoder extends EncoderBase<List<String>> {

  protected List<String> list;

  public StringListEncoder() {
    this.list = new ArrayList<>();
  }

  @Override
  public List<String> build() {
    List<String> stringList = list;
    list = new ArrayList<>();
    return stringList;
  }

  @Override
  public void writeNull() throws IOException {
    list.add(null);
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    list.add(b ? "true" : "false");
  }

  @Override
  public void writeInt(int n) throws IOException {
    list.add(Integer.toString(n));
  }

  @Override
  public void writeLong(long n) throws IOException {
    list.add(Long.toString(n));
  }

  @Override
  public void writeFloat(float f) throws IOException {
    list.add(Float.toString(f));
  }

  @Override
  public void writeDouble(double d) throws IOException {
    list.add(Double.toString(d));
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    list.add(utf8.toString());
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    throw new IOException("This method is not supported by this Encoder.");
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    throw new IOException("This method is not supported by this Encoder.");
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    throw new IOException("This method is not supported by this Encoder.");
  }

  @Override
  public void writeEnum(int e) throws IOException {
    writeInt(e);
  }

  @Override
  public void writeArrayStart() throws IOException {
    throw new IOException("This method is not supported by this Encoder.");
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    throw new IOException("This method is not supported by this Encoder.");
  }

  @Override
  public void startItem() throws IOException {
    throw new IOException("This method is not supported by this Encoder.");
  }

  @Override
  public void writeArrayEnd() throws IOException {
    throw new IOException("This method is not supported by this Encoder.");
  }

  @Override
  public void writeMapStart() throws IOException {
    throw new IOException("This method is not supported by this Encoder.");
  }

  @Override
  public void writeMapEnd() throws IOException {
    throw new IOException("This method is not supported by this Encoder.");
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
  }

  @Override
  public void flush() throws IOException {
  }
}
