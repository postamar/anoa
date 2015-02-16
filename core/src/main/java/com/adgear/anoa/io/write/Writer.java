package com.adgear.anoa.io.write;

import java.io.IOException;
import java.io.OutputStream;

public interface Writer<IN,OUT> {

  void write(IN in, OUT out) throws IOException;

  void writeToStream(IN in, OutputStream out) throws IOException;

}
