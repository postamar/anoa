package com.adgear.anoa.source.serialized;

import com.adgear.anoa.provider.base.CounterlessProviderBase;
import com.adgear.anoa.source.Source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * A Source for iterating over lines in a text stream, exposed as String.
 *
 * @see com.adgear.anoa.source.serialized.BytesLineSource
 */
public class StringLineSource extends CounterlessProviderBase<String> implements Source<String> {

  final private BufferedReader reader;
  private String nextLine = null;
  private boolean noNext = false;

  public StringLineSource(Reader in) {
    this(new BufferedReader(in));
  }

  public StringLineSource(BufferedReader in) {
    this.reader = in;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  protected void update() throws IOException {
    if (!noNext && nextLine == null) {
      if ((nextLine = reader.readLine()) == null) {
        noNext = true;
      }
    }
  }

  @Override
  protected String getNext() throws IOException {
    update();
    String result = nextLine;
    nextLine = null;
    return result;
  }

  @Override
  public boolean hasNext() {
    try {
      update();
    } catch (IOException e) {
      logger.error(e.getMessage());
      noNext = true;
    }
    return !noNext;
  }
}
