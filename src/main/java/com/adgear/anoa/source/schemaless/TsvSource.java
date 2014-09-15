package com.adgear.anoa.source.schemaless;

import org.supercsv.prefs.CsvPreference;

import java.io.Reader;

/**
 * Iterates over tab-separated values, exposed as String lists.
 *
 * @see com.adgear.anoa.source.schemaless.TsvWithHeaderSource
 */
public class TsvSource extends CsvSource {

  public TsvSource(Reader in) {
    this(in, false);
  }

  protected TsvSource(Reader in, boolean hasHeader) {
    super(in, CsvPreference.TAB_PREFERENCE, hasHeader);
  }
}
