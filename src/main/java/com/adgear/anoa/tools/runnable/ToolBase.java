package com.adgear.anoa.tools.runnable;

import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;

public abstract class ToolBase implements Runnable {

  @SuppressWarnings("unchecked")
  protected static <R extends SpecificRecord> Class<R> getRecordClass(String name)
      throws ClassNotFoundException {
    if (name == null) {
      throw new ClassNotFoundException("class name must not be null.");
    }
    Class<?> recordClass = Class.forName(name);
    if (!SpecificRecord.class.isAssignableFrom(recordClass)) {
      throw new ClassCastException(name + " does not implement SpecificRecord.");
    }
    return (Class<R>) recordClass;
  }

  abstract public void execute() throws IOException;

  /**
   * Calls {@link #execute()}, but rethrows any <code>IOException</code> as a
   * <code>RuntimeException</code>.
   */
  @Override
  public void run() {
    try {
      execute();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
