package com.adgear.anoa.provider.avro;

import com.adgear.anoa.provider.Provider;

import org.apache.avro.Schema;

/**
 * A Provider which exposes an Avro schema describing its records.
 *
 * @param <T> Type of the records to be provided.
 * @see com.adgear.anoa.provider.Provider
 */
public interface AvroProvider<T> extends Provider<T> {

  /**
   * @return Avro schema describing the provided records.
   */
  public Schema getAvroSchema();

}
