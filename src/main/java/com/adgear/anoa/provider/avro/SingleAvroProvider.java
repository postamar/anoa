package com.adgear.anoa.provider.avro;

import com.adgear.anoa.provider.SingleProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;

/**
 * @param <R> Type of the records to be provided, implementing {@link GenericContainer}.
 * @see com.adgear.anoa.provider.SingleProvider
 * @see com.adgear.anoa.provider.avro.AvroProvider
 */
public class SingleAvroProvider<R extends GenericContainer>
    extends SingleProvider<R>
    implements AvroProvider<R> {

  public SingleAvroProvider(R datum) {
    super(datum);
  }

  @Override
  public Schema getAvroSchema() {
    return datum.getSchema();
  }
}
