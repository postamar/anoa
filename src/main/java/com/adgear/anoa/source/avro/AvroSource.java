package com.adgear.anoa.source.avro;

import com.adgear.anoa.provider.avro.AvroProvider;
import com.adgear.anoa.source.Source;

/**
 * An interface for Avro-aware Sources.
 *
 * @param <T> Type of the records to be provided.
 * @see com.adgear.anoa.provider.avro.AvroProvider
 */
public interface AvroSource<T> extends Source<T>, AvroProvider<T> {

}
