package com.adgear.anoa.source;

import com.adgear.anoa.provider.Provider;

import java.io.Closeable;

/**
 * Sources are Providers with no upstream provider. In other words, the opposite of {@link
 * com.adgear.anoa.codec.Codec}. {@link #getProvider()} should return null.
 *
 * @param <T> Type of the records to be provided.
 * @see com.adgear.anoa.provider.Provider
 */
public interface Source<T> extends Provider<T>, Closeable {

}
