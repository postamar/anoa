package com.adgear.anoa;

public interface ThrowingFunction<T, R> {

  R apply(T t) throws Exception;

}
