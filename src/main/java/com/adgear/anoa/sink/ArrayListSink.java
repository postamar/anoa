package com.adgear.anoa.sink;

import java.util.ArrayList;


public class ArrayListSink<T> extends CollectionSink<T,ArrayList<T>> {

  public ArrayListSink() {
    this(new ArrayList<T>());
  }

  public ArrayListSink(ArrayList<T> collection) {
    super(collection);
  }
}
