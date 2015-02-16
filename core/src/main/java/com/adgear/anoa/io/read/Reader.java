package com.adgear.anoa.io.read;

import com.adgear.anoa.AnoaTypeException;

import java.io.IOException;

public interface Reader<IN,OUT> {

  OUT read(IN in) throws IOException;

  OUT readStrict(IN in) throws AnoaTypeException, IOException;

}
