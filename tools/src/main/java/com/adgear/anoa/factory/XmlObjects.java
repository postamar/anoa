package com.adgear.anoa.factory;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;

public class XmlObjects extends JacksonObjects<
    XmlMapper,
    XmlFactory,
    FormatSchema,
    FromXmlParser,
    ToXmlGenerator> {

  public XmlObjects() {
    super(new XmlMapper());
  }
}
