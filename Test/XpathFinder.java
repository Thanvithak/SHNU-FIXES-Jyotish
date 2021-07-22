
package com.mycompany.domdemo;
import java.util.HashMap;
import java.util.Map;
import java.io.FileInputStream;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.XMLReader;
import org.xml.sax.InputSource;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import org.xml.sax.helpers.DefaultHandler;


public class XpathFinder extends DefaultHandler {

    private String xPath = "/";
    private XMLReader xmlReader;
    private XpathFinder parent;
    private StringBuilder characters = new StringBuilder();
    private Map<String, Integer> elementNameCount = new HashMap<String, Integer>();

    public XpathFinder (XMLReader xmlReader) {
        this.xmlReader = xmlReader;
    }

    private XpathFinder (String xPath, XMLReader xmlReader, XpathFinder parent) {
        this(xmlReader);
        this.xPath = xPath;
        this.parent = parent;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        Integer count = elementNameCount.get(qName);
        if(null == count) {
            count = 1;
        } else {
            count++;
        }
        elementNameCount.put(qName, count);
        String childXPath = xPath + "/" + qName + "[" + count + "]";
       System.out.println(childXPath);
//        int attsLength = atts.getLength();
//        for(int x=0; x<attsLength; x++) {
//            System.out.println(childXPath + "[@" + atts.getQName(x) + "='" + atts.getValue(x)+"'" + ']');
//        }
        XpathFinder child = new XpathFinder(childXPath, xmlReader, this);
        xmlReader.setContentHandler(child);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
//       String value = characters.toString().trim();
//        if(value.length() > 0) {
//            System.out.println(xPath + "='" + characters.toString() + "'");             
//        }
        xmlReader.setContentHandler(parent);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        characters.append(ch, start, length);
    }
    
    public static void main(String[] args) throws Exception {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        SAXParser sp = spf.newSAXParser();
        XMLReader xr = sp.getXMLReader();
        xr.setContentHandler(new XpathFinder(xr));
      xr.parse(new InputSource(new FileInputStream("C:\\Users\\VishnuvardhanreddyKe\\Downloads\\DMM_for_school_org_PD2DS.XML")));
     //xr.parse(new InputSource(new FileInputStream("C:\\Users\\VishnuvardhanreddyKe\\Downloads\\LedgerDimension.XML")));
    //  xr.parse(new InputSource(new FileInputStream("C:\\Users\\VishnuvardhanreddyKe\\Downloads\\fhir-all-xsd\\account.sch")));
     //xr.parse(new InputSource(new FileInputStream("C:\\Users\\VishnuvardhanreddyKe\\Downloads\\fhir-single.xsd")));
      //xr.parse(new InputSource(new FileInputStream("C:\\Users\\VishnuvardhanreddyKe\\Downloads\\fhir.schema.json\\fhir.schema.json")));
    }
}



