package com.mycompany.domdemo;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import javax.xml.parsers.*;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
public class XpathGen extends DefaultHandler
{
    
    // map of all encountered tags and their running count
    private Map<String, Integer> tagCount;
    // keep track of the succession of elements
    private Stack<String> tags;

    // set to the tag name of the recently closed tag
    String lastClosedTag;

    /**
     * Construct the XPath expression
     */
     public static void main (String[] args) throws Exception {
//        if (args.length < 1) {
//            System.err.println("Usage: SAXCreateXPath <file.xml>");
//            System.exit(1);
//        }

        // Create a JAXP SAXParserFactory and configure it
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        spf.setValidating(false);

        // Create a JAXP SAXParser
        SAXParser saxParser = spf.newSAXParser();

        // Get the encapsulated SAX XMLReader
        XMLReader xmlReader = saxParser.getXMLReader();

        // Set the ContentHandler of the XMLReader
        xmlReader.setContentHandler(new XpathGen());

        String filename = "ledger.xsd";
        String path = new File(filename).getAbsolutePath();
        if (File.separatorChar != '/') {
            path = path.replace(File.separatorChar, '/');
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        // Tell the XMLReader to parse the XML document
        xmlReader.parse("file:"+path);
    }
    @Override
    public void endElement (String uri, String localName, String qName) throws SAXException {
        // if two tags are closed in succession (without an intermediate opening tag),
        // then the information about the deeper nested one is discarded
        if (lastClosedTag != null) {
            tags.pop();
        }
        lastClosedTag = localName;
    }
     @Override
    public void startElement (String namespaceURI, String localName, String qName, Attributes atts)
        throws SAXException
    {
        boolean isRepeatElement = false;

        if (tagCount.get(localName) == null) {
            tagCount.put(localName, 1);
        } else {
            tagCount.put(localName, 1 + tagCount.get(localName));
        }

        if (lastClosedTag != null) {
            // an element was recently closed ...
            if (lastClosedTag.equals(localName)) {
                // ... and it's the same as the current one
                isRepeatElement = true;
            } else {
                // ... but it's different from the current one, so discard it
                tags.pop();
            }
        }

        // if it's not the same element, add the new element and zero count to list
        if (! isRepeatElement) {
            tags.push(localName);
        }

        System.out.println(getCurrentXPath());
        lastClosedTag = null;
    }
    @Override
    public void startDocument() throws SAXException {
        tags = new Stack();
        tagCount = new HashMap<String, Integer>();
    }
     private String getCurrentXPath() {
        String str = "//";
        boolean first = true;
        for (String tag : tags) {
            if (first)
                str = str + tag;
            else
                str = str + "/" + tag;
            str += "["+tagCount.get(tag)+"]";
            first = false;
        }
        return str;
    }
}
