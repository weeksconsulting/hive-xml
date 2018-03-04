package org.apache.hadoop.hive.ql.io.xml;

import net.sf.saxon.s9api.*;

import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SaxonXmlReader extends AbstractXmlReader {

    private XdmValue rootNodes;
    private List<XPathSelector> colXPathSelectors;
    private XdmItem currentNode;
    private int nodeCount;
    private int size;
    private int current;

    @Override
    void initialize(InputStream is, Map<String, String> namespaces, String rootXPath, List<String> colXPaths) throws IOException {
        final Processor processor = new Processor(false);
        final DocumentBuilder builder = processor.newDocumentBuilder();
        final XPathCompiler xPath = processor.newXPathCompiler();

        try {
            XPathSelector rootXPathSelector = xPath.compile(rootXPath).load();
            XdmNode rootNode = builder.build(new StreamSource(is));
            rootXPathSelector.setContextItem(rootNode);
            rootNodes = rootXPathSelector.evaluate();
            colXPathSelectors = new ArrayList<>();
            for (String colXPath : colXPaths) {
                colXPathSelectors.add(xPath.compile(colXPath).load());
            }
        } catch (SaxonApiException e) {
            throw new IOException(e);
        }
        size = colXPaths.size();
        nodeCount = rootNodes.size();
        current = 0;
    }

    @Override
    boolean next() {
        if (current == nodeCount) {
            return false;
        } else {
            currentNode = rootNodes.itemAt(current);
            current++;
            return true;
        }
    }

    @Override
    int size() {
        return size;
    }

    @Override
    String getString(int i) throws IOException {
        try {
            XPathSelector colXPathSelector = colXPathSelectors.get(i);
            colXPathSelector.setContextItem(currentNode);
            XdmItem colValue = colXPathSelector.evaluateSingle();
            return colValue == null ? EMPTY_STRING : colValue.getStringValue();
        } catch (SaxonApiException e) {
            throw new IOException(e);
        }
    }
}
