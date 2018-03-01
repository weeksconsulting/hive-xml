package org.apache.hadoop.hive.ql.io.xml;

import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class XmlRecordReader extends RecordReader<NullWritable, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(XmlRecordReader.class);

    private static final String XPATH_ROOT = "hive.xml.xpath.root";
    private static final String XPATH_COLS = "hive.xml.xpath.column.";
    private static final String ENABLE_DEBUG_PROPS = "hive.xml.debug.props";
    private static final String DELIM_PROP = "hive.xml.field.delim";
    private static final String DEFAULT_DELIM = "\1";
    private static final String EMPTY_STRING = "";

    private FSDataInputStream fileIn;
    private XdmValue rootNodes;
    private Map<Integer, XPathSelector> colXPaths;
    private NullWritable key;
    private Text value;
    private String delim;
    private int size;
    private int current;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Path file = split.getPath();
        Configuration job = context.getConfiguration();
        FileSystem fs = file.getFileSystem(job);

        key = NullWritable.get();
        value = new Text();
        delim = job.get(DELIM_PROP, DEFAULT_DELIM);

        // If Debug Properties Enabled - Dump All hive.xml properties.
        if (job.getBoolean(ENABLE_DEBUG_PROPS, false)) {
            Map<String, String> props = job.getValByRegex("hive\\.xml\\.");
            // Doing this awfulness to sort the output.
            // Since this is only run if debug is on it shouldn't be an issue.
            props = new TreeMap<>(props);
            props.forEach((key, value) -> LOG.info(key + " : " + value));
        }

        final String rootXPathExpression = job.get(XPATH_ROOT);
        final Map<String, String> colXPathExpressions = job.getPropsWithPrefix(XPATH_COLS);

        fileIn = fs.open(file);
        Processor processor = new Processor(false);
        DocumentBuilder builder = processor.newDocumentBuilder();
        try {
            XdmNode rootNode = builder.build(new StreamSource(fileIn));
            XPathCompiler xPath = processor.newXPathCompiler();
            XPathSelector rootXPath = xPath.compile(rootXPathExpression).load();
            rootXPath.setContextItem(rootNode);
            rootNodes = rootXPath.evaluate();

            // Compile and Aa Column XPath Selectors to Collection in Order
            colXPaths = new TreeMap<>();
            for (Map.Entry<String, String> e : colXPathExpressions.entrySet()) {
                colXPaths.put(Integer.valueOf(e.getKey()), xPath.compile(e.getValue()).load());
            }
        } catch (SaxonApiException ex) {
            throw new IOException(ex);
        }

        current = 0;
        size = rootNodes.size();

        // TODO Implement Compression
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (current == size) {
            return false;
        } else {
            XdmItem node = rootNodes.itemAt(current);
            StringBuilder columnStringBuilder = new StringBuilder();
            for (Map.Entry<Integer, XPathSelector> e : colXPaths.entrySet()) {
                if (e.getKey() > 1) {
                    columnStringBuilder.append(delim);
                }
                XPathSelector colXPath = e.getValue();
                try {
                    colXPath.setContextItem(node);
                    XdmItem colValue = colXPath.evaluateSingle();
                    if (colValue != null) {
                        columnStringBuilder.append(colValue.getStringValue());
                    } else {
                        columnStringBuilder.append(EMPTY_STRING.getBytes());
                    }
                } catch (SaxonApiException ex) {
                    throw new IOException(ex);
                }
            }
            value.set(columnStringBuilder.toString());
            current++;
            return true;
        }
    }

    @Override
    public NullWritable getCurrentKey() {
        return this.key;
    }

    @Override
    public Text getCurrentValue() {
        return this.value;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        if (fileIn != null) {
            fileIn.close();
        }
    }
}
