package org.apache.hadoop.hive.ql.io.xml;

import net.sf.saxon.s9api.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

public class XmlRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(XmlRecordReader.class);

    private static final String XPATH_ROOT = "hive.xml.xpath.root";
    private static final String XPATH_COLS = "hive\\.xml\\.xpath\\.column\\.[0-9]+";
    private static final String ENABLE_DEBUG_PROPS = "hive.xml.debug.props";
    private static final char DELIM = '\1';

    private FSDataInputStream fileIn;
    private Processor processor;
    private DocumentBuilder builder;
    private XdmNode rootNode;
    private XdmValue rootNodes;

    private Map<Integer, XPathSelector> colXPaths;

    private NullWritable key;
    private BytesWritable value;
    private int start;
    private int end;
    private int current;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        final Path file = split.getPath();
        final FileSystem fs = file.getFileSystem(job);
        final boolean enableDebugProps = job.getBoolean(ENABLE_DEBUG_PROPS, false);

        key = NullWritable.get();
        value = new BytesWritable();

        // If Debug Properties Enabled - Dump All hive.xml properties.
        if (enableDebugProps) {
            Map<String, String> props = job.getValByRegex("hive\\.xml\\..*");
            props.forEach((key, value) -> {
                LOG.info(key + " : " + value);
            });
        }

        final String rootXPathExpression = job.get(XPATH_ROOT);
        final Map<String, String> colXPathExpressions = job.getValByRegex(XPATH_COLS);

        fileIn = fs.open(file);
        processor = new Processor(false);
        builder = processor.newDocumentBuilder();
        InputSource is = new InputSource(fileIn);
        try {
            rootNode = builder.build(new StreamSource(fileIn));
            XPathCompiler xPath = processor.newXPathCompiler();
            XPathSelector rootXPath = xPath.compile(rootXPathExpression).load();
            rootXPath.setContextItem(rootNode);
            rootNodes = rootXPath.evaluate();

            colXPaths = new TreeMap<>();
            for (Map.Entry<String, String> e : colXPathExpressions.entrySet()) {
                colXPaths.put(Integer.valueOf(e.getKey().replace("hive.xml.xpath.column.","")), xPath.compile(e.getValue()).load());
            }
        } catch (SaxonApiException ex) {
            throw new IOException(ex);
        }

        start = 0;
        current = 0;
        end = rootNodes.size();

        // TODO Implement Compression
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (current == end) {
            return false;
        } else {
            XdmItem node = rootNodes.itemAt(current);
            ByteArrayOutputStream colBytes = new ByteArrayOutputStream();
            for (Map.Entry<Integer, XPathSelector> e : colXPaths.entrySet()) {
                if (e.getKey().intValue() > 1) {
                    colBytes.write(DELIM);
                }
                XPathSelector colXPath = e.getValue();
                try {
                    colXPath.setContextItem(node);
                    XdmValue colValue = colXPath.evaluate();
                    switch (colValue.size()) {
                        case 0:
                            colBytes.write("".getBytes());
                            break;
                        case 1:
                            colBytes.write(colValue.itemAt(0).getStringValue().getBytes());
                            break;
                        default:
                            colBytes.write(colValue.toString().getBytes());
                            break;
                    }
                } catch (SaxonApiException ex) {
                    throw new IOException(ex);
                }
                value.set(colBytes.toByteArray(),0,colBytes.size());
            }
            current++;
            return true;
        }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        if (fileIn != null) {
            fileIn.close();
        }
    }
}
