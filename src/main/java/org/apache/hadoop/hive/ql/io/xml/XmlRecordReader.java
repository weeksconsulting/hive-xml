package org.apache.hadoop.hive.ql.io.xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class XmlRecordReader extends RecordReader<NullWritable, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(XmlRecordReader.class);

    private static final String XML_READER_CLASS = "hive.xml.reader.class";
    private static final String XPATH_ROOT = "hive.xml.xpath.root";
    private static final String XPATH_COLS_PREFIX = "hive.xml.xpath.column.";
    private static final String DELIM_PROP = "hive.xml.field.delim";
    private static final String DEFAULT_DELIM = "\1";

    private AbstractXmlReader xmlReader;

    private FSDataInputStream fileIn;
    private final Text value = new Text();
    private String delim;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        final FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();
        final Configuration job = context.getConfiguration();
        final FileSystem fs = file.getFileSystem(job);
        if (xmlReader == null) {
            xmlReader = (AbstractXmlReader) ReflectionUtils.newInstance(job.getClass(XML_READER_CLASS, SaxonXmlReader.class), job);
        }
        final String rootXPath = job.get(XPATH_ROOT);
        final List<String> colXPaths = extractColXPathExpressions(job);

        delim = job.get(DELIM_PROP, DEFAULT_DELIM);
        fileIn = fs.open(file);

        // TODO Implement Compression

        xmlReader.initialize(fileIn, AbstractXmlReader.EMPTY_NAMESPACE_LIST, rootXPath, colXPaths);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (xmlReader.next()) {
            StringBuilder columnStringBuilder = new StringBuilder();
            for (int i = 0; i < xmlReader.size(); i++) {
                if (i > 0) {
                    columnStringBuilder.append(delim);
                }
                columnStringBuilder.append(xmlReader.getString(i));
            }
            value.set(columnStringBuilder.toString());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
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

    // Helper method to workaround the lack of getPropsWithPrefix method in Hadoop 2.8+
    // Once all major vendors have upgraded to Hadoop 3.0 this can be replaced
    private List<String> extractColXPathExpressions(Configuration conf) {
        Map<Integer, String> colXPathMap = new TreeMap<>();
        for (Map.Entry<String, String> entry : conf) {
            if (entry.getKey().startsWith(XmlRecordReader.XPATH_COLS_PREFIX)) {
                String key = entry.getKey().substring(XmlRecordReader.XPATH_COLS_PREFIX.length());
                String value = entry.getValue();
                colXPathMap.put(Integer.valueOf(key), value);
            }
        }
        return new ArrayList<>(colXPathMap.values());
    }
}
