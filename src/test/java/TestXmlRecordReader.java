import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.xml.XmlRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.mockito.Mockito.when;

public class TestXmlRecordReader {

    private static final Logger LOG = LoggerFactory.getLogger(TestXmlRecordReader.class);

    @Mock
    private FileSplit split;
    @Mock
    private TaskAttemptContext context;

    @Before
    public void setup(){
        MockitoAnnotations.initMocks(this);

        // Set configuration options for testing.
        Configuration conf = new Configuration();
        // Enabled Local File System
        conf.set("fs.defaultFS","file:///");
        // Enabled Property Debugging
        conf.set("hive.xml.debug.props","true");
        // Set Delimiter
        conf.set("hive.xml.field.delim","|");
        // Sets Root XPath for Record Reader
        conf.set("hive.xml.xpath.root","//book");
        // Sets Col XPaths for Record Reader
        // TODO Really need to think about this pattern. Does Hive have a better way to handle this?
        conf.set("hive.xml.xpath.column.1","@id");
        conf.set("hive.xml.xpath.column.2","author");
        conf.set("hive.xml.xpath.column.3","title");
        conf.set("hive.xml.xpath.column.4","genre");
        conf.set("hive.xml.xpath.column.5","count(preceding-sibling::*) + 1");

        when(context.getConfiguration()).thenReturn(conf);
        when(split.getPath()).thenReturn(new Path("src/test/resources/xml/sample_1.xml"));
    }

    @Test
    public void testXmlRecordReader() throws IOException, InterruptedException {
        RecordReader<NullWritable, Text> recordReader =  new XmlRecordReader();
        recordReader.initialize(split,context);
        while(recordReader.nextKeyValue()){
            LOG.info(recordReader.getCurrentValue().toString());
        }
    }
}
