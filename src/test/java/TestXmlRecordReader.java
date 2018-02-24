import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.xml.XmlInputFormat;
import org.apache.hadoop.hive.ql.io.xml.XmlRecordReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

public class TestXmlRecordReader {

    @Mock
    private FileSplit split;
    @Mock
    private TaskAttemptContext context;

    @Before
    public void setup(){
//        File resourcesDirectory = new File("src/test/resources/winutils");
//        System.setProperty("hadoop.home.dir", resourcesDirectory.getAbsolutePath());

        MockitoAnnotations.initMocks(this);
        when(split.getPath()).thenReturn(new Path("src/test/resources/xml/sample_1.xml"));

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        when(context.getConfiguration()).thenReturn(conf);
    }

    @Test
    public void testXmlRecordReader() throws IOException, InterruptedException {
        RecordReader<NullWritable, BytesWritable> recordReader =  new XmlRecordReader();
        recordReader.initialize(split,context);
    }
}
