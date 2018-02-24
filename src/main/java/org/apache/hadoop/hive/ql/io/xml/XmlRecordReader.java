package org.apache.hadoop.hive.ql.io.xml;

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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class XmlRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(XmlRecordReader.class);

    private FSDataInputStream fileIn;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        final Path file = split.getPath();
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
//        BufferedReader br = new BufferedReader(new InputStreamReader(fileIn));
//        String line = null;
//        int lineCount = 0;
//        while ((line = br.readLine()) != null) {
//            lineCount++;
//            if (lineCount % 1000 == 0 && lineCount > 0) {
//                LOG.info("Read " + lineCount + " Line(s)");
//            }
//        }
//        LOG.info("Read " + lineCount + " Line(s)");
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return null;
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
