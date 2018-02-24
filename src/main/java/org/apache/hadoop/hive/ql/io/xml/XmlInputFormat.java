package org.apache.hadoop.hive.ql.io.xml;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class XmlInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(XmlInputFormat.class);


    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        return new XmlRecordReader();
    }
}
