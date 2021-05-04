package com.yeshaswini.movingaverage;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class MultipleOPByMovie extends MultipleOutputFormat<Text, DoubleWritable> {
    @Override
    protected String generateFileNameForKeyValue(Text key, DoubleWritable value, String name) {
        return key.toString();
    }

    @Override
    protected RecordWriter<Text, DoubleWritable> getBaseRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
        return null;
    }
}
