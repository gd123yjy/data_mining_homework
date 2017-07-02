package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by yjy on 17-7-1.
 */
public class CSVOutputFormat extends FileOutputFormat<LongWritable, TextArrayWritable> {

    public static String SEPERATOR = "mapreduce.output.csvoutputformat.separator";

    private CSVParser parser;

    public void setParser(CSVParser parser) {
        this.parser = parser;
    }

    @Override
    public org.apache.hadoop.mapreduce.RecordWriter<LongWritable, TextArrayWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = conf.get(SEPERATOR, ",");
        CompressionCodec codec = null;
        String extension = "";
        if(isCompressed) {
            Class file = getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(file, conf);
            extension = codec.getDefaultExtension();
        }

        Path file1 = this.getDefaultWorkFile(job, extension);
        FileSystem fs = file1.getFileSystem(conf);
        FSDataOutputStream fileOut;
        if(!isCompressed) {
            fileOut = fs.create(file1, false);
            return new CSVRecordWriter(fileOut, parser,SEPERATOR);
        } else {
            fileOut = fs.create(file1, false);
            return new CSVRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), parser,SEPERATOR);
        }
    }
}
