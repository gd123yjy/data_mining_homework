package util;

import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by yjy on 17-7-1.
 */
public class CSVRecordWriter extends RecordWriter<LongWritable, TextArrayWritable> {

    private final static byte[] newline;
    private CSVParser parser;
    private final byte[] keyValueSeparator;
    protected DataOutputStream out;

    public CSVRecordWriter(DataOutputStream out,CSVParser parser,String keyValueSeparator) {
        this.out = out;
        if (parser==null) throw new IllegalArgumentException("parser cannot be null");
        this.parser = parser;
        this.keyValueSeparator = keyValueSeparator.getBytes();
    }

    public void write(LongWritable key, TextArrayWritable value) throws IOException {

        boolean nullKey = key == null;
        boolean nullValue = value == null;
        if(!nullKey || !nullValue) {
            if(!nullKey) {
                this.writeObject(key);
            }

            if(!nullKey && !nullValue) {
                this.out.write(this.keyValueSeparator);
            }

            if(!nullValue) {
                this.writeObject(value);
            }

            this.out.write(newline);
        }
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.out.close();
    }

    private void writeObject(Object o) throws IOException {
        if(o instanceof TextArrayWritable) {
            String to = parser.parseOutLine((TextArrayWritable)o);
            this.out.write(to.getBytes());
        } else {
            this.out.write(o.toString().getBytes("UTF-8"));
        }

    }


    static {
        try {
            newline = "\n".getBytes("UTF-8");
        } catch (UnsupportedEncodingException var1) {
            throw new IllegalArgumentException("can\'t find UTF-8 encoding");
        }
    }

}
