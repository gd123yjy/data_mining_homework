package util;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * Created by yjy on 17-7-1.
 */
public class CSVRecordReader
        extends RecordReader<LongWritable, TextArrayWritable> {

    private LineRecordReader lineReader;
    private TextArrayWritable value;
    private CSVParser parser;

    // 新建CSVParser实例，用来解析每一行CSV文件的每一行
    public CSVRecordReader() {
        this.lineReader = new LineRecordReader();
        this.parser = new MyCSVParser();
    }

    // 调用LineRecordReader的初始化方法，寻找分片的开始位置
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        lineReader.initialize(split, context);
    }

    // 使用LineRecordReader来得到下一条记录（即下一行）。
    // 如果到了分片（Input Split）的尾部，nextKeyValue将返回NULL
    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException {
        if (lineReader.nextKeyValue()) {
            //如果有新记录，则进行处理
            loadCSV();
            return true;
        }
        else {
            value = null;
            return false;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return lineReader.getCurrentKey();
    }

    @Override
    public TextArrayWritable getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }

    // 对CSV文件的每一行进行处理
    private void loadCSV() throws IOException {
        String line = lineReader.getCurrentValue().toString();
        // 通过OpenCSV将解析每一行的各字段
        String[] tokens = parser.parseInLine(line);
        value = new TextArrayWritable(convert(tokens));
    }

    // 将字符串数组批量处理为Text数组
    private Text[] convert(String[] tokens) {
        Text[] t = new Text[tokens.length];
        for (int i = 0; i < t.length; i++) {
            t[i] = new Text(tokens[i]);
        }
        return t;
    }
}