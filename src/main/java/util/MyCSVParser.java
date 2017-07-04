package util;

import util.CSVParser;
import util.TextArrayWritable;

/**
 * Created by yjy on 17-7-1.
 */
public class MyCSVParser extends CSVParser {

    private Character separator = ',';

    public MyCSVParser() {
    }

    @Override
    public String[] parseInLine(String line) {
        return line.split(separator+"");
    }

    @Override
    public String parseOutLine(TextArrayWritable value) {
        StringBuilder stringBuilder = new StringBuilder();
        int i = 0;
        for (; i < value.get().length-1; i++) {
            stringBuilder.append(value.get()[i].toString());
            stringBuilder.append(separator);
        }
        stringBuilder.append(value.get()[i].toString());
        return stringBuilder.toString();
    }
}
