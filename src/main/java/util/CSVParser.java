package util;

/**
 * Created by yjy on 17-7-1.
 */
public abstract class CSVParser {

    public abstract String[] parseInLine(String line);

    public abstract String parseOutLine(TextArrayWritable value);
}
