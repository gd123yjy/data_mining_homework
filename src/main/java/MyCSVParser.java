import util.CSVParser;
import util.TextArrayWritable;

/**
 * Created by yjy on 17-7-1.
 */
public class MyCSVParser extends CSVParser {

    private Character separator = ' ';

    public MyCSVParser(Character separator) {
        this.separator = separator;
    }

    @Override
    public String[] parseInLine(String line) {
        // TODO: 17-7-1
        return new String[0];
    }

    @Override
    public String parseOutLine(TextArrayWritable value) {
        // TODO: 17-7-1
        return null;
    }
}
