import org.apache.hadoop.io.LongWritable;

/**
 * Created by yjy on 17-7-2.
 */
public class LSHStrategy {

    private int[] record;

    public LSHStrategy() {
    }

    public void setRecord(int[] record) {
        this.record = record;
    }

    public LongWritable hash() {
        // TODO: 17-7-2
        return null;
    }
}
