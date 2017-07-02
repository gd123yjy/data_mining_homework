import org.apache.hadoop.io.LongWritable;

/**
 * Created by yjy on 17-7-2.
 */
public class LSHStrategy {

    private enum HashStrategy{
        Type1,
        Type2,
        Type3,
        Type_default
    };

    private long[] record;

    public LSHStrategy() {
    }

    /**
     *
     * @param record 特征值数值，每个元素都是整形，取值范围不限
     */
    public void setRecord(long[] record) {
        this.record = record;
    }

    public LongWritable hash() {
        //分成4块,每块有field_count/4个字段，块内拼接后hash，然后将块间的hashcode拼接，组成最后的hashcode
        int field_count = record.length;
        int block = (field_count/4)>0?(field_count/4):1;
        //前3块，每块固定有field_count/4个字段
        String result = "";
        for (int i = 0; i < 3; i++) {
            String block_combine="";
            for (int j=0;j<block;j++){
                block_combine+=record[i*4+j];
            }
            result = result+hash(Long.parseLong(block_combine),getStrategyType(i));
        }
        //最后一块，可能不足field_count/4个字段
        String block_combine="";
        for (int i = 3 * block + 1; i < field_count ; i++) {
            block_combine+=record[i];
        }
        result = result+hash(Long.parseLong(block_combine),getStrategyType(4));

        return new LongWritable(Long.parseLong(result));
    }

    private HashStrategy getStrategyType(int i) {
        switch (i%3){
            case 0:
                return HashStrategy.Type1;
            case 1:
                return HashStrategy.Type2;
            case 2:
                return HashStrategy.Type3;
            default:
                return HashStrategy.Type_default;
        }
    }

    /**
     * hash范围的选取很关键，太大会导致相似集为空，
     * 太小会导致许多毫不相似的条目被当做相似
     * @param key
     * @param type hash algorithm
     * @return value
     */
    private long hash(long key,HashStrategy type){
        switch (type){
            case Type1:
                return (key+3)%100;
            case Type2:
                return (3*key+2)%100;
            case Type3:
                return (5*key+1)%100;
            default:
                return key;
        }
    }
}
