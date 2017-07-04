package bean;

import org.apache.hadoop.io.Writable;
import util.ObjectAndBytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by yjy on 17-7-2.
 */
public class Case implements Writable {

    public int userID;
    public String merchantID;
    public int action;
    public int couponID;
    public String discountRate;
    public String dateReceive;
    public String date;

    @Override
    public String toString() {
        return userID +
                "," + merchantID +
                "," + action +
                "," + couponID +
                "," + discountRate +
                "," + dateReceive +
                "," + date;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(toString().getBytes());
    }

    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();

        String[] strings = in.readLine().split(",");
        this.userID=Integer.parseInt(strings[0]);
        this.merchantID=strings[1];
        this.action=Integer.parseInt(strings[2]);
        this.couponID=Integer.parseInt(strings[3]);
        this.discountRate=strings[4];
        this.dateReceive=strings[5];
        this.date=strings[6];
    }
}
