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
public class Case implements Writable,Serializable {

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
        dataOutput.write(ObjectAndBytes.toByteArray(this));
    }

    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();

        byte[] data=new byte[count];
        in.readFully(data);
        Case aCase = (Case) ObjectAndBytes.toObject(data);

        this.userID=aCase.userID;
        this.merchantID=new String(aCase.merchantID);
        this.action=aCase.action;
        this.couponID=aCase.couponID;
        this.discountRate=new String(aCase.discountRate);
        this.dateReceive=new String(aCase.dateReceive);
        this.date=new String(aCase.date);
    }
}
