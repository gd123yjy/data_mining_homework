package bean;

/**
 * Created by yjy on 17-7-2.
 */
public class CaseForResult extends Case {

    public CaseForResult(int userID, int couponID, String dateReceive) {
        this.userID = userID;
        this.couponID = couponID;
        this.dateReceive = dateReceive;
    }

    @Override
    public String toString() {
        return userID +
                "," + couponID +
                "," + dateReceive;
    }
}
