package bean;

import org.apache.hadoop.io.Text;

/**
 * Created by yjy on 17-7-2.
 */
public class CaseFactory {

    public static Case getCase(Text[] texts) {
        Case a = new Case();
        a.userID = Integer.parseInt(texts[0].toString());
        a.merchantID = texts[1].toString();
        a.action = Integer.parseInt(texts[2].toString());
        a.couponID = Integer.parseInt(texts[3].toString());
        a.discountRate = texts[4].toString();
        a.dateReceive = texts[5].toString();
        a.date = texts[6].toString();
        return a;
    }


    /**
     * 特征值：（不知道如何选取特征值，简单几个好了。）
     * user_id+merchant_id,#某个用户n次在某家店看、领、用卷
     * user_id+action,#某个用户看/领/用了n家店铺的卷子
     * merchant_id+coupon_rate,#某个商家某个折扣
     * coupon_id,#某张折扣卷
     *
     * 输出格式：
     * feature1
     */
    public static int[] getFeatures(Case aCase) {
        // TODO: 17-7-2
        return new int[0];
    }
}
