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
        try {
            a.couponID = Integer.parseInt(texts[3].toString());
        }catch (Exception e){
            //couponID = fixed
            a.couponID = 1;
        }
        a.discountRate = texts[4].toString();
        a.dateReceive = texts[5].toString();
        a.date = texts[6].toString();
        return a;
    }


    /**
     * 特征值：（不知道如何选取特征值，简单几个好了。）
     *
     * user_id+merchant_id,
     * user_id+action,
     * user_id+date_received,
     * user_id+date,
     *
     * merchant_id+coupon_id,
     * merchant_id+discount_rate,
     * merchant_id+date_received,
     * merchant_id+date,
     *
     * coupon_id+discount_rate,
     * coupon_id+date_received,
     * coupon_id+date,
     *
     * 输出格式：
     * feature1
     */
    public static long[] getFeatures(Case aCase) {

        long[] features = new long[11];

        features[0] = Long.parseLong(aCase.userID+aCase.merchantID);
        features[1] = Long.parseLong(""+aCase.userID+aCase.action);
        features[2] = Long.parseLong(aCase.userID+aCase.dateReceive);
        features[3] = Long.parseLong(aCase.userID+aCase.date);

        features[4] = Long.parseLong(aCase.merchantID+aCase.couponID);
        features[5] = Long.parseLong(aCase.merchantID+parseDiscountRate(aCase.discountRate));
        features[6] = Long.parseLong(aCase.merchantID+aCase.dateReceive);
        features[7] = Long.parseLong(aCase.merchantID+aCase.date);

        features[8] = Long.parseLong(""+aCase.couponID+parseDiscountRate(aCase.discountRate));
        features[9] = Long.parseLong(aCase.couponID+aCase.dateReceive);
        features[10] = Long.parseLong(aCase.couponID+aCase.date);

        return features;
    }

    /**
     *
     * @param discountRate eg:   500:50  means 50 discounted,you can buy it with 450 yuan
     * @return rate percentage,eg: 10 mean discount rate is 10%
     */
    public static int parseDiscountRate(String discountRate) {
        if (discountRate==null || "".equals(discountRate))
        return 0;
        else {
            String[] strings = discountRate.split(":");
            return 100*Integer.parseInt(strings[1])/Integer.parseInt(strings[0]);
        }
    }
}
