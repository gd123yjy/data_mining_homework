package bean;

import org.apache.hadoop.io.Text;

/**
 * Created by yjy on 17-7-2.
 */
public class CaseFactory {

    public static Case getCase(Text[] texts) {
        Case a = new Case();
        //user_id
        a.userID = Integer.parseInt(texts[0].toString());
        //merchant_id
        a.merchantID = texts[1].toString();
        //action
        a.action = Integer.parseInt(texts[2].toString());
        //coupon_id,discount_rate,date_receive
        if ("null".equals(texts[3].toString())){
            a.couponID=0;
            a.discountRate="";
            a.dateReceive="";
        }else {
            try {
                a.couponID = Integer.parseInt(texts[3].toString());
            }catch (Exception e){
                //couponID = fixed
                a.couponID = 1;
            }

            try {
                a.discountRate = texts[4].toString();
            } catch (Exception e) {
                //discount_rate = fixed
                e.printStackTrace();
            }

            a.dateReceive = texts[5].toString();
        }

        a.date = texts[6].toString();
        if ("null".equals(a.date)){
            a.date = "";
        }

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
    public static String[] getFeatures(Case aCase) {

        String[] features = new String[11];

        features[0] = aCase.userID+aCase.merchantID;
        features[1] = ""+aCase.userID+aCase.action;
        features[2] = aCase.userID+aCase.dateReceive;
        features[3] = aCase.userID+aCase.date;

        features[4] = aCase.merchantID+aCase.couponID;
        features[5] = aCase.merchantID+parseDiscountRate(aCase.discountRate);
        features[6] = aCase.merchantID+aCase.dateReceive;
        features[7] = aCase.merchantID+aCase.date;

        features[8] = ""+aCase.couponID+parseDiscountRate(aCase.discountRate);
        features[9] = aCase.couponID+aCase.dateReceive;
        features[10] = aCase.couponID+aCase.date;

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
