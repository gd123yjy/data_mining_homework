import bean.Case;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by yjy on 17-7-2.
 */
public class ProbilityCalculator {

    private HashMap<Case,Integer> similarCases;
    /**
     *
     * @param similarCase
     */
    public ProbilityCalculator(HashMap<Case, Integer> similarCase) {
        this.similarCases = similarCase;
    }

    public float calculate() {
        float probility = 0.0f;
        Iterator iterator = similarCases.entrySet().iterator();
        int i = 0;
        for (;iterator.hasNext();){
            Map.Entry<Case,Integer> entry = (Map.Entry) iterator.next();
            Case aCase = entry.getKey();
            if (aCase.action==2){
                //该相似用户核销了优惠券,那么根据相似程度推断概率
                probility+=entry.getValue()*0.01;
            }
            i++;
        }
        //没有相似用户则做出最中立的估计
        return i==0?0.5f:(probility/i);
    }
}
