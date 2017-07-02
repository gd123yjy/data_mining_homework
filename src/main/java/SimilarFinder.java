import bean.Case;
import bean.CaseFactory;

import java.util.HashMap;
import java.util.TreeSet;


/**
 * Created by yjy on 17-7-2.
 */
public class SimilarFinder {

    private Case aCase;
    private Iterable<Case> values;
    /**
     *
     * @param aCase current case
     * @param values similar gross
     */
    public SimilarFinder(Case aCase, Iterable<Case> values) {
        this.aCase = aCase;
        this.values = values;
    }

    /**
     *
     * @param i find first i similar record
     * @return map<case,similarValue>
     */
    public HashMap<Case, Integer> find(int i) {
        TreeSet<Integer> treeSet = null;
        for (Case record :
                values) {
            if (aCase.equals(record)){
                //过滤自己和自己相似的情况
                continue;
            }else {
                //对所有条目计算相似度，加入到堆中
                treeSet = new TreeSet<Integer>();
                treeSet.add(calculateSimilarity(aCase,record));
            }
        }
        if (treeSet==null || treeSet.isEmpty()) return new HashMap<Case, Integer>(0);
        HashMap<Case,Integer> results = new HashMap<Case, Integer>(20);
        //从堆中取出前5条记录
        for (int j = 0; j < 5; j++) {
            if (treeSet.isEmpty()) break;
            Integer similarity = treeSet.first();
            results.put(aCase,similarity);
            treeSet.remove(similarity);
        }
        return results;
    }

    /**
     *
     * @param aCase
     * @param record
     * @return similarity ,rang 0-100
     */
    private Integer calculateSimilarity(Case aCase, Case record) {
        int similarity = 0;
        if (aCase.userID==record.userID){
            //同个用户，+50相似度
            similarity+=46;
        }
        if (aCase.merchantID==aCase.merchantID){
            //同个商家，+10相似度
            similarity+=10;
        }
        if (aCase.action==record.action){
            //同种操作，视操作类型决定
            switch (aCase.action){
                case 0:
                    similarity+=1;
                    break;
                case 1:
                    similarity+=4;
                    break;
                case 2:
                    similarity+=8;
                    break;
                default:
                    break;
            }
        }
        if (aCase.couponID!=0 && aCase.couponID!=1 && aCase.couponID==record.couponID){
            //同家商铺的同种优惠券
            similarity+=15;
        }
        if (aCase.discountRate!=null && !"".equals(aCase.discountRate) && aCase.discountRate.equals(record.discountRate)){
            //优惠率相同，且使用底价也相同
            similarity+=10;
        }
        int discountRate = CaseFactory.parseDiscountRate(aCase.discountRate);
        if (discountRate!=0 && discountRate==CaseFactory.parseDiscountRate(record.discountRate)){
            //优惠率相同，但使用底价不同
            similarity+=5;
        }
        if (aCase.dateReceive.equals(record.dateReceive)){
            //同一天领券
            similarity+=5;
        }
        if (aCase.date!=null && !"".equals(aCase.date) && aCase.date.equals(record.date)){
            //同一天消费
            similarity+=6;
        }
        return similarity;
    }
}
