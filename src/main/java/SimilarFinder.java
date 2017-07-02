import bean.Case;

import java.util.HashMap;


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
        // TODO: 17-7-2
        return null;
    }
}
