import bean.Case;

import java.util.HashMap;

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
        // TODO: 17-7-2
        return 0;
    }
}
