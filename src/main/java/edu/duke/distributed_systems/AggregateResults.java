package edu.duke.distributed_systems;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for aggregating all results from different actors into a user-friendly format.
 * @author Bill Xiong
 */

public class AggregateResults extends Result {
    private Map<String, Map<String, String>> aggregateResults;
    private List<Map<String, String>> resultList;

    public AggregateResults() {
        resultList = null;
        aggregateResults = new HashMap<>();
    }

    public void generateAggregateResults(List<Result> results) {

        for(Result result : results) {
            ScanResult scanRes = (ScanResult) result;
            Map<String, String> map = scanRes.getResult();
            for(String storeKey : map.keySet()) {
                String primary;
                try {
                    primary = scanRes.getTablePrimaryKey(storeKey);

                } catch (Result.MalformedKeyException e) {
                    e.printStackTrace();
                    return;
                }
                if(!aggregateResults.containsKey(primary)) {
                    aggregateResults.put(primary, new HashMap<>());
                }
                aggregateResults.get(primary).put(scanRes.getColname(), map.get(storeKey));

            }
        }
    }

    public List<Map<String, String>> getResult() {
        if(resultList != null)
            return resultList;
        resultList = new ArrayList<>();
        for(String key : aggregateResults.keySet())
            resultList.add(aggregateResults.get(key));

        return resultList;
    }

    public void applyWhere(String where){
        if(where.length() > 0){
            String[] clauses = where.split("AND");
            applyEqual(getConstraints(clauses));
        }

    }

    private void applyEqual(HashMap<String, String> eqs){
        ArrayList<String> keysToRemove = new ArrayList<>();
        for(String key : aggregateResults.keySet()){
            Map<String, String> res = aggregateResults.get(key);
            for(String col : eqs.keySet()){
                if(!eqs.get(col).equals(res.get(col))){
                    keysToRemove.add(key);
                    break;
                }
            }
        }
        keysToRemove.forEach(key -> aggregateResults.remove(key));
    }

    private HashMap<String, String> getConstraints(String[] eqs){
        HashMap<String, String> res = new HashMap<>();
        for(String s : eqs){
            String[] clause = s.split("=");
            String col = clause[0].trim();
            String val = clause[1].trim();
            res.put(col,val);
        }
        return res;
    }
}
