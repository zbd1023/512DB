package edu.duke.distributed_systems;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Stores results for Scanning KVStore
 * @author Bill Xiong
 */
public class ScanResult extends Result {
    //    private Map<String, Map<String, String>> results;
    private Map<String, String> results;
    private String colName;

    public ScanResult(String key) throws MalformedKeyException {
        super();
        this.init(key);
    }
    private void init(String key) throws MalformedKeyException {
        System.out.println(key);
        String strs[] = key.split("/");
        if(strs.length != 3) {
            throw new MalformedKeyException("Key is not formatted correctly.");
        }
        this.results = new HashMap<>();
        this.colName = strs[2];
    }

    public Map<String, String> getResult() {
        return Collections.unmodifiableMap(results);
    }

    public String getColname() {
        return colName;
    }

    public String getTablePrimaryKey(String storeKey) throws MalformedKeyException {
        String strs[] = storeKey.split("/");
        if(strs.length != 3) {
            throw new MalformedKeyException("Key not formatted correctly");
        }
        return strs[2];
    }

    /**
     * Adds a key pair from kvstore to results map
     * @param storeKey key from kv store, which is in the form of TABLE/PRIMARY_KEY/COLUMN
     * @param columnValue The value from kv store, which represents the column value
     */
    public void addResult(String storeKey, String columnValue) throws MalformedKeyException {
        //there should not be duplicated keys
        if(results.containsKey(storeKey)) {
            throw new MalformedKeyException("Duplicate key in key value store");
        }

        results.put(storeKey, columnValue);
    }

//    /**
//     * Adds a key pair from kvstore to results map
//     * @param storeKey key from kv store, which is in the form of TABLE/PRIMARY_KEY/COLUMN
//     * @param columnValue The value from kv store, which represents the column value
//     */
//    public void addResult(String storeKey, String columnValue) throws MalformedKeyException {
//        String[] strs = storeKey.split("/");
//        if(strs.length != 3) {
//            throw new MalformedKeyException("Key is not in the format TABLE/PRIMARY_KEY/COLUMN");
//        }
//
//        String tableName = strs[0];
//        String primaryKey = strs[1];
//        String columnKey = strs[2];
//
//        String key = tableName + "/" + primaryKey;
//        if(!results.containsKey(key)) {
//            results.put(key, new HashMap<>());
//        }
//        results.get(key).put(columnKey, columnValue);
//    }
//
//    public Map<String, Map<String, String>> getResult() {
//        if(results.isEmpty())
//            return null;
//
//        return Collections.unmodifiableMap(results);
//    }
}
