package edu.duke.distributed_systems;

import akka.actor.ActorRef;
import scala.concurrent.*;
import scala.concurrent.duration.*;
import akka.util.*;
import akka.pattern.*;

import java.util.*;

public class KVClient {
    private ActorRef[] KVStores;
    
    public KVClient(ActorRef[] KVS){
        this.KVStores = KVS;
    }

    public Result processTransaction(Transaction tx) throws Exception {
    	UUID transactionID = tx.getTransactionID();
    	List<Action> actionList = tx.getActions();
    	
    	// map of KVStore ActorRef to its list of Actions to execute
    	Map<ActorRef, List<Action>> transactionMap = new HashMap<>();
        for (int i = 0; i < actionList.size(); i++) {
    		Action action = actionList.get(i);
    		ActorRef store = route(action.getKey());
    		
    		if (!transactionMap.containsKey(store)) {
    			transactionMap.put(store, new ArrayList<>());
    		}
    		transactionMap.get(store).add(action);
    	}
    	// send the Actions to each KVStore ActorRef and request votes
    	for (ActorRef store : transactionMap.keySet()) {
    		Timeout timeout = new Timeout(Duration.create(1, "seconds"));

    		//begins transaction and immediately asks for votes
            Future<Object> future = Patterns.ask(store, new KVStore.beginTransaction(transactionID, transactionMap.get(store)), timeout);
            boolean vote = (Boolean) Await.result(future, timeout.duration());
            
            if (!vote) {
            	abortTransaction(transactionID);
            	return null;
            }
    	}
    	
    	return commitTransaction(transactionID);
    }
    
    private void abortTransaction(UUID transactionID) throws Exception {
    	for (int i = 0; i < KVStores.length; i++) {
    		Timeout timeout = new Timeout(Duration.create(1, "seconds"));
    		Future<Object> future = Patterns.ask(KVStores[i], new KVStore.abortTransaction(transactionID), timeout);
    		Await.result(future, timeout.duration());
    	}
    }
    
    private Result commitTransaction(UUID transactionID) throws Exception {
        List<Result> resultList = new ArrayList<>();
    	for (int i = 0; i < KVStores.length; i++) {
    		Timeout timeout = new Timeout(Duration.create(5, "seconds"));
    		Future<Object> future = Patterns.ask(KVStores[i], new KVStore.commitTransaction(transactionID), timeout);
    		List<Result> result = (List<Result>) Await.result(future, timeout.duration());
    		resultList.addAll(result);
     	}
     	if(resultList.size() == 0)
     	    return null;

    	System.out.println("generating sql result now");
     	if(resultList.get(0) instanceof ScanResult) {
            //TODO conditions here after retrieving all results
            AggregateResults agg = new AggregateResults();
            agg.generateAggregateResults(resultList);
            return agg;
        }
        else {
    	    return new InsertResult(true);
        }
    }
    
    public String read(String key) throws Exception{
        Timeout timeout = new Timeout(Duration.create(1, "seconds"));
        Future<Object> future = Patterns.ask(route(key), new KVStore.get(key), timeout);
        return (String) Await.result(future, timeout.duration());
    }

    public void write(String key, String value) throws Exception{
        Timeout timeout = new Timeout(Duration.create(1, "seconds"));
        Future<Object> future = Patterns.ask(route(key), new KVStore.put(key, value), timeout);
        Await.result(future, timeout.duration());
    }

    public SortedMap<String, String> scan(String start, String end) throws Exception{
        Timeout timeout = new Timeout(Duration.create(5, "seconds"));
        Future<Object> future = Patterns.ask(route(start), new KVStore.scan(start, end), timeout);
        return (SortedMap<String, String>) Await.result(future, timeout.duration());
    }

    private ActorRef route(String key){
        // route same table to the same store
        return KVStores[(key.charAt(0) - 'A')%KVStores.length];
    }
}
