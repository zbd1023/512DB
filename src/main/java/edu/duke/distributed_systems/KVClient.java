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

    public void processTransaction(Transaction tx) throws Exception {
    	UUID transactionID = tx.getTransactionID();
    	List<Action> actionList = tx.getActions();
    	
    	// map of KVStore ActorRef to its list of Actions to execute
    	Map<ActorRef, List<Action>> transactionMap = new HashMap<ActorRef, List<Action>>();
    	
    	for (int i = 0; i < actionList.size(); i++) {
    		Action action = actionList.get(i);
    		ActorRef store = route(action.getActionKey());
    		
    		if (!transactionMap.containsKey(store)) {
    			transactionMap.put(store, new ArrayList<Action>());
    		}
    		transactionMap.get(store).add(action);
    	}
    	
    	// send the Actions to each KVStore ActorRef and request votes
    	for (ActorRef store : transactionMap.keySet()) {
    		Timeout timeout = new Timeout(Duration.create(1, "seconds"));
            Future<Object> future = Patterns.ask(store, new KVStore.beginTransaction(transactionID, actionList), timeout);
            boolean vote = (Boolean) Await.result(future, timeout.duration());
            
            if (!vote) {
            	abortTransaction(transactionID);
            	return;
            }
    	}
    	
    	commitTransaction(transactionID);
    	// TODO: return the result somehow
    }
    
    private void abortTransaction(UUID transactionID) throws Exception {
    	for (int i = 0; i < KVStores.length; i++) {
    		Timeout timeout = new Timeout(Duration.create(1, "seconds"));
    		Future<Object> future = Patterns.ask(KVStores[i], new KVStore.abortTransaction(transactionID), timeout);
    		Await.result(future, timeout.duration());
    	}
    }
    
    private void commitTransaction(UUID transactionID) throws Exception {
    	for (int i = 0; i < KVStores.length; i++) {
    		Timeout timeout = new Timeout(Duration.create(1, "seconds"));
    		Future<Object> future = Patterns.ask(KVStores[i], new KVStore.commitTransaction(transactionID), timeout);
    		Await.result(future, timeout.duration());
     	}
    	
    	// TODO: return the result somehow
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
        // need to figure out this routing function
        return KVStores[0];
    }
}
