package edu.duke.distributed_systems;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.security.Key;
import java.util.*;

public class KVStore extends AbstractActor{
    static public Props props() {
        return Props.create(KVStore.class);
    }
    
    public static class put {
        public String key;
        public String value;
        public put(String k, String v){
            this.key = k;
            this.value = v;
        }
    }
    
    public static class get {
        public String key;
        public get(String k){
            this.key = k;
        }
    }

    public static class scan {
        public String start;
        public String end;
        public scan(String s, String e){
            this.start = s;
            this.end = e;
        }
    }
    
    public static class beginTransaction {
    	public UUID transactionID;
    	public List<Action> actionList;
    	public beginTransaction(UUID transactionID, List<Action> actionList) {
    		this.transactionID = transactionID;
    		this.actionList = actionList;
    	}
    }
    
    public static class abortTransaction {
    	public UUID transactionID;
    	public abortTransaction(UUID transactionID) {
    		this.transactionID = transactionID;
    	}
    }
    
    public static class commitTransaction {
    	public UUID transactionID;
    	public commitTransaction(UUID transactionID) {
    		this.transactionID = transactionID;
    	}
    }

    private TreeMap<String, String> store = new TreeMap<>();
    private HashMap<UUID, List<Action>> transactionMap = new HashMap<>();
    private HashSet<String> lockSet = new HashSet<>();

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(put.class, p -> Put(p.key, p.value))
                .match(get.class, g -> Get(g.key))
                .match(scan.class, s -> Scan(s.start, s.end))
                .match(beginTransaction.class, bt -> BeginTransaction(bt.transactionID, bt.actionList))
                .match(abortTransaction.class, at -> AbortTransaction(at.transactionID))
                .match(commitTransaction.class, ct -> CommitTransaction(ct.transactionID))
                .build();
    }

    private void Put(String k, String v){
        store.put(k, v);
        getSender().tell("", getSelf());
    }
    
    private void Get(String k){
        getSender().tell(store.get(k), getSelf());
    }
    
    private void Scan(String s, String e){
        getSender().tell(store.subMap(s, e), getSelf());
    }
    
    private void BeginTransaction(UUID transactionID, List<Action> actionList) {
    	transactionMap.put(transactionID, actionList);
    	
    	// acquire locks
    	for (int i = 0; i < actionList.size(); i++) {
    		Action action = actionList.get(i);
    		String lockKey = action.getActionKey();
    		
    		if (lockSet.contains(lockKey)) {
    			// vote no commit
    			getSender().tell(false, getSelf());
    			return;
    		}
    		lockSet.add(lockKey);
    	}
    	
    	// vote yes commit
    	getSender().tell(true, getSelf());
    }
    
    private void AbortTransaction(UUID transactionID) {
    	if (!transactionMap.containsKey(transactionID)) {
    		return;
    	}
    	
    	List<Action> actionList = transactionMap.get(transactionID);
    	releaseLocks(actionList);
    	transactionMap.remove(transactionID);
    }
    
    private void CommitTransaction(UUID transactionID) {
    	if (!transactionMap.containsKey(transactionID)) {
    		return;
    	}
    	
    	List<Action> actionList = transactionMap.get(transactionID);
    	
    	// execute each Action
    	for (int i = 0; i < actionList.size(); i++) {
    		Action action = actionList.get(i);
    		
    		if (action instanceof GetAction) {
    			GetAction getAction = (GetAction) action;
    			// TODO: execute GetAction and return result
    		}
    		else if (action instanceof PutAction) {
    			PutAction putAction = (PutAction) action;
    			// TODO: execute PutAction and return result
    		}
    		else if (action instanceof ScanAction) {
    			ScanAction scanAction = (ScanAction) action;
    			// TODO: execute ScanAction and return result
    		}
    	}
    	
    	releaseLocks(actionList);
    	transactionMap.remove(transactionID);
    }
    
    private void releaseLocks(List<Action> actionList) {
    	for (int i = 0; i < actionList.size(); i++) {
    		Action action = actionList.get(i);
    		String lockKey = action.getActionKey();
    		lockSet.remove(lockKey);
    	}
    }
}
