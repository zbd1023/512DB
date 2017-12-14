package edu.duke.distributed_systems;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KVStore extends AbstractActor{
    public static Props props() {
        return Props.create(KVStore.class);
    }
    
    public static class put {
        public String key;
        public String value;
        put(String k, String v){
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
    // For the entire kvstore, this lines creates a concurrent HashSet
    Set<String> lockSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

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
    	Queue<String> locksNeedToAcquire = new LinkedList<>();
    	actionList.forEach(a -> locksNeedToAcquire.add(a.getKey()));

    	//TODO implement timeout or max number of retries
    	while(!locksNeedToAcquire.isEmpty()){
    	    String lock = locksNeedToAcquire.poll();
    	    if(!lockSet.contains(lock)){
    	        lockSet.add(lock);
            }
            else {
                locksNeedToAcquire.add(lock);
            }
        }

        //after acquire al;l necessary locks, can add transaction
        transactionMap.put(transactionID, actionList);

//    	// acquire locks
//    	for (int i = 0; i < actionList.size(); i++) {
//    		Action action = actionList.get(i);
//    		String lockKey = action.getActionKey();
//    		locks
//    		if (lockSet.contains(lockKey)) {
//    			// vote no commit
//    			getSender().tell(false, getSelf());
//    			return;
//    		}
//    		lockSet.add(lockKey);
//    	}
    	
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
    
    private void CommitTransaction(UUID transactionID) throws Result.MalformedKeyException {
        List<Result> sendList = new ArrayList<>();
    	if (!transactionMap.containsKey(transactionID)) {
    	    //send something back to client so it doesnt timeout
            getSender().tell(sendList, getSelf());
    		return;
    	}

    	List<Action> actionList = transactionMap.get(transactionID);

    	// execute each Action
    	for (int i = 0; i < actionList.size(); i++) {
    		Action action = actionList.get(i);

    		//NOTE right now, no action will ever be a GetAction
    		if (action instanceof GetAction) {
    			GetAction getAction = (GetAction) action;
    		}
    		//INSERT STATEMENT
    		else if (action instanceof PutAction) {
    			PutAction putAction = (PutAction) action;
                sendList.add(handleInsertRes(action));

    		}
    		//SELECT STATEMENT
    		else if (action instanceof ScanAction) {
    		    ScanResult res = this.handleSelectRes(action);
    		    sendList.add(res);
    		}
    	}
        getSender().tell(sendList, getSelf());
    	releaseLocks(actionList);
    	transactionMap.remove(transactionID);
    }

    private InsertResult handleInsertRes(Action action) {
        PutAction putAction = (PutAction) action;
        store.put(putAction.getKey(), putAction.getValue());
        System.out.println(store);
        return new InsertResult(true);
    }

    private ScanResult handleSelectRes(Action action) throws Result.MalformedKeyException {
        ScanAction scanAction = (ScanAction) action;
        System.out.println("Scan!!!");
        System.out.println(scanAction);
        String start = scanAction.getKey();
        String end = scanAction.getEndKey();

        String strs[] = start.split("/");
        String colName = strs[strs.length - 1];
        ScanResult result = new ScanResult(colName);
        //start to end is not inclusive, we need to find a better way to find the range
        SortedMap<String, String> ranges = store.subMap(start, end);
        for(String key : ranges.keySet()) {
            try {
                if(key.contains(colName)) {
                    result.addResult(key, store.get(key));
                }
            }
            catch (Result.MalformedKeyException e) {
                e.printStackTrace();
            }
        }

        return result;
    }
    
    private void releaseLocks(List<Action> actionList) {
    	for (int i = 0; i < actionList.size(); i++) {
    		Action action = actionList.get(i);
    		String lockKey = action.getKey();
    		lockSet.remove(lockKey);
    	}
    }
}
