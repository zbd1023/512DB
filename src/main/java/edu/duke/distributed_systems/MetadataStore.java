package edu.duke.distributed_systems;

import java.util.*;

public class MetadataStore {
	private HashMap<String, String> store;
	
	public MetadataStore() {
		this.store = new HashMap<>();
	}
	
	public String getPrimaryKey(String table) {
		return store.get(table + "/primaryKey");
	}
	
	public void setPrimaryKey(String table, String primaryKey) {
		store.put(table + "/primaryKey", primaryKey);
	}
	
	public String getFirstPrimaryKey(String table) {
		return store.get(table + "/firstPrimaryKey");
	}
	
	public void setFirstPrimaryKey(String table, String firstPrimaryKey) {
		store.put(table + "/firstPrimaryKey", firstPrimaryKey);
	}
	
	public String getLastPrimaryKey(String table) {
		return store.get(table + "/lastPrimaryKey");
	}
	
	public void setLastPrimaryKey(String table, String lastPrimaryKey) {
		store.put(table + "/lastPrimaryKey", lastPrimaryKey);
	}
}
