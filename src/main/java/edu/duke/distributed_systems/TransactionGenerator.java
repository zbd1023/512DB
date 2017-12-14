package edu.duke.distributed_systems;

import java.util.*;

public class TransactionGenerator {
	private MetadataStore metadataStore;
	
	public TransactionGenerator(MetadataStore metadataStore) {
		this.metadataStore = metadataStore;
	}
	
	/**
	 * Generates a Transaction (list of ScanActions) from a SELECT statement
	 * 
	 * WHERE clause is currently ignored... the problem is the WHERE clause 
	 * can only be used after the transaction is exectuted (after which all
	 * the rows have been retrieved). This means the WHERE filtering is 
	 * outside the scope of this method.
	 */
	public Transaction makeSelectTransaction(String query) throws Exception {
		SelectSqlParser parser = new SelectSqlParser(query);
		String table = parser.getTable();
		List<String> columns = parser.getColumns();

		//TODO do where clause
		String where = parser.getWhere();
		String firstPrimaryKey = metadataStore.getFirstPrimaryKey(table);
		String lastPrimaryKey = metadataStore.getLastPrimaryKey(table);
		if(firstPrimaryKey == null || lastPrimaryKey == null) {
			return new Transaction(new ArrayList<>());

		}
		
		// create a ScanAction for each column to be selected
		List<Action> actionList = new ArrayList<>();
		for (int i = 0; i < columns.size(); i++) {
			String startKey = table + "/" + firstPrimaryKey + "/" + columns.get(i);
			String endKey = table + "/" + lastPrimaryKey + "/" + columns.get(i);
			actionList.add(new ScanAction(startKey, endKey));
		}
		
		return new Transaction(actionList);
	}
	
	/**
	 * Generates a Transaction (list of PutActions) from an INSERT statement
	 */
	public Transaction makeInsertTransaction(String query) throws Exception {
		InsertSqlParser parser = new InsertSqlParser(query);
		
		String table = parser.getTable();
		List<String> columns = parser.getColumns();
		List<String> values = parser.getValues();
		
		String primaryKey = metadataStore.getPrimaryKey(table);
		String primaryKeyValue = "";
		
		// find the primary key value in the inserted row
		for (int i = 0; i < columns.size(); i++) {
			if (columns.get(i).equals(primaryKey)) {
				primaryKeyValue = values.get(i);
				break;
			}
		}
		
		// update table metadata (first and last primary key) if necessary
		if (primaryKey == null || primaryKeyValue.compareTo(metadataStore.getFirstPrimaryKey(table)) < 0) {
			metadataStore.setFirstPrimaryKey(table, primaryKeyValue);
		}
		if (primaryKey == null || primaryKeyValue.compareTo(metadataStore.getLastPrimaryKey(table)) > 0) {
			metadataStore.setLastPrimaryKey(table, primaryKeyValue);
		}
		
		// create a PutAction for each column in the inserted row
		List<Action> actionList = new ArrayList<>();
		for (int i = 0; i < columns.size(); i++) {
			String key = table + "/" + primaryKeyValue + "/" + columns.get(i);
			String value = values.get(i);
			actionList.add(new PutAction(key, value));
		}
		
		Transaction tx = new Transaction(actionList);
		return tx;
	}
}
