package edu.duke.distributed_systems;

import java.util.*;

public class SqlLayer {
	
	public static Transaction makeSelectTransaction(String query) {
		SelectSqlParser parser = new SelectSqlParser(query);
		String table = parser.getTable();
		List<String> columns = parser.getColumns();
		String where = parser.getWhere();
		
		
	}
	
	public static Transaction makeInsertTransaction(String query) {
		
	}
	
	public static Transaction makeCreateTableTransaction(String query) {
		
	}
}
