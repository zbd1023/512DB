package edu.duke.distributed_systems;

import java.util.*;

public class Transaction {
	private List<Action> actions;
	private UUID transactionID;
	
	public Transaction(List<Action> actions) {
		this.actions = actions;
		this.transactionID = UUID.randomUUID();
	}
	
	public List<Action> getActions() {
		return actions;
	}
	
	public UUID getTransactionID() {
		return this.transactionID;
	}
}
