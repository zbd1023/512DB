package edu.duke.distributed_systems;

import java.util.*;

public class Transaction {
	private List<Action> actions;
	
	public Transaction(List<Action> actions) {
		this.actions = actions;
	}
	
	public List<Action> getActions() {
		return actions;
	}
}
