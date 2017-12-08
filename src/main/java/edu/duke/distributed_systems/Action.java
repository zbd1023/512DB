package edu.duke.distributed_systems;

public abstract class Action {
	public String getActionKey() {
		if (this instanceof GetAction) {
			GetAction getAction = (GetAction) this;
			return getAction.getKey();
		}
		else if (this instanceof PutAction) {
			PutAction putAction = (PutAction) this;
			return putAction.getKey();
		}
		else if (this instanceof ScanAction) {
			ScanAction scanAction = (ScanAction) this;
			return scanAction.getStartKey();
		}
		else {
			return null;
		}
	}
}
