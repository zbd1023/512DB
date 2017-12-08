package edu.duke.distributed_systems;

public class PutAction extends Action {
	private String key;
	private String value;
	
	public PutAction(String key, String value) {
		this.key = key;
		this.value = value;
	}
	
	public String getKey() {
		return key;
	}
	
	public String getValue() {
		return value;
	}
}
