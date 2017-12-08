package edu.duke.distributed_systems;

public class GetAction extends Action {
	private String key;
	
	public GetAction(String key) {
		this.key = key;
	}
	
	public String getKey() {
		return key;
	}
}
