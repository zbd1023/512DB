package edu.duke.distributed_systems;

public class ScanAction extends Action {
	private String startKey;
	private String endKey;
	
	public ScanAction(String startKey, String endKey) {
		this.startKey = startKey;
		this.endKey = endKey;
	}
	
	public String getStartKey() {
		return startKey;
	}
	
	public String getEndKey() {
		return endKey;
	}
}
