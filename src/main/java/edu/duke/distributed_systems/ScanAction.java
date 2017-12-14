package edu.duke.distributed_systems;

public class ScanAction extends Action {
	private String startKey;
	private String endKey;
	
	public ScanAction(String startKey, String endKey) {
		this.startKey = startKey;
		this.endKey = endKey;
	}
	
	public String getKey() {
		return startKey;
	}
	
	public String getEndKey() {
		return endKey;
	}

	@Override
	public String toString() {
		return "Scan Action{ start key: "+startKey+" end key: "+endKey+" }";
	}
}
