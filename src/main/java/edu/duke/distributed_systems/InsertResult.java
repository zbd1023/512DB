package edu.duke.distributed_systems;

/**
 * Class to store insert query results on kvstore
 * @author Bill Xiong
 */
public class InsertResult extends Result {

    private Boolean success;

    public InsertResult(boolean success) {
        super();
        this.success = success;
    }

    public Boolean getResult() {
        return success;
    }
}
