package edu.duke.distributed_systems;

/**
 * Super class for holding data for SQL Query results
 * @author Bill Xiong
 */
public abstract class Result {

    class MalformedKeyException extends Exception {
        MalformedKeyException(String message) {
            super(message);
        }
    }

    public Result() {

    }

    public abstract Object getResult();
}

