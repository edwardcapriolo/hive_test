package io.teknek.hiveunit.common;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * Contains Hive responses to certain executed statement
 */
public class Response {
    private ResponseStatus statusCode;
    private List<String> result;

    public Response(ResponseStatus statusCode, List<String> result) {
        if (statusCode == null) {
            this.statusCode = ResponseStatus.FAILURE;
        } else {
            this.statusCode = statusCode;
        }
        if (result == null) {
            result = Lists.newArrayList();
        }
        this.result = Collections.unmodifiableList(result);
    }

    /**
     * @param statusCode - command execution status code: 0 for success, other number otherwise;
     * @param result     - list with result lines;
     */
    public Response(int statusCode, List<String> result) {
        this((statusCode == 0) ? ResponseStatus.SUCCESS : ResponseStatus.FAILURE, result);
    }

    /**
     * Response status stating if there was an execution success or failure.
     *
     * @return ResponseStatus, never null;
     */
    public ResponseStatus getResponseStatus() {
        return statusCode;
    }

    /**
     * Lines we got as statement execution result;
     *
     * @return List<String>, never null;
     */
    public List<String> getResult() {
        return result;
    }
}
