package org.athena.imis.diachron.archive.web.services;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
/**
 * Data object for building the HTTP API response 
 *
 */
@JsonInclude(Include.NON_NULL)
public class Response {

	
	/**
     * Simple Success response which can be used for custom ajax calls
     */
    public final static Response SUCCESS = new Response(true, null);

    protected String message = "";
    protected String data = null;

    protected boolean success = false;

    public Response() {
        //no-op constructor
    }

    @JsonSerialize(using = JSONStringSerializer.class)
    public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public Response(boolean success) {
        this(success,null);
    }

    public Response(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public boolean getSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
