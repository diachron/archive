package org.athena.imis.diachron.archive.core.dataloader;

/**
 * This class models exceptions for the Reconstruct class.
 * @author Marios Meimaris
 *
 */
public class ReconstructException extends Exception {

	
	    public ReconstructException () {

	    }

	    public ReconstructException (String message) {
	        super (message);
	    }

	    public ReconstructException (Throwable cause) {
	        super (cause);
	    }

	    public ReconstructException (String message, Throwable cause) {
	        super (message, cause);
	    }
	
	
}
