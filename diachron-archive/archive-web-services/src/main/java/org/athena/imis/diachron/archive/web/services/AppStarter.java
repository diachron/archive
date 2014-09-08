package org.athena.imis.diachron.archive.web.services;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;


public class AppStarter  extends HttpServlet {/**
	 * 
	 */
	private static final long serialVersionUID = -8939757419152372438L;

	@Override
	public void init() throws ServletException {
		// FIXME There are still several static initializations based on 
	    // hard-coded paths (see StoreConnection class). Those should be
	    // modified to be more configurable. Usage of Spring for configuration
	    // & dependency injection is highly recommended at this point
	}

	

}