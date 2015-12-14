package org.athena.imis.diachron.archive.web;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.athena.imis.diachron.archive.core.dataloader.DictionaryCache;
import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;


public class AppStarter  extends HttpServlet {/**
	 * 
	 */
	private static final long serialVersionUID = -8939757419152372438L;

	@Override
	public void init() throws ServletException {
		
			StoreConnection.init();
			DictionaryCache.init();
	}

	

}