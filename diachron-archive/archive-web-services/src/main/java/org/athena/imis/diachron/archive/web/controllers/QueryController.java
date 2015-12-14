package org.athena.imis.diachron.archive.web.controllers;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;
import org.athena.imis.diachron.archive.api.Query;
import org.athena.imis.diachron.archive.api.QueryLib;
import org.athena.imis.diachron.archive.api.QueryStatement;
import org.athena.imis.diachron.archive.web.Response;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * This class implements the web service interface for querying the archive through SPAQL queries or through the templates functionality.
 * 
 */
@Controller
public class QueryController {
	private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    @Autowired
	QueryStatement queryStatement;

    @RequestMapping(value = "/archive", method = RequestMethod.GET)
    public @ResponseBody Response queryData(HttpServletRequest request) {

        logger.info("getquery Called");
        
        String query = request.getParameter("query"); 
		String queryType = request.getParameter("queryType");
		JSONObject json = null;
		Response data = new Response();
        
		try {	        
	        if (query == null) {
	        	String URLQuery = request.getQueryString();
	        	URLQuery = java.net.URLDecoder.decode(URLQuery, "UTF-8");
	            try {
					json = new JSONObject(URLQuery);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					logger.error(e.getMessage(),e);
				}
				if (json == null) {
					logger.error("query param is required");
					data.setSuccess(false);
					data.setMessage("query param is required");
				} else {
					if (json.has("query")) {
						query = json.getString("query");
						if (json.has("queryType")) {
							queryType = json.getString("queryType");
						} else {
							logger.error("queryType param is required");
							data.setSuccess(false);
							data.setMessage("queryType param is required");
						}
					}
				}
				
	        } 
	        
	        logger.info(query);
	        logger.info(queryType);
	        
	        if (query != null && queryType != null) {
		        Query q = new Query();
		        q.setQueryText(query);
		        q.setQueryType(queryType);
		        ArchiveResultSet ars = queryStatement.executeQuery(q);
		        //String resultString = ars.serializeJenaResultSet();
		        String resultString = ars.serializeResults(queryType);
		        data.setSuccess(true);
		        //data.setData(HtmlUtils.htmlEscape(resultString));
		        data.setData(resultString);
		        //data.setData("\""+resultString.replaceAll("\n", "").replaceAll("\r", "").replace("\"", "\\\"")+"\"");
		        //data.setData(sb.toString().trim().replace("\n", ""));
		        
		        //logger.info(resultString);	        
	        } else {
				logger.error("query and queryType params are both required");
				data.setSuccess(false);
				data.setMessage("query and queryType params are both required");
	        }
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
        	data.setSuccess(false);
        	data.setMessage(e.getMessage());
        }

        return data;
    }

    @RequestMapping(value = "/archive/templates", method = RequestMethod.GET)
    public @ResponseBody Response queryTemplates(HttpServletRequest request) {

        logger.info("getquery templates Called");
        Response data = new Response();
        
        try {
        	/*
        	String contentType = request.getHeader("Content-Type");
        	String contentDisposition= request.getHeader("Content-Disposition");
        	System.out.println(contentType);
        	System.out.println(contentDisposition);
        	*/
            boolean isForm = true;
            String templateName = request.getParameter("name");
            JSONObject json = null;
			
	        if (templateName == null) {
	        	String URLQuery = request.getQueryString();
	        	URLQuery = java.net.URLDecoder.decode(URLQuery, "UTF-8");
	            try {
					json = new JSONObject(URLQuery);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					logger.error(e.getMessage(),e);
				}
				if (json == null) {
					logger.error("not existing template");
					data.setSuccess(false);
					data.setMessage("not existing template");
				} else {
					if (json.has("name")) {
						templateName = json.getString("name");
						isForm = false;
					}
				}
				
	        } 
	        
	        if ("listDiachronicDatasets".equals(templateName)) {
	        	data = handleListDiachronicDatasets();
	        } else if ("listDatasets".equals(templateName)) {
	        	data = handleListDatasets(request, isForm, json);
	        } else if ("getDataset".equals(templateName)) {
	        	data = handleGetDataset(request, isForm, json);
	        } else if ("getChangeSet".equals(templateName)) {
	        	data = handleGetChangeSet(request, isForm, json);
	        } else if ("getChangesFromChangeSet".equals(templateName)) {
	        	data = handleGetChangesFromChangeSet(request, isForm, json);
	        } else if ("getDiachronicDatasetMetadata".equals(templateName)) {
	        	data = handleGetDiachronicDatasetMetadata(request, isForm, json);
	        } else if ("getDatasetMetadata".equals(templateName)) {
	        	data = handleGetDatasetMetadata(request, isForm, json);
	        } else if ("getResourceData".equals(templateName)) {
	        	data = handleGetResouceData(request, isForm, json);
	        } else {
	        	logger.error("not existing template");
				data.setSuccess(false);
				data.setMessage("not existing template");
	        }
	        
             
        } catch (Exception e) {
        	logger.error(e.getMessage(),e);
        	data.setSuccess(false);
        	data.setMessage(e.getMessage());
        }

        return data;
    }

	private Response handleListDiachronicDatasets()
			throws Exception {
		Response data = new Response();
		QueryLib queryLib = new QueryLib();
        String resultString = queryLib.listDiachronicDatasets();
		data.setSuccess(true);
		data.setData(resultString);
		return data;
	}

	private Response handleListDatasets(HttpServletRequest request,
			boolean isForm, JSONObject json)
			throws Exception {
		Response data = new Response();
		QueryLib queryLib = new QueryLib();
        String diachronicDatasetId = getStringParam(request, isForm, json, "diachronicDatasetId");
		
        if (diachronicDatasetId != null) {
			// TODO time conditions
			//ArchiveResultSet ars = queryLib.listDatasetVersions(request.getParameter("diachronicDatasetId"), null);
			String resultString = queryLib.listDatasetVersions(diachronicDatasetId, null);
			if (resultString != null) {
				//String resultString = ars.serializeJenaModel();
		        data.setSuccess(true);
		        data.setData(resultString);data.setSuccess(true);
				data.setData(resultString);
			} else {
				logger.error("not existing Diachronic Dataset");
		    	data.setSuccess(false);
		    	data.setMessage("not existing Diachronic Dataset");
			}
		} else {
			logger.error("diachronicDatasetId is required");
			data.setSuccess(false);
			data.setMessage("diachronicDatasetId is required");
		}
		return data;
	}

	private Response handleGetDataset(HttpServletRequest request, boolean isForm, JSONObject json) {
		Response data = new Response();
		QueryLib queryLib = new QueryLib();
        String datasetId = getStringParam(request, isForm, json, "datasetId");
		if (datasetId != null) {
			//ArchiveResultSet ars = queryLib.getDatasetVersionById(request.getParameter("datasetId"));
			String resultString = queryLib.getDatasetVersionById(datasetId);
			if (resultString!= null) {
				//String resultString = ars.serializeResults("CONSTRUCT");
		        data.setSuccess(true);
				data.setData(resultString);
			} else {
				logger.error("not existing Dataset");
		    	data.setSuccess(false);
		    	data.setMessage("not existing Dataset");
			}
		} else {
			logger.error("datasetId is required");
			data.setSuccess(false);
			data.setMessage("datasetId is required");
		}
		return data;
	}

	private Response handleGetResouceData(HttpServletRequest request, boolean isForm, JSONObject json) throws Exception {
		Response data = new Response();
		QueryLib queryLib = new QueryLib();
		
		String datasetId = getStringParam(request, isForm, json, "datasetId");
		String resourceId = getStringParam(request, isForm, json, "resourceId");
		
		if (resourceId != null && datasetId != null) {
			String resultString = queryLib.getResource(datasetId, resourceId);
			if (resultString!= null) {
				//sString resultString = resultSet.serializeResults("SELECT");
		        data.setSuccess(true);
				data.setData(resultString);
			} else {
				logger.error("null resultString in getResouceData");
		    	data.setSuccess(false);
		    	data.setMessage("null resultString in getResouceData");
			}
		} else {
			logger.error("resourceId and datasetId are required");
			data.setSuccess(false);
			data.setMessage("resourceId and datasetId are required");
		}
		return data;
	}
	
	private Response handleGetChangeSet(HttpServletRequest request, boolean isForm, JSONObject json) {
		Response data = new Response();
		QueryLib queryLib = new QueryLib();
		
		String oldVersion = getStringParam(request, isForm, json, "oldVersion");
		String newVersion = getStringParam(request, isForm, json, "newVersion");
		
		if (newVersion != null && oldVersion != null) {
			String resultString = queryLib.getChangeSet(oldVersion, newVersion);
			if (resultString!= null) {
				//sString resultString = resultSet.serializeResults("SELECT");
		        data.setSuccess(true);
				data.setData(resultString);
			} else {
				logger.error("not existing change set");
		    	data.setSuccess(false);
		    	data.setMessage("not existing change set");
			}
		} else {
			logger.error("newVersion and oldVersion are required");
			data.setSuccess(false);
			data.setMessage("newVersion and oldVersion are required");
		}
		return data;
	}

	private Response handleGetChangesFromChangeSet(HttpServletRequest request,
			boolean isForm, JSONObject json) throws UnsupportedEncodingException {
		Response data = new Response();
		QueryLib queryLib = new QueryLib();
		
		String changeSetId = getStringParam(request, isForm, json, "changeSetId");
		if (changeSetId == null) {
        	logger.error("changeSetId param is required");
			data.setSuccess(false);
			data.setMessage("changeSetId param is required");
        } else {
        	String changeType = getStringParam(request, isForm, json, "changeType");
        	List<String[]> changeTypeParams = new ArrayList<String[]>();
			
        	if (changeType != null) {
        		if (isForm) {
	        		String[] params = request.getParameterValues("changeTypeParams");
	        		for (String param: params) {
	        			changeTypeParams.add(param.split("|"));
	        		}
        		} else if (json.has("templateParams")) {
        			JSONArray templateParams = json.getJSONArray("templateParams");
        			for (int i = 0; i< templateParams.length(); i++) {
        				JSONObject obj = templateParams.getJSONObject(i);
        				if (obj.has("changeTypeParams")) {
        					JSONArray params = obj.getJSONArray("changeTypeParams");
        					for (int j = 0; j<params.length(); j++) {
        						JSONObject param = params.getJSONObject(j);
        						String [] ar = {param.getString("name"),
        									param.getString("value")};
        						changeTypeParams.add(ar);
        					}
        				}
        			}
        		}
        	}
    		
    		String resultString = queryLib.getChangesFromChangeSet(changeSetId, changeType, changeTypeParams);
			if (resultString!= null) {
		        data.setSuccess(true);
				data.setData(resultString);
			} else {
				logger.error("not existing Dataset");
		    	data.setSuccess(false);
		    	data.setMessage("not existing Dataset");
			}
			
        }
		return data; 
		
	}
	
	private Response handleGetDatasetMetadata(HttpServletRequest request,
			boolean isForm, JSONObject json) throws Exception {Response data = new Response();
		QueryLib queryLib = new QueryLib();
        String datasetId = getStringParam(request, isForm, json, "datasetId");
		if (datasetId != null) {
			//ArchiveResultSet ars = queryLib.getDatasetVersionById(request.getParameter("datasetId"));
			String resultString = queryLib.getDatasetMetadata(datasetId);
			if (resultString!= null) {
				//String resultString = ars.serializeResults("CONSTRUCT");
		        data.setSuccess(true);
				data.setData(resultString);
			} else {
				logger.error("not existing Dataset");
		    	data.setSuccess(false);
		    	data.setMessage("not existing Dataset");
			}
		} else {
			logger.error("datasetId is required");
			data.setSuccess(false);
			data.setMessage("datasetId is required");
		}
		return data;
	}

	private Response handleGetDiachronicDatasetMetadata(HttpServletRequest request,
			boolean isForm, JSONObject json) throws Exception {Response data = new Response();
		QueryLib queryLib = new QueryLib();
        String diachronicDatasetId = getStringParam(request, isForm, json, "datasetId");
		if (diachronicDatasetId != null) {
			//ArchiveResultSet ars = queryLib.getDatasetVersionById(request.getParameter("datasetId"));
			String resultString = queryLib.getDatasetMetadata(diachronicDatasetId);
			if (resultString!= null) {
				//String resultString = ars.serializeResults("CONSTRUCT");
		        data.setSuccess(true);
				data.setData(resultString);
			} else {
				logger.error("not existing Dataset");
		    	data.setSuccess(false);
		    	data.setMessage("not existing Dataset");
			}
		} else {
			logger.error("datasetId is required");
			data.setSuccess(false);
			data.setMessage("datasetId is required");
		}
		return data;
	}

	private String getStringParam(HttpServletRequest request, boolean isForm,
			JSONObject json, String name) {
		if (isForm)
			return request.getParameter(name);
		else if (json.has("templateParams")) {
			return getStringFromJSONTemplateParams(json, name);
		}
		return null;
	}

	private String getStringFromJSONTemplateParams(JSONObject json,
			String name) {
		JSONArray templateParams = json.getJSONArray("templateParams");
		for (int i = 0; i< templateParams.length(); i++) {
			JSONObject obj = templateParams.getJSONObject(i);
			if (obj.has("changeSetId"))
				return obj.getString("changeSetId");
		}
		return null;
	}


}