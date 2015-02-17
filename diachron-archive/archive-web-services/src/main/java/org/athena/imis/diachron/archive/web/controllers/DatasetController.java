package org.athena.imis.diachron.archive.web.controllers;

import org.athena.imis.diachron.archive.api.DataStatement;
import org.athena.imis.diachron.archive.core.dataloader.ArchiveEntityMetadata;
import org.athena.imis.diachron.archive.web.Response;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

/**
 * This class implements the web service interface for creating new diachronic datasets
 */
@Controller
@RequestMapping("/archive/dataset")
public class DatasetController {
	
	
    private static final Logger logger = LoggerFactory.getLogger(DatasetController.class);

    @Autowired
    private DataStatement dataStatement; // = StatementFactory.createDataStatement();
	
    
    @RequestMapping(method = RequestMethod.GET)
    public @ResponseBody Response getDataset() {

        logger.info("getDataset called");
        
        Response data = new Response();
        
        data.setSuccess(false);
        data.setMessage("No functionality for GET method here");

        return data;
    }

    @RequestMapping(method = RequestMethod.POST,  consumes = {"application/x-www-form-urlencoded" , "multipart/form-data"})
    public @ResponseBody Response createDiachronicDataset(@RequestParam("datasetName") String datasetName, @RequestParam("label") String label, 
    		@RequestParam("creator") String creator) {

        logger.info("createDiachronicDataset called");
        Response resp = new Response();
        
        
		try {
			ArchiveEntityMetadata metadata = prepareInput(datasetName, label,
					creator);
			
			//This creates a diachronic dataset with the defined metadata 
			String URI = dataStatement.createDiachronicDataset(metadata, datasetName);
			logger.info("createDiachronicDataset new diachronic id:"+URI);
			// TODO create appropriate json field
			resp.setData(URI);
	        resp.setSuccess(true);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
        	resp.setSuccess(false);
        	resp.setMessage(e.getMessage());
        }
		
        return resp;
    }
    
    @RequestMapping(method = RequestMethod.POST,  consumes = "application/json")
    public @ResponseBody Response createDiachronicDatasetFromJSON(@RequestBody String requestBody) {

        logger.info("createDiachronicDataset called");
        Response resp = new Response();
        try {
	        JSONObject jsonPost = new JSONObject(requestBody);
	        //reading the dataset name
	        String datasetName = jsonPost.optString("datasetName", "");
	        //reading metadata
	        String label = jsonPost.optString("label", "");
	        String creator = jsonPost.optString("creator", "");
	        
	        ArchiveEntityMetadata metadata = prepareInput(datasetName, label,
					creator);
			
			//This creates a diachronic dataset with the defined metadata 
			String URI = dataStatement.createDiachronicDataset(metadata, datasetName);
			logger.info("createDiachronicDataset new diachronic id:"+URI);
			// TODO create appropriate json field
			resp.setData(URI);
	        resp.setSuccess(true);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
        	resp.setSuccess(false);
        	resp.setMessage(e.getMessage());
        }
        
        return resp;
    }

	private ArchiveEntityMetadata prepareInput(String datasetName,
			String label, String creator) throws Exception {
		if ("".equals(datasetName)) {
			throw new Exception("datasetName parameter is required");
		}
      
		ArchiveEntityMetadata metadata = new ArchiveEntityMetadata();
		HashMap<String, String> metadataMap = new HashMap<String, String>();
		if (!"".equals(label))
			metadataMap.put(RDFS.label.toString(), label);
		if (!"".equals(creator))
			metadataMap.put(DCTerms.creator.toString(), creator);		
		metadata.setMetadataMap(metadataMap);
		return metadata;
	}
	
	
	@RequestMapping(method = RequestMethod.POST)
    public @ResponseBody Response invalidContentTypeRequest() {

        logger.info("createDiachronicDataset called");
        Response resp = new Response();
        
        resp.setSuccess(false);
        resp.setMessage("No valid http header for content type");
        
        return resp;
    }

    @RequestMapping(method = RequestMethod.DELETE)
    public @ResponseBody Response deleteDiachronicDataset(@RequestBody int[] ids) {

    	logger.info("deleteDiachronicDataset called");

        Response data = new Response();
        data.setSuccess(false);
        data.setMessage("Not supported yet");

        return data;
    }


}