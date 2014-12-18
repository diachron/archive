package org.athena.imis.diachron.archive.web.controllers;

import org.athena.imis.diachron.archive.api.DataStatement;
import org.athena.imis.diachron.archive.core.dataloader.ArchiveEntityMetadata;
import org.athena.imis.diachron.archive.web.Response;
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
	
	private static final DateFormat df;
	static {
		df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ"); //ISO 8601 format
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
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

    @RequestMapping(method = RequestMethod.POST)
    public @ResponseBody Response createDiachronicDataset(@RequestParam("label") String label, 
    		@RequestParam("creator") String creator) {

        logger.info("createDiachronicDataset called");
        Response resp = new Response();
        
        ArchiveEntityMetadata metadata = new ArchiveEntityMetadata();
		HashMap<String, String> metadataMap = new HashMap<String, String>();
		metadataMap.put(RDFS.label.toString(), label);
		metadataMap.put(DCTerms.creator.toString(), creator);
		metadataMap.put(DCTerms.created.toString(), df.format(new Date()));
		
		metadata.setMetadataMap(metadataMap);
		try {
			//This creates a diachronic dataset with the defined metadata 
			String URI = dataStatement.createDiachronicDataset(metadata);
			logger.info("createDiachronicDataset new diachronic id:"+URI);
			// TODO create appropriate json field
			resp.setData(URI);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
        	resp.setSuccess(false);
        	resp.setMessage(e.getMessage());
        }
        resp.setSuccess(true);
        
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