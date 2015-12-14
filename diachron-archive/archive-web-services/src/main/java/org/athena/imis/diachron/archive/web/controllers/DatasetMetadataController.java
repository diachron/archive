package org.athena.imis.diachron.archive.web.controllers;

import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.Streams;
import org.athena.imis.diachron.archive.api.DataStatement;
import org.athena.imis.diachron.archive.web.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * This class implements the web service interface for creating new dataset versions 
 * 
 *
 */
@Controller
@RequestMapping("/archive/dataset/metadata")
public class DatasetMetadataController {
	
	private static final Logger logger = LoggerFactory.getLogger(DatasetMetadataController.class);

	@Autowired
    private DataStatement dataStatement; 
	
    @RequestMapping(method = RequestMethod.GET)
    public @ResponseBody Response getDataset() {

        logger.info("getDataset called");
        
        Response data = new Response();
        
        data.setSuccess(false);
        data.setMessage("No functionality for GET method here");

        return data;
    }

    @RequestMapping(method = RequestMethod.POST)
    public @ResponseBody Response createDiachronicDatasetMetadata(HttpServletRequest request) {

        logger.info("createDatasetVersion called");
        Response resp = new Response();
        try {
        	boolean isMultipart = ServletFileUpload.isMultipartContent(request);
	        if (isMultipart) {
	        	// Create a new file upload handler
	        	ServletFileUpload upload = new ServletFileUpload();
	
	        	// Parse the request
	        	FileItemIterator iter = upload.getItemIterator(request);
	        	
	        	String diachronicDatasetURI = null;
	        	while (iter.hasNext()) {
	        	    FileItemStream item = iter.next();
	        	    String name = item.getFieldName();
	        	    InputStream stream = item.openStream();
	        	    
	        	    if (item.isFormField()) {
	        	    	if (name.equals("DiachronicDatasetURI")) {
	        	    		diachronicDatasetURI = Streams.asString(stream);
	        	    	} else {
	        	    		resp.setSuccess(false);
	        	        	resp.setMessage("invalid field name");
	        	        	break;
	        	    	}
	        	    } else {
	        	        if (name.equals("DataFile")) {
	        	        	// String filename = item.getName(); // no use for now
	        	        	// Process the input stream
	        	        	if (diachronicDatasetURI != null) {
	        	        		dataStatement.loadDiachronicDatasetMetadata(stream, diachronicDatasetURI);
	        	        	} else {
		        	        	// TODO asynchronous upload, file received before Diachronic Dataset URI
		        	        	// options: a) upload to temp graph and then assign
		        	        	// b) do not stream, save to file and then push to archive
		        	        	// c) reject the upload
		        	        	
		        	        }
		        	        /*
		        	        // save to file	
		        	        OutputStream os = new FileOutputStream("c:/temp/upload/"+item.getName());
							copyStream(stream, os);
							os.close();
							*/
	        	        } else {
	        	        	resp.setSuccess(false);
	        	        	resp.setMessage("invalid field name");
	        	        	break;
	        	        }
	        	    }
	        	}
	        	resp.setSuccess(true);
	        } else {
	        	//request body upload with json
	        	resp.setSuccess(false);
	        	resp.setMessage("only form multipart upload supported at the moment");
	        }
	        

        } catch (Exception e) {
        	logger.error(e.getMessage(),e);
        	resp.setSuccess(false);
        	resp.setMessage(e.getMessage());
        }
        
        return resp;
    }

    @RequestMapping(method = RequestMethod.DELETE)
    public @ResponseBody Response deleteDiachronicDatasetMetadata(@RequestBody int[] ids) {

    	logger.info("deleteDiachronicDatasetMetadata called");

        Response data = new Response();
        data.setSuccess(false);
        data.setMessage("Not supported yet");

        return data;
    }

    @SuppressWarnings("unused")
	private void copyStream(InputStream is, OutputStream os) {
        final int buffer_size=1024;
        try
        {
            byte[] bytes=new byte[buffer_size];
            for(;;)
            {
              int count=is.read(bytes, 0, buffer_size);
              if(count==-1)
                  break;
              os.write(bytes, 0, count);
            }
        }
        catch(Exception e){
        	logger.error(e.getMessage(),e);
        }
    }

}