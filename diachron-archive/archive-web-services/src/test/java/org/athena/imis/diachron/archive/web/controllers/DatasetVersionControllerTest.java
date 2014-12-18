/**
 * 
 */
package org.athena.imis.diachron.archive.web.controllers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.InputStream;
import java.util.HashMap;

import org.athena.imis.diachron.archive.api.DataStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

/**
 * @author chris
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
		"file:src/main/webapp/WEB-INF/spring/applicationContext.xml",
		"file:src/main/webapp/WEB-INF/spring/webmvc-test-config.xml" })
@WebAppConfiguration
public class DatasetVersionControllerTest {
	private MockMvc mockMvc;
	
	@Autowired
	private WebApplicationContext webApplicationContext;

	@Autowired
	private DataStatement dataStatementMock;
	

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() {
		// We have to reset our mock between tests because the mock objects
		// are managed by the Spring container. If we would not reset them,
		// stubbing and verified behavior would "leak" from one test to another.
		Mockito.reset(dataStatementMock);

		mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext)
				.build();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}


	/**
	 * Test method for {@link org.athena.imis.diachron.archive.web.controllers.DatasetVersionController#createDiachronicDatasetVersion(javax.servlet.http.HttpServletRequest)}.
	 * @throws Exception 
	 */
	@Test
	public final void testCreateDiachronicDatasetVersion() throws Exception {
		String diachronicDatasetId = "testURI";
		String jsonContent = "{\"message\":\"\",\"data\":\"testReply\",\"success\":true}";
		
		ArgumentCaptor<InputStream> argCaptorInputStream = ArgumentCaptor.forClass(InputStream.class);
		ArgumentCaptor<String> argCaptorStr = ArgumentCaptor.forClass(String.class);
		
		when(dataStatementMock.loadData(argCaptorInputStream.capture(), argCaptorStr.capture()))
				.thenReturn("\"testReply\"");		       

		HashMap<String, String> contentTypeParams = new HashMap<String, String>();
	    //boundary value can be anything
	    contentTypeParams.put("boundary", "A1152");
	    MediaType mediaType = new MediaType("multipart", "form-data", contentTypeParams);
	    
	    String contentValue = prepareContent(diachronicDatasetId);
        mockMvc.perform(MockMvcRequestBuilders.post("/archive/dataset/version")
        			.contentType(mediaType)
    	    	    .content(contentValue))
    	    .andExpect(status().isOk())
				.andExpect(jsonPath("$.success", is(true)))
				.andExpect(content().json(jsonContent));
        
        
		assertEquals(argCaptorStr.getValue(), diachronicDatasetId);
		/*
		byte[] bytesArray = new byte[1000] ; 
		argCaptorInputStream.getValue().read(bytesArray);
		assertEquals(new String(bytesArray), "some data");
		*/
		//System.out.println(res.getResponse().getContentAsString());

		
	}
	
	private String prepareContent(String diachronicDatasetId) {
		
	    byte[] fileData =  "some data".getBytes();
	    //Set the bytes to a string for 'octet-stream' to read
	    String content = new String(fileData);
	    
	    return "--A1152\r\nContent-Disposition: form-data; name=\"DiachronicDatasetURI\"\r\n\r\n" 
	    		+ diachronicDatasetId 
	    		+ "\r\n--A1152\r\nContent-Disposition: form-data; name=\"DataFile\"; filename=\"data.json"
	            //content type, in this case application/octet-stream
	            + "\"\r\nContent-Type: application/octet-stream"
	            + "\r\n\r\n"
	            + content 
	            + "\r\n--A1152--\r\n";
	    
	   
	}

}
