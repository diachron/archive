/**
 * 
 */
package org.athena.imis.diachron.archive.web.controllers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.athena.imis.diachron.archive.api.DataStatement;
import org.athena.imis.diachron.archive.core.dataloader.ArchiveEntityMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * @author chris
 * 
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
		"file:src/main/webapp/WEB-INF/spring/applicationContext.xml",
		"file:src/main/webapp/WEB-INF/spring/webmvc-test-config.xml" })
@WebAppConfiguration
public class DatasetControllerTest {
	private MockMvc mockMvc;
	// private static final Logger logger =
	// LoggerFactory.getLogger(DatasetControllerTest.class);

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
	 * Test method for
	 * {@link org.athena.imis.diachron.archive.web.controllers.DatasetController#createDiachronicDataset(java.lang.String, java.lang.String)}
	 * .
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCreateDiachronicDataset() throws Exception {
		String jsonContent = "{\"message\":\"\",\"data\":\"test\",\"success\":true}";
		ArgumentCaptor<ArchiveEntityMetadata> argumentCaptor = ArgumentCaptor.forClass(ArchiveEntityMetadata.class);
		
		when(dataStatementMock.createDiachronicDataset(argumentCaptor.capture()))
				.thenReturn("\"test\"");

		// MvcResult res =
		mockMvc.perform(post("/archive/dataset")
					.param("label", "labelMock")
					.param("creator", "creatorMock"))
				.andExpect(status().isOk())
				// .andExpect(content().contentType(TestUtil.APPLICATION_JSON_UTF8))
				.andExpect(jsonPath("$.success", is(true)))
				.andExpect(content().json(jsonContent))
				// .andExpect(jsonPath("$.description", is("Lorem ipsum")))
				.andReturn();

		assertEquals(argumentCaptor.getValue().getMetadataMap().get(RDFS.label.toString()), "labelMock");
		assertEquals(argumentCaptor.getValue().getMetadataMap().get(DCTerms.creator.toString()), "creatorMock");
		
		// System.out.println(res.getResponse().getContentAsString());

		// verify(dataStatementMock, times(1)).createDiachronicDataset(null);
		// verifyNoMoreInteractions(dataStatementMock);

		// fail("Not yet implemented");
	}

}
