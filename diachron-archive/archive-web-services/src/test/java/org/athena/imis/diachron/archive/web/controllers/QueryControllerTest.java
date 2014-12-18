/**
 * 
 */
package org.athena.imis.diachron.archive.web.controllers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;
import org.athena.imis.diachron.archive.api.Query;
import org.athena.imis.diachron.archive.api.QueryStatement;
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

/**
 * @author chris
 *
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
		"file:src/main/webapp/WEB-INF/spring/applicationContext.xml",
		"file:src/main/webapp/WEB-INF/spring/webmvc-test-config.xml" })
@WebAppConfiguration
public class QueryControllerTest {

	private MockMvc mockMvc;
	
	@Autowired
	private WebApplicationContext webApplicationContext;

	@Autowired
	private QueryStatement queryStatementMock;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		// We have to reset our mock between tests because the mock objects
		// are managed by the Spring container. If we would not reset them,
		// stubbing and verified behavior would "leak" from one test to another.
		Mockito.reset(queryStatementMock);

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
	 * Test method for {@link org.athena.imis.diachron.archive.web.controllers.QueryController#queryData(javax.servlet.http.HttpServletRequest)}.
	 * @throws Exception 
	 */
	@Test
	public final void testQueryData() throws Exception {
		String jsonContent = "{\"message\":\"\",\"data\":\"serialized results\",\"success\":true}";
		ArgumentCaptor<Query> argumentCaptor = ArgumentCaptor.forClass(Query.class);
		
		ArchiveResultSet arsMock = mock(ArchiveResultSet.class);
		
		when(queryStatementMock.executeQuery(argumentCaptor.capture()))
				.thenReturn(arsMock);
		when(arsMock.serializeResults("SELECT"))
			.thenReturn("serialized results");

		// MvcResult res =
		mockMvc.perform(get("/archive")
					.param("queryType", "SELECT")
					.param("query", "test query"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.success", is(true)))
				.andExpect(content().json(jsonContent))
				// .andExpect(jsonPath("$.description", is("Lorem ipsum")))
				.andReturn();

		assertEquals(argumentCaptor.getValue().getQueryText(), "test query");
		assertEquals(argumentCaptor.getValue().getQueryType(), "SELECT");
		
		// System.out.println(res.getResponse().getContentAsString());

		// verify(dataStatementMock, times(1)).createDiachronicDataset(null);
		// verifyNoMoreInteractions(dataStatementMock);

		// fail("Not yet implemented");
	}
//
//	/**
//	 * Test method for {@link org.athena.imis.diachron.archive.web.controllers.QueryController#queryTemplates(javax.servlet.http.HttpServletRequest)}.
//	 */
//	@Test
//	public final void testQueryTemplates() {
//		fail("Not yet implemented"); // TODO
//	}

}
