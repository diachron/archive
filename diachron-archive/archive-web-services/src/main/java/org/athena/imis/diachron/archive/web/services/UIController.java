package org.athena.imis.diachron.archive.web.services;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * Controller for the web User Interface
 *
 */
@Controller
public class UIController {

	   @RequestMapping(method = RequestMethod.GET, value="/testtest/{name}")  
	   public String myView(@PathVariable("name") String  name) {
	      return name;   
	   }

	
}