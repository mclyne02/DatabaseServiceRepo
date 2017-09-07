package org.exercise.databaseservice.resources;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.exercise.databaseservice.business.TableEntryResponse;
import org.exercise.databaseservice.service.ExerciseService;

@Path("/response")
public class ResponseResources {
	
	private ExerciseService service = new ExerciseService();
	
	@GET
	@Path("/validateAlphaNumeric/{fileId}")
	@Produces(MediaType.TEXT_PLAIN)
	public String validateAlphaNumeric(@PathParam("fileId") Long fileId) {
		return service.validateAlphaEntries(fileId);
	}
	
	@GET
	@Path("/validateNumeric/{fileId}")
	@Produces(MediaType.TEXT_PLAIN)
	public String validateNumeric(@PathParam("fileId") Long fileId) {
		return service.validateNumericEntries(fileId);
	}
	
	@GET
	@Path("/variance/{fileId}")
	@Produces(MediaType.TEXT_PLAIN)
	public String determineVariance(@PathParam("fileId") Long fileId) {
		return service.findVariance(fileId);
	}
	
	@GET
	@Path("/transfer")
	@Produces(MediaType.TEXT_PLAIN)
	public String transferValidEntries() {
		service.transferValidEntries();
		return "Transferred";
	}
	
	@GET
	@Path("/validEntries/{fileId}")
	@Produces(MediaType.APPLICATION_JSON)
	public List<TableEntryResponse> getResponse(@PathParam("fileId") Long fileId) {
		return service.getTableResponse(fileId);
	}

}
