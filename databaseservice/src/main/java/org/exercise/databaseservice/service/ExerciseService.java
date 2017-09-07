package org.exercise.databaseservice.service;

import java.util.ArrayList;
import java.util.List;

import org.exercise.databaseservice.business.TableEntryResponse;
import org.exercise.databaseservice.dao.ExerciseDao;

public class ExerciseService {
	private ExerciseDao dao = new ExerciseDao();
	
	public String validateAlphaEntries(Long fileId) {
		return dao.validateEntriesAlphaNumeric(fileId);
	}
	
	public String validateNumericEntries(Long fileId) {
		return dao.validateEntriesNumeric(fileId);
	}
	
	public String findVariance(Long fileId) {
		return dao.findVariance(fileId);
	}
	
	public void transferValidEntries() {
		dao.transferFromStaging();
	}
	
	public List<TableEntryResponse> getTableResponse(Long fileId) {
		//Return the valid entries from the NAV table
		List<TableEntryResponse> entryList = new ArrayList<TableEntryResponse>();
		return entryList = dao.getValidEntries(entryList, fileId);
	}
}
