package org.exercise.databaseservice.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.exercise.databaseservice.business.TableEntryResponse;

import oracle.jdbc.OracleTypes;

public class ExerciseDao {
	//Connection details
	private static final String url = "jdbc:oracle:thin:@localhost:1521/ORCLPDB";
	private static final String user = "HR";
	private static final String password = "hr";
	private static Connection conn = null;
	private static CallableStatement cstmt = null;
	
	/* Stored Procedure Definitions */
	//Check for the nulls in the alphanumeric and numeric fields
	private static final String validateEntriesAlphaNumericSp = "call validate_entries_alpha(?, ?)";
	private static final String validateEntriesNumericSp = "call validate_entries_numeric(?, ?)";
	
	//Check the variance
	private static final String findVarianceSp = "call find_variance_sp(?, ?)";
	
	//Transfer the valid entries to the NAV table
	private static final String transferFromStagingSp = "call transfer_from_stage()";
	
	//Return the valid entries from the NAV table
	private static final String getValidEntriesSP = "call get_valid_entries(?, ?)";
	
	//Validate the alpha numeric values (check for nulls)
	public String validateEntriesAlphaNumeric(Long fileId) {
		getConnection();
		String response = "";
		
		try {
			cstmt = conn.prepareCall(validateEntriesAlphaNumericSp);
			cstmt.setLong(1, fileId);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			cstmt.execute();
			response = cstmt.getString(2);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return response;
	}
	
	//Validate the numeric values (check for nulls/zeros)
	public String validateEntriesNumeric(Long fileId) {
		getConnection();
		String response = "";
		
		try {
			cstmt = conn.prepareCall(validateEntriesNumericSp);
			cstmt.setLong(1, fileId);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			cstmt.execute();
			response = cstmt.getString(2);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return response;
	}
	
	//Find the Variance from one day to the next, return error foe > 1%, warning for below 1% and greater than 0, success otherwise
	public String findVariance(Long fileId) {
		getConnection();
		String response = "";
		
		try {
			cstmt = conn.prepareCall(findVarianceSp);
			cstmt.setLong(1, fileId);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			cstmt.execute();
			response = cstmt.getString(2);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return response;
	}
	
	//Transfer the valid entries from the staging table to the NAV table
	public String transferFromStaging() {
		getConnection();
		String response = "";
		
		try {
			cstmt = conn.prepareCall(transferFromStagingSp);
			cstmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return response;
	}
	
	public List<TableEntryResponse> getValidEntries(List<TableEntryResponse> entryList, Long fileId) {
		getConnection(); 
		
		try {
			cstmt = conn.prepareCall(getValidEntriesSP);
			cstmt.setLong(1, fileId);
			cstmt.registerOutParameter(2, OracleTypes.CURSOR);
			cstmt.executeQuery();
			ResultSet rs = (ResultSet) cstmt.getObject(2);
			while(rs.next()) {
				TableEntryResponse entry = new TableEntryResponse();
				entry.setLoadId(rs.getLong("LOAD_ID"));
				entry.setFileId(rs.getLong("FILE_ID"));
				entry.setFundId(rs.getString("FUND_ID"));
				entry.setAsOfDate(rs.getDate("AS_OF_DATE"));
				entry.setNavValNumber(rs.getDouble("NAV_VAL"));
				entry.setMdmRegId(rs.getLong("MDM_REG_ID"));
				entry.setUpdateId(rs.getString("UPDATE_ID"));
				entryList.add(entry);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return entryList;
	}
	
	public void getConnection() {
		try {
			DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
			conn = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
