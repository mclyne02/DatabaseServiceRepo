package org.exercise.databaseservice.business;

import java.sql.Date;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TableEntryResponse {
	long loadId;
	long fileId;
	String fundId;
	Date asOfDate;
	double navValNumber;
	long mdmRegId;
	String updateId;
	
	public TableEntryResponse() {}
	
	//GETTERS AND SETTERS
	public long getLoadId() {
		return loadId;
	}
	public void setLoadId(long loadId) {
		this.loadId = loadId;
	}
	public long getFileId() {
		return fileId;
	}
	public void setFileId(long fileId) {
		this.fileId = fileId;
	}
	public String getFundId() {
		return fundId;
	}
	public void setFundId(String fundId) {
		this.fundId = fundId;
	}
	public Date getAsOfDate() {
		return asOfDate;
	}
	public void setAsOfDate(Date asOfDate) {
		this.asOfDate = asOfDate;
	}
	public double getNavValNumber() {
		return navValNumber;
	}
	public void setNavValNumber(double navValNumber) {
		this.navValNumber = navValNumber;
	}
	public long getMdmRegId() {
		return mdmRegId;
	}
	public void setMdmRegId(long mdmRegId) {
		this.mdmRegId = mdmRegId;
	}
	public String getUpdateId() {
		return updateId;
	}
	public void setUpdateId(String updateId) {
		this.updateId = updateId;
	}
}
