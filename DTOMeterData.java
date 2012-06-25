package etlMain;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * DTO - Data Transfer Object which servers to retrieve data from the 
 * DAOMeterData.java class from persistent storage and load it into an 
 * equivalent Java object
 * 
 * @author Mark Bosakowski
 *
 */
public class DTOMeterData {

	private String esiid = "";
	private String usageDate = "";
	private String usageStartTime = "";
	private String usageEndTime = "";
	private String usageKwh = "";
	private String estimatedOrActual = "";
	
	DTOMeterData () {
		
	}

	/**
	 * @return the SMT esiid
	 */
	public String getEsiid() {
		return esiid;
	}

	/**
	 * @param esiid the esiid to set
	 */
	public void setEsiid(String esiid) {
		this.esiid = esiid;
	}

	/**
	 * @return the usageDate
	 */
	public String getUsageDate() {
		return usageDate;
	}

	/**
	 * @param usageDate the usageDate to set
	 */
	public void setUsageDate(String usageDate) {
		this.usageDate = usageDate;
	}

	/**
	 * @return the usageStartTime
	 */
	public String getUsageStartTime() {
		return usageStartTime;
	}

	/**
	 * @param usageStartTime the usageStartTime to set
	 */
	public void setUsageStartTime(String usageStartTime) {
		this.usageStartTime = usageStartTime;
	}

	/**
	 * @return the usageEndTime
	 */
	public String getUsageEndTime() {
		return usageEndTime;
	}

	/**
	 * @param usageEndTime the usageEndTime to set
	 */
	public void setUsageEndTime(String usageEndTime) {
		this.usageEndTime = usageEndTime;
	}

	/**
	 * @return the usageKwh
	 */
	public String getUsageKwh() {
		return usageKwh;
	}

	/**
	 * @param usageKwh the usageKwh to set
	 */
	public void setUsageKwh(String usageKwh) {
		this.usageKwh = usageKwh;
	}

	/**
	 * @return the estimatedOrActual
	 */
	public String getEstimatedOrActual() {
		return estimatedOrActual;
	}

	/**
	 * @param estimatedOrActual the estimatedOrActual to set
	 */
	public void setEstimatedOrActual(String estimatedOrActual) {
		this.estimatedOrActual = estimatedOrActual;
	}	
	
	public boolean processMeter( DTOMeterData nextMeter, String strLine, 
			int otherCnt, int actualCnt, int estimatedCnt, int lineNumber) throws IOException {
		boolean rc = false;
		int tokenNumber = 0;
		StringTokenizer st = null;
		String eaVal = "";

		/**
		 * break comma separated line using "," as a delimiter
		 */
		st = new StringTokenizer(strLine, ",");

		/**
		 * The initial SMT file format has 6 comma separated values that
		 * may have to be validated in different ways  
		 * For starters, collect metrics on the EstimatedActual (field 5) 
		 */
		while(st.hasMoreTokens() && tokenNumber < 6)
		{
			/**
			 * break input line into assigned fields.
			 */
			switch(tokenNumber++) {
			case 0 :
				nextMeter.setEsiid(st.nextToken());
				break;

			case 1 :
				nextMeter.setUsageDate(st.nextToken());
				break;

			case 2 :
				nextMeter.setUsageStartTime(st.nextToken());
				break;

			case 3 :
				nextMeter.setUsageEndTime(st.nextToken());
				break;

			case 4 :
				nextMeter.setUsageKwh(st.nextToken());
				break;

			case 5 :
				eaVal = st.nextToken();
				/**
				 * Keep metrics on Actual/Estimated/Other counts
				 */
				if( eaVal.compareTo("E") == 0)
					estimatedCnt++;
				else if(eaVal.compareTo("A") == 0)
					actualCnt++;
				else {
					otherCnt++;
					System.out.println(
							"Actual-Estimated Field has unexpected data: " 
									+ eaVal + " otherCnt=" + otherCnt);
				}
				nextMeter.setEstimatedOrActual(eaVal);
				rc = true;    // We have all data so success it is!
				break;

			default : //display any CSV field value not defined above
				System.out.println(	
						"Line # " + lineNumber + 
						", Extra Token # " + tokenNumber + 
						", Token : "+ st.nextToken());
				break;
			}				
		}
		return rc;
	}

}
