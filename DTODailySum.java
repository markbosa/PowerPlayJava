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
	 * Business Requirement : 
	 *	Produce a Usage Data CSV file for FTP export to ExactTarget
	 *
	 *	File Name: Usage_Data.csv
	 *
	 *	File Format		
	 *		Column Header Name	Data Type	Nullable	Sample Data : Source of Data
	 *		DateKey*	        Number	           Y	1			: 1-90
	 *		PeriodDate	        Date	           Y	4/10/2012	: MySQL/powerplay View "mw_usage_cust_data.PeriodDate"
	 *		Usage	            Decimal	           Y	30.24		: SMT_Summary DynamoDB table
	 *		AvgTemp	            Decimal	           Y	85			: MySQL/powerplay View "mw_usage_cust_data.AvgTemp"
	 *		PowerPlayID	        Number	           N	111222333	: MySQL/powerplay View "mw_usage_cust_data.PowerPlayID"
	 */
		public class DTODailySum {
		private String esiid = "";  		// Primary key
		private String periodDate = "";		// Also used as Range Key
		private String dateKey = "";
		private String usage = "";
		private String avgTemp = "";
		private String powerPlayId = "";

		DTODailySum () {
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
		public String getPeriodDate() {
			return periodDate;
		}

		/**
		 * @param usageDate the usageDate to set
		 */
		public void setPeriodDate(String usageDate) {
			this.periodDate = usageDate;
		}

		/**
		 * @return the dateKey
		 */
		public String getDateKey() {
			return dateKey;
		}

		/**
		 * @param dateKey the dateKey to set
		 */
		public void setDateKey(String dateKey) {
			this.dateKey = dateKey;
		}

		/**
		 * @return the usageKwh
		 */
		public String getUsage() {
			return usage;
		}

		/**
		 * @param usageKwh the usageKwh to set
		 */
		public void setUsage(String usageKwh) {
			this.usage = usageKwh;
		}

		/**
		 * @return the avgTemp
		 */
		public String getAvgTemp() {
			return avgTemp;
		}

		/**
		 * @param avgTemp the avgTemp to set
		 */
		public void setAvgTemp(String avgTemp) {
			this.avgTemp = avgTemp;
		}

		/**
		 * @return the powerPlayId
		 */
		public String getPowerPlayId() {
			return powerPlayId;
		}

		/**
		 * @param powerPlayId the powerPlayId to set
		 */
		public void setPowerPlayId(String powerPlayId) {
			this.powerPlayId = powerPlayId;
		}

		/**
		 * 
		 * @param nextMeter
		 * @param strLine - a comma separated list of sequence sensitive data to load
		 * @return
		 * @throws IOException
		 */
		public boolean saveDtoDailySum(String strLine) throws IOException {
			boolean rc = true;
			int tokenNumber = 0;
			StringTokenizer st = null;
		
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
					this.setEsiid(st.nextToken());
					break;
		
				case 1 :
					this.setPeriodDate(st.nextToken());
					break;
		
				case 2 :
					this.setDateKey(st.nextToken());
					break;
					
				case 3 :
					this.setUsage(st.nextToken());
					break;
					
				case 4 :
					this.setAvgTemp(st.nextToken());
					break;
					
				case 5 :
					this.setPowerPlayId(st.nextToken());
					break;
					
				default : //display any CSV field value not defined above
					System.out.println(	
							", Extra Token # " + tokenNumber + 
							", Token : "+ st.nextToken());
					rc = false;
					break;
				}				
			}
			return rc;
		}
	}
