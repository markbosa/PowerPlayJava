package etlMain;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UsageDataCsv {

	private Connection con = null;
	// For customer list
	private	Statement st1 = null;
	private	ResultSet rs1 = null;
	// For weather data detail
	private	Statement st2 = null;
	private	ResultSet rs2 = null;

	private String serviceDropSourceSystemId = "";
	private String powerPlayID = "";
	private String dateString = "";
	private String avgTemp = "";
	private String lbeStartDate = "";
	private String csvStartDate = "";
	private int startDay = 0;
	private int startMonth =0;
	private int startYear = 0;
	private String sEndDate = "";
	private int endDay = 0;

	private UsageCSV csv;

	UsageDataCsv() {
		this.startDay = 15;
		this.startMonth = 4;  		// Gregorian months start at zero, so adjust
		this.startYear = 2012;
		this.lbeStartDate = "2012/04/15";
		this.csvStartDate = "2012/04/14";
	}

	UsageDataCsv(int year, int month, int day) {
		this.startDay = day;
		this.startMonth = month - 1;// Gregorian months start at zero, adjust
		this.startYear = year;
		/**
		 * To match format of SMT data, we MUST strip out leading zeros
		 * on the Month and day fields.  String data is not adjusted for month
		 */
		String sMonth = padLeadingZero(Integer.toString(month));
		String sDay   = padLeadingZero(Integer.toString(day));

		this.lbeStartDate = Integer.toString(year) + "/" + sMonth + "/" + sDay;
	}

	public String padLeadingZero(String s) {
		if(s.length() == 1) {
			s = "0" + s;
		}
		return s;
	}

	public String stripLeadingZero(String s) {
		if(s.startsWith("0") == true) {
			s = s.substring(1);
		}
		return s;
	}

	
	/**
	 * @return the sEndDate
	 */
	public String getCsvStartDate() {
		return this.csvStartDate;
	}

	/**
	 * @param sEndDate the sEndDate to set
	 */
	public void setCsvStartDate(String startDate) {
		this.csvStartDate = startDate;
	}

	/**
	 * @return the sEndDate
	 */
	public String getEndDate() {
		return sEndDate;
	}

	/**
	 * @param sEndDate the sEndDate to set
	 */
	public void setEndDate(String sEndDate) {
		this.sEndDate = sEndDate;
	}

	void connectToCloud() {

		String url = "jdbc:mysql://powerplaymysqlinstance.cfzdhcemilxt.us-east-1.rds.amazonaws.com:3306/powerplay";
		String user = "etl_user";
		String password = "PowerPlay123";

		try {
			con = DriverManager.getConnection(url, user, password);
			st1 = con.createStatement();
			st2 = con.createStatement();
			// rs = st.executeQuery("SELECT VERSION()");
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(UsageDataCsv.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
		} 
	}


	/** 
	 * Generate USAGE CSV File Algorithm:
	 * 
	 * I) Generate a list of customers in play from MySQL
	 *     For each customer in list, calculate Summary data (See II) 
	 * II) Generate 60 days of look-back SMT Daily Summary data
	 * 		from 96 day-parts (15 minute segments) 
	 *     - Set customerId from MySQL customer query
	 *     - Set look-back-increment = 1
	 *     - Set look-back-count     = 60  (two months of data)
	 * III) Merge Daily Weather Data into Summary Table
	 *     - Use service_drop_source_system_id which maps to SMT ESIID
	 * 	   - USAGE_DATE Range Key identifies item to ADD ATTRIBUTE Weather
	 * IV) Generate CSV File from Summary Usage Table
	 *  
	 * @throws SQLException 
	 */
	boolean getCustomers() throws SQLException {		
		/**
		 * I) Generate a list of customers identified by PowerPlayID
		 */
		rs1 = st1.executeQuery(
				"select * from vw_usage_cust_list order by PowerPlayID" ); // where service_drop_source_system_id like '%292'");		
		return true;
	}

	boolean getCustWeather(String custId) throws SQLException {
		/**
		 * III) Run this query to get weather data for weather data merge
		 * 
		 * Should return 60 days.. but now will return all.  
		 */
		System.out.println("getCustomerWeather:: End Date: " + this.sEndDate +
				" Start Date: " + this.csvStartDate );

		rs2 = st2.executeQuery(	"select * from vw_usage_cust_data where PowerPlayID = '" + custId + "'" + 
				" AND DateString BETWEEN '" + this.sEndDate + 
				"' AND '" + this.csvStartDate + "'" );
		//	" where service_drop_source_system_id like 'custId'");
		return true;
	}

	/**
	 * populateSummaryTable
	 * 
	 * II) Generate 60 days of look-back SMT summary data from 15 minute 
	 * 		SMT interval usage data
	 * 
	 * @return true if successful - false if exception thrown
	 * @throws SQLException
	 */
	boolean populateSummaryTable () throws SQLException {
		boolean rc = true;

		/**
		 * Open up CSV output file to enable serial re-use of line item output
		 *
		 * Generate a CSV file from the Usage Summary table
		 * Creates connectors for 1) AWS SMT_Daily_Summary table
		 *                        2) BufferedWriter to create CSV file
		 */				
		boolean cb = false;

		try {
			csv = new UsageCSV();

			if(cb = csv.openUsageFile() == false) {
				rc = false;
			}
		} catch (Exception e) {
			System.out.println("Failure to open Usage Summary File rc=" + cb );
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		LookBackEngine lbe = new LookBackEngine();

		/**
		 *  Create an instance of this class for each customer 
		 *  Customer ID gets passed in as first argument
		 *  Target Date gets passed in as second argument 
		 *  
		 *  This allows for MapReduce jobs to spawn off tasks on 
		 */
		while (rs1.next()  && (rc == true)) {
			System.out.println(	"Processing service_drop: " +
					rs1.getString("service_drop_source_system_id") + " : " +
					" PowerPlay ID: " +
					rs1.getString("PowerPlayID"));

			serviceDropSourceSystemId	= rs1.getString("service_drop_source_system_id");
			powerPlayID 				= rs1.getString("PowerPlayID");

			/**
			 * Generate 60 days of SMT data for each customer
			 */
			// temporary code for testing only William Silliman (only one with data)
			if(serviceDropSourceSystemId.compareTo("10443720002592292") != 0)
				continue;

			/**
			 * Set the Customer ID - The primary key used for searching
			 */
			lbe.setEsiid(serviceDropSourceSystemId);
			//	lbe.setEsiid("10443720002592292");  // William Silliman
			/**
			 * Set the PowerPlayID
			 */
			lbe.setPowerPlayId(powerPlayID);
			/**
			 * Set the search type
			 */
			lbe.setSearchType(MeterSearch.SEARCH_BY_ESIID_DATE);
			/**
			 * Set the look-back-count - number of days to process
			 */
			lbe.setlookBackCount(60);
			/**
			 * Set the look-back-increment (# of days to go back in time)
			 */
			lbe.setLookBackIncrement(1);
			/**
			 * Reset the KWh hours to zero before we begin the summation loop
			 * This should be used within a customer loop to reset each time
			 */
			lbe.setSumKwhUsage(0.0);
			/**
			 * Set debug modes
			 */
			lbe.setDebugDetail(false);			

			/**
			 * Capture the todays run date for reporting purposes
			 */
			String DATE_FORMAT = "MM/dd/yyyy";
			SimpleDateFormat sdf =	new SimpleDateFormat(DATE_FORMAT);
			Calendar todayCal = Calendar.getInstance(); // today
			
			/**
			 *  Pass in the startDate as a command line argument so that we
			 *  will be able to run these small java jobs in parallel 
			 *  
			 *  Please note that the Gregorian Months start at zero, so you 
			 *  MUST subtract the input by 1 to get the correct month.
			 */	
			GregorianCalendar startCal = new GregorianCalendar(this.startYear, this.startMonth, this.startDay);	
			lbe.setGregorianCalendar(startCal);
			lbe.setStartDate(lbeStartDate);
			
			/**
			 * Calculate Start DAY_OF_WEEK (i.e Thursday) and for CSV file
			 * we need the day before for the SQL Weather data
			 */
			int startDay = lbe.getDayOfYear(lbe.getStartDate());
			//		startCal.set(Calendar.DAY_OF_YEAR, startDay );   // no longer needed.. already set above..
			lbe.setDayOfWeek(lbe.getGregorianCalendar().getDisplayName(Calendar.DAY_OF_WEEK, Calendar.LONG, lbe.locale));

			/**
			 * Calculate the CSV Start date to drive the weather data query
			 *  for a synchronous merge on the same date range
			 */
			int csvDay = startDay - 1;
			GregorianCalendar csvStartCal = new GregorianCalendar(this.startYear, this.startMonth, csvDay);	
			/** Generate string representation of previous back days
			 */
			csvStartCal.set(Calendar.DAY_OF_YEAR, csvDay);
			
			String month = String.format("%1$tm", csvStartCal);
			String day   = String.format("%1$te", csvStartCal);
			String year  = String.format("%1$tY", csvStartCal);
			this.setCsvStartDate(year +"/" + this.padLeadingZero(month) + "/" + this.padLeadingZero(day));
			
			/**
			 * Calculate the end date to drive the weather data query
			 *  for a synchronous merge on the same date range
			 */
			this.endDay = startDay - lbe.getlookBackCount() * lbe.getLookBackIncrement();

			//		this.endMonth = this.startMonth;
			//		this.endYear = this.startYear;
			//		/**
			//		 * adjust the end date if we cross yearly boundary
			//		 */
			//		if(this.endDay <= 0) {
			//			this.endDay = 365 + this.endDay;
			//			this.endMonth = 12 - (this.endDay % 31);
			//			this.endYear--;
			//		}
			//		
			GregorianCalendar endCal = new GregorianCalendar(this.startYear, this.startMonth, endDay);	

			/** Generate string representation of previous back days
			 */
			endCal.set(Calendar.DAY_OF_YEAR, this.endDay);

			month = String.format("%1$tm", endCal);
			day   = String.format("%1$te", endCal);
			year  = String.format("%1$tY", endCal);
			this.setEndDate(year +"/" + this.padLeadingZero(month) + "/" + this.padLeadingZero(day));

			System.out.println("Todays Date   : " + sdf.format(todayCal.getTime()));
			System.out.println("LBE Start Date: " + sdf.format(lbe.getGregorianCalendar().getTime()));
			System.out.println("CSV Start Date: " + sdf.format(csvStartCal.getTime()));
			System.out.println(" End Date eCal: " + sdf.format(endCal.getTime()));

			/**
			 * Start the hot-tub time machine
			 */
			rc = lbe.lookBackAndSummarize();

			/**
			 * Now that we have generated a day summary record, l
			 */
			getCustWeather(powerPlayID);

			this.mergeWeather();
		}  // end customer list loop

		/**
		 * With the current data at hand
		 * When we get more than 1 customer, put this in a loop of customers to 
		 * generate CSV files. 
		 */
		csv.saveSummaryToFile( "10443720002592292", csvStartDate, sEndDate);

		System.out.println("UsageData:: Lookback Program : CSV File Completion Status: " 
				+ ((rc == true) ? "Success" : "Failure"));
		return rc;
	}

	/**
	 * III) Merge Daily Weather Data into Summary Table
	 *  
	 */
	boolean mergeWeather() throws SQLException {
		boolean rc = true;
		DAODailySum daoDailySum = new DAODailySum();
		try {
			daoDailySum.init("SMT_Daily_Summary");

			while (rs2.next()  && (rc == true)) {
				System.out.println(	rs2.getString("service_drop_source_system_id") + " : " + 
						rs2.getString("PowerPlayID") + " : " +
						rs2.getString("DateString") + " : " + 
						rs2.getString("AvgTemp"));

				serviceDropSourceSystemId	= rs2.getString("service_drop_source_system_id");
				powerPlayID 				= rs2.getString("PowerPlayID");
				dateString					= rs2.getString("DateString");
				avgTemp						= rs2.getString("AvgTemp");	

				/**
				 * Update the DAODailySum object with the updated weather info
				 * This should overwrite the default "-999" value 
				 */
				daoDailySum.updateWeatherAttribute(avgTemp, serviceDropSourceSystemId, dateString);
			}
		} catch (Exception e) {
			System.out.println("mergeWeather() failed to retrieve data: rc="  + rc);
			e.printStackTrace();
		}
		return rc;
	}

	private void closeAll () {
		/**
		 * close 2 statement/result set variables and connection
		 */
		try {
			if (this.rs1 != null) {
				this.rs1.close();
			}
			if (this.st1 != null) {
				this.st1.close();
			}

			if (this.rs2 != null) {
				this.rs2.close();
			}
			if (this.st2 != null) {
				this.st2.close();
			}

			if (this.con != null) {
				this.con.close();
			}

		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(UsageDataCsv.class.getName());
			lgr.log(Level.WARNING, ex.getMessage(), ex);
		}	
	}

   /**
	* Business Requirement : 
	* Produce a Usage Data CSV file for FTP export to ExactTarget
	*
	* File Name: Usage_Data.csv
	*
	* File Format		
	* 	Column Header Name	Data Type	Nullable	Sample Data : Source of Data
	* 	DateKey*	        Number	           Y	1			: 1-90
	* 	PeriodDate	        Date	           Y	4/10/2012	: MySQL/powerplay View "mw_usage_cust_data.PeriodDate"
	* 	Usage	            Decimal	           Y	30.24		: SMT_Summary DynamoDB table
	* 	AvgTemp	            Decimal	           Y	85			: MySQL/powerplay View "mw_usage_cust_data.AvgTemp"
	*	PowerPlayID	        Number	           N	111222333	: MySQL/powerplay View "mw_usage_cust_data.PowerPlayID"
	*/

	public static void main(String[] args) {
		UsageDataCsv udc = new UsageDataCsv(2012, 3, 1);

		udc.connectToCloud();
		try {
			/**
			 * Step I)
			 */
			udc.getCustomers();
			/**
			 * Step II)
			 */
			udc.populateSummaryTable();
			/**
			 * Step III) After summary data is populated in Step II
			 *
			 *	udc.getCustWeather();
			 *	udc.mergeWeather();
			 *
			 * Step IV) Generate CSV File from DynamoDB table
			 *
			 *	etlMain.UsageCSV.saveSummaryToFile(String, String, String)
			 */
		} catch (SQLException e) {
			System.out.println("UsageDataCSV:main() program failed");
			e.printStackTrace();
		} finally {	
			udc.closeAll();
		}
	}
}
