package etlMain;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;

/**
 * LookBackEngine
 * 
 * This class implement the business rules for the curtailment process
 * The process will query the SMT_Meter_Data DynamodDB database and
 * output the results to another DynamoDB for subsequent processing
 * 
 *  Implement LookBackEngine: Average Day-Of-Week (DOW) Usage
 *    1) Select 1 DOW (Monday through Sunday)
 *    2) Set Start Date - Calculate Day of Year in numeric form (i.e. day 187)
 *    3) Set LBC (Look-back-count) (i.e. LW= Number of days to query)
 *    4) Set LBI (Look-back-increment) (# of days to search back:  7 = weekly)
 *    5) Set Customer Id (possibly in a loop of many customers)
 *    6) Sum All weeks for that customer for that day going back LW weeks
 *    7) Calculate the average usage in KWh for that customer Total KWh / LW
 *    8) Store new Item as Customer/Usage/Date in AverageDOWUsage table
 * @author Mark Bosakowski
 *
 */
public class LookBackEngine {
	// AWS Support variables
	protected static AmazonDynamoDBUtils dbUtilInput = null;

	// MeterSearch variables
	private MeterSearch query = new MeterSearch();
	private int spCnt = 5;  // Number of parameters in search parm array
	private String searchParams[] = new String[spCnt];
	private int searchType = MeterSearch.NO_SEARCH;

	// Instance Variables
	private String esiid = "";
	private String powerPlayId = "";
	private String dayOfWeek = "";   // Save as Strings for reporting
	private String startDate = "";
	private GregorianCalendar gCal = null;
	private int lookBackCount = 0; 
	private int lookBackIncrement = 0;
	private double SumKwhUsage = 0.0;
	// Create a very specific English-speaking, U.S. locale
	protected Locale locale = new Locale("en", "US", "Texas");
	public boolean debugDetail = true;  //false;
	
	// Constructor
	LookBackEngine() {
		// Create one instance of each for every instance of this class.
		dbUtilInput = new AmazonDynamoDBUtils();
	};

	/**
	 * Set Search Parameter list based on the search type
	 * @return 
	 *
	 * @return the searchType
	 */
	public int getSearchType() {
		return this.searchType;
	}

	/**
	 * @param searchType the searchType to set
	 */
	public void setSearchType(int searchType) {
		this.searchType = searchType;
	}

	/**
	 * @return the esiid
	 */
	public String getEsiid() {
		return this.esiid;
	}

	/**
	 * @param Set the ESIID - Primary Key value in SMT_Meter_Data
	 */
	public void setEsiid(String esiid) {
		this.esiid = esiid;
	}

	/**
	 * @return the powerPlayId
	 */
	public String getpowerPlayId() {
		return this.powerPlayId;
	}

	/**
	 * @param Set the powerPlayId - Primary Key value in SMT_Meter_Data
	 */
	public void setPowerPlayId(String powerPlayId) {
		this.powerPlayId = powerPlayId;
	}
	
	/**
	 * @return the dayOfWeek
	 */
	public String getDayOfWeek() {
		return this.dayOfWeek;
	}

	/**
	 * @param dayOfWeek the dayOfWeek to set
	 */
	public void setDayOfWeek(String dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	/**
	 * @return void - get the start Date as a String for reporting 
	 */
	public String getStartDate() {
		return this.startDate;
	}
	/**
	 * @return void - set the start Date as a String for reporting 
	 */
	public void setStartDate(String newStartDate) {
		this.startDate = newStartDate;
	}

	/**
	 * @param set the calendar to the startingDate 
	 */
	public GregorianCalendar getGregorianCalendar() {
		return this.gCal;
	}
	/**
	 * @param set the calendar to the startingDate 
	 */
	public void setGregorianCalendar(GregorianCalendar startGregorianCalendar) {
		this.gCal = startGregorianCalendar;
	}

	/**
	 * @return the lookBackCount
	 */
	public int getlookBackCount() {
		return this.lookBackCount;
	}

	/**
	 * @param lookBackCount the lookBackCount to set
	 */
	public void setlookBackCount(int lookBackCnt) {
		this.lookBackCount = lookBackCnt;
	}

	/**
	 * @return the lookBackIncrement
	 */
	public int getLookBackIncrement() {
		return lookBackIncrement;
	}

	/**
	 * @param lookBackIncrement the lookBackIncrement to set
	 */
	public void setLookBackIncrement(int lookBackIncrement) {
		this.lookBackIncrement = lookBackIncrement;
	}

	/**
	 * @return the sumKwhUsage
	 */
	public double getSumKwhUsage() {
		return SumKwhUsage;
	}

	/**
	 * @param sumKwhUsage the sumKwhUsage to set
	 */
	public void setSumKwhUsage(double sumKwhUsage) {
		SumKwhUsage = sumKwhUsage;
	}
	/**
	 * get the Day of the Year (1-365/6)
	 * based on the current Calendar setting
	 */
	public int getDayOfYear(String tDate) {
		System.out.println("getDayOfYear: " 
				+ getGregorianCalendar().get(Calendar.DAY_OF_YEAR));
		return getGregorianCalendar().get(Calendar.DAY_OF_YEAR); 
	}

	/**
	 * get the Day of the Year (1-365/6)
	 * based on the current Calendar setting
	 */
	public int getDayOfYear() {
		System.out.println("getDayOfYear: " 
				+ getGregorianCalendar().get(Calendar.DAY_OF_YEAR));
		return getGregorianCalendar().get(Calendar.DAY_OF_YEAR); 
	}

	/**
	 * @return the debugDetail
	 */
	public boolean isDebugDetail() {
		return debugDetail;
	}

	/**
	 * @param debugDetail the debugDetail to set
	 */
	public void setDebugDetail(boolean debugDetail) {
		this.debugDetail = debugDetail;
	}

	/**
	 * Primary business logic of look-back-engine	
	 * @return
	 */
	boolean lookBackAndSummarize() {
		boolean rc = false;

		/**
		 * AWS Interface starts here..
		 */
		try {
			dbUtilInput.setTableName("SMT_Meter_Data");
			TableDescription tableDesc = null;

			/**
			 * See if the table name requested exists as a DynamodDB database on AWS
			 */
			tableDesc = dbUtilInput.getTableIfExists(dbUtilInput.getTableName());
			String tableStatus = tableDesc.getTableStatus();

			/**
			 * If table is defined, use it, otherwise create a new DynamoDB table
			 */
			if (tableStatus.equals(TableStatus.ACTIVE.toString())) { 
				System.out.println("DynamoDB table state: " + tableStatus);
			}

			// Wait for it to become active
			dbUtilInput.waitForTableToBecomeAvailable(dbUtilInput.getTableName());

			// Describe our new table
			DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(dbUtilInput.getTableName());
			TableDescription tableDescription = dbUtilInput.getDynamoDB().describeTable(describeTableRequest).getTable();
			System.out.println("Table Description: " + tableDescription);	        


			/**
			 * Now that we have an AWS connection to meter data we set up the
			 * job parameters used in the search and scanning of the meter data
			 */
			String DATE_FORMAT = "MMddyyyy";
			SimpleDateFormat sdf =	new SimpleDateFormat(DATE_FORMAT);
			Calendar c1 = Calendar.getInstance(); // today
			System.out.println("Todays Date Cal: " + sdf.format(c1.getTime()));
			System.out.println("Target Day gCal: " + sdf.format(this.gCal.getTime()));

			//			String month = String.format("%1$tm", this.gCal);
			//			String day   = String.format("%1$te", this.gCal);
			//			String year  = String.format("%1$tY", this.gCal);	

			/**
			 * Open the output summary table using DynamoDB
			 */
			DAODailySum daoDailySum = new DAODailySum();
			daoDailySum.init("SMT_Daily_Summary");

			/**
			 * Set parametric loop starting conditions
			 */
			int dateKey = 1;
			int startDay = this.getDayOfYear(this.getStartDate());
			int backDay  = startDay - this.getLookBackIncrement();
			GregorianCalendar tmpCal = this.getGregorianCalendar();

			/**
			 * Loop through previous weeks or days and gather usage data
			 */
			for(dateKey = 1; dateKey <= this.getlookBackCount(); dateKey++){
				/**
				 * Generate string representation of previous back days
				 * This is a DATE transform operation
				 */
				tmpCal.set(Calendar.DAY_OF_YEAR, backDay);
				//	tmpCal.set(this.getGregorianCalendar().get, backDay);

				String month = String.format("%1$tm", tmpCal);
				String day   = String.format("%1$te", tmpCal);
				String year  = String.format("%1$tY", tmpCal);

				/**
				 * To match format of SMT data, we MUST strip out leading zeros
				 * on the Month and day fields.
				 */
				if(month.startsWith("0") == true) {
					month = month.substring(1);
				}

				if(day.startsWith("0") == true) {
					day = day.substring(1);
				}
				/**
				 * Format date string to match SMT data 
				 */
				String searchDate = month + "/"+ day + "/" + year;
				System.out.println("\nSearch back " + (dateKey) + " Increments of " +
								this.getLookBackIncrement() + 
								" Days setting searchDate to[" 
								+ searchDate + "]");

				/**
				 * Add flexibility to look-back engine by supporting multiple
				 * search types
				 */
				switch(this.getSearchType()) { 

				case MeterSearch.NO_SEARCH:
					break;
				case MeterSearch.SEARCH_BY_ESIID_DATE:
					// Search by ESIID and DATE
					// Returns all items with DATE in Range Key (i.e "1/12/2012")
					this.query.setSearchType(MeterSearch.SEARCH_BY_ESIID_DATE);
					this.query.setQueryDebug(false);
					this.searchParams[0] = this.getEsiid();
					this.searchParams[1] = searchDate;
					this.searchParams[2] = "end";
					this.query.setSearchParameters(this.searchParams);
					this.query.ExecuteQuery(dbUtilInput.getTableName());
					break;
				case MeterSearch.SEARCH_BY_ESIID_DATE_RANGE:
					/**
						Returns all items in a RANGE from Day 1 to Day 2 
					    Caller MUST set TWO parameters:
					     Parameter 1 - START DATE (i.e."2012-01-12")
					     Parameter 2 - END DATE   (i.e."2012-01-18")
					 */
					break;
				case MeterSearch.SEARCH_BY_ESIID_DATE_DAYPART:
					break;
				case MeterSearch.SEARCH_BY_ESIID_DATE_DAYPART_RANGE:
					break;
				case MeterSearch.SEARCH_BY_ESIID_DATE_DAYPART_MIN:
					break;
				case MeterSearch.SEARCH_BY_ESIID_DATE_DAYPART_MAX:
					break;
				case MeterSearch.SEARCH_BY_ESIID_DATE_DAYPART_KWH_RANGE:
					break;
				default:
					break;
				}	

				/**
				 * Go back in time one more time increment
				 */
				backDay = backDay - this.getLookBackIncrement();

				QueryResult qResult = this.query.getQueryResult();

				double kwhSum = 0.0;

				for (Map<String, AttributeValue> item : qResult.getItems()) {
					kwhSum += processItem(item, this.isDebugDetail());
				}

				/**
				 * Save the value
				 */
				this.setSumKwhUsage(kwhSum);
				System.out.println(
						"Date:" + searchDate + 
						" Total KWh hours: " +  
						Double.toString(this.getSumKwhUsage()));

				/**
				 * ETL data cleanup before storing data 

				 *    1) Pad single digit months and day with leading zero
				 *    2) Format year for data warehouse format YYYY/MM/DD
				 *    3) Set fixed output for daily usage to 3.3 decimal
				 */				
				if(month.length() == 1)
					month = "0" + month;
				if(day.length() == 1)
					day = "0" + day;

				String outputDate = year + "/" + month + "/" + day ;
				// System.out.println("Search back " + (dateKey) + " increment to day[" + outputDate + "]");

				DecimalFormat df = new DecimalFormat("###.###");
				
				String csvSum = this.getEsiid() + "," + 
								outputDate + "," +
								dateKey + "," +
								df.format(kwhSum) + "," +
								"-999"+ "," +
								this.getpowerPlayId();

				// System.out.println("csvSum=" + csvSum);
				/**
				 * Instantiates one meter DTO record instance and writes to AWS
				 */
				rc = daoDailySum.putDailySumItem(csvSum);
			}
		} catch (AmazonServiceException ase) {
			System.out.println(
					"Caught an AmazonServiceException, which means your request made it "
							+ "to AWS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println(
					"Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with AWS, "
							+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} catch (Exception e) {
			// Auto-generated catch block  - Catches daoDailySum.init()
			System.out.println("Caught a daoDailySum.init() Exception: " + e.getMessage());
			e.printStackTrace();
		}
		System.out.println("lookBackAndSummarize() Completed");

		return rc; 
	}  // end of lookBackAndSummarize() 



	private static double processItem(Map<String, AttributeValue> attributeList, boolean detailFlag) {
		double KWh = 0.0;
		String uDate = "";
		String uStartTime = "";

		for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
			String attributeName = item.getKey();
			AttributeValue value = item.getValue();

			if(attributeName.compareTo("USAGE_KWH") == 0) {
				KWh = Double.valueOf(value.getS());
			}
			else if(attributeName.compareTo("USAGE_DATE") == 0) {
				uDate = value.getS();
			}
			else if(attributeName.compareTo("USAGE_START_TIME") == 0) {
				uStartTime = value.getS();
			}
			else 
				System.out.println(attributeName
						+ " "
						+ (value.getS()  == null ? "" :  "S=[" + value.getS() +  "]")
						+ (value.getN()  == null ? "" :  "N=[" + value.getN() +  "]")
						+ (value.getSS() == null ? "" : "SS=[" + value.getSS() + "]")
						+ (value.getNS() == null ? "" : "NS=[" + value.getNS() + "] \n"));
		}
		if(detailFlag == true)
			System.out.println("Date:" + uDate + " Time: " + uStartTime + " KWh: " + KWh );

		return KWh;
	}

	/**
	 * @param args - TODO:  Add command line processor
	 */
	public static void main(String[] args) {
		boolean mrc = false;
		/**
		 *  Create an instance of this class for each customer 
		 *  Customer ID gets passed in as first argument
		 *  Target Date gets passed in as second argument 
		 *  
		 *  This allows for MapReduce jobs to spawn off tasks on 
		 */
		LookBackEngine lbe = new LookBackEngine();

		/**
		 * Set the Customer ID - The primary key used for searching
		 */
		lbe.setEsiid("10443720002592292");  // William Silliman - Sample Customer
		/**
		 * Set the PowerPlayID
		 */
		lbe.setPowerPlayId("0");
		/**
		 * Set the search type
		 */
		lbe.setSearchType(MeterSearch.SEARCH_BY_ESIID_DATE);
		/**
		 * Set the date to start calculations and also capture what
		 * Day-Of-The-Week that was for reporting purposes
		 * 
		 *  For now, hard code to end of sample data.  Later on we will
		 *  pass in the startDate as a command line argument so that we
		 *  will be able to run these small java jobs in parallel 
		 *  
		 *  Please note that the Gregorian Months start at zero, so you 
		 *  MUST subtract the input by 1 to get the correct month.
		 */
		lbe.gCal = new GregorianCalendar(2012,4,15);
		lbe.setDayOfWeek(lbe.gCal.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.LONG, lbe.locale));
		lbe.setGregorianCalendar(lbe.gCal);   // Save set value		

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
		 * Start the hot-tub time machine
		 */
		mrc = lbe.lookBackAndSummarize();
		
		System.out.println("Main Program Completed: " + ((mrc==true)?"True":"False"));

}	// End of main()

}

