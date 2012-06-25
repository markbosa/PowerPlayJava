package etlMain;

import java.util.Map;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.QueryResult;

/**
 * A utility class that reads from an Amazon table and outputs the very
 * same data to a CSV file.  As AWS Items are ready serially, they are
 * loaded into the DTO object and the CSV line item write is performed
 * immediately. 
 */

public class UsageCSV {
	private int lineNumber = 0;
	protected static AmazonDynamoDBUtils dbUtil = null;
	private FileUtils fileUtils = null;
	private DAODailySum daoDailySum  = null;
	private DTODailySum dtoDailySum = null;

	// Usage Summary Attribute variables are defined in DTODailySum.java 	

	UsageCSV() throws Exception {
		init();
	}
	/**
	 *  init(): Create AWS input and CSV output adapters
	 */
	private void init() throws Exception {
		daoDailySum = new DAODailySum();
		daoDailySum.init("SMT_Daily_Summary");
		
		dtoDailySum = daoDailySum.dtoDailySum;
		
		fileUtils = new FileUtils();
	}

	/**
	 * Set DTO data access parameters
	 */
	public boolean openUsageFile() {
		boolean rc = false;
	/**
	 * Set file name
	 */
	this.fileUtils.setFileName("Usage.CSV");

	/**
	 * Set the File directory path
	 */
	if(this.fileUtils.getOperatingSystem() == FileUtils.LINUX )
		this.fileUtils.setFilePathName("~\\AMI\\ExactTarget\\");
	else
		this.fileUtils.setFilePathName("C:\\Dev\\AMI\\ExactTarget\\");
	
	/**
	 * Open the file for write access
	 */
	rc = this.fileUtils.openFileWrite();	           		
	/**
	 * Prove that the file has data - for DEBUG only; remove later
	 */
	//this.dumpCSVFile();

	if(rc != true) {
		System.out.println("Data file not found - program exiting!");
		System.exit(1);
	}
	return true;
}

	/**
	 * Main iterator through the usage summary data which 
	 * 1) Reads data from persistent source - SMT_Daily_Summary 
	 * 2) Performs load from AWS / DynamoDB database into DTODailySum object
	 * 3) Append each new item to a CSV file
	 * @param esiid 
	 * @param startDate 
	 * @param endDate 
	 * @return
	 */  
	public boolean saveSummaryToFile(String esiid, String startDate, String endDate) {
		boolean rc = false;
		String header;
		
		/**
		 * create a data access object to read from the SMT_Daily_Summary table
		 */
		this.lineNumber = 0;
		this.daoDailySum.connectToTable();
		this.daoDailySum.setQueryDebug(true);
		
		/**
		 * Execute the query and get the result set
		 */
		this.daoDailySum.ExecuteQuery(esiid, startDate, endDate);
		
		QueryResult qResult = this.daoDailySum.getQueryResult();

		/**
		 * loop through list converting result set data to a CSV formatted
		 * line of output.
		 */
		for (Map<String, AttributeValue> item : qResult.getItems()) {
			saveSummaryToDto(item);

			/**
			 * Print Header only on first line
			 */
			if(lineNumber == 0) {
				header = "DateKey, PeriodDate, Usage, AvgTemp, PowerPlayID";
				this.fileUtils.writeToFile(header);
				}
			/**
			 * Send CSV formatted line to file
			 */
			buildUsageCsvLine(lineNumber);
			
			this.fileUtils.writeToFile(buildUsageCsvLine(lineNumber++));
		}

//		this.dumpCSVFile();
		
		this.fileUtils.closeBufferedWriter();

		return rc;
	}

	
	private boolean saveSummaryToDto(Map<String, AttributeValue> attributeList) {
		boolean rc = false;

		for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
			String attributeName = item.getKey();
			AttributeValue value = item.getValue();

			if(attributeName.compareTo("ESIID") == 0) {
				dtoDailySum.setEsiid(value.getS());
			}
			else if(attributeName.compareTo("PERIOD_DATE") == 0) {
				dtoDailySum.setPeriodDate(value.getS());
			}
			else if(attributeName.compareTo("AVG_TEMP") == 0) {
				dtoDailySum.setAvgTemp(value.getS());
			}
			else if(attributeName.compareTo("DATE_KEY") == 0) {
				dtoDailySum.setDateKey(value.getS());
			}
			else if(attributeName.compareTo("USAGE") == 0) {
				dtoDailySum.setUsage(value.getS());
			}
			else if(attributeName.compareTo("POWER_PLAY_ID") == 0) {
				dtoDailySum.setPowerPlayId(value.getS());
			}
			else 
				System.out.println(attributeName
						+ " "
						+ (value.getS()  == null ? "" :  "S=[" + value.getS() +  "]")
						+ (value.getN()  == null ? "" :  "N=[" + value.getN() +  "]")
						+ (value.getSS() == null ? "" : "SS=[" + value.getSS() + "]")
						+ (value.getNS() == null ? "" : "NS=[" + value.getNS() + "] \n"));
		}
			
		rc = true;
		
		return rc;
	}

	/**
	 * buildCsvLine - construct a line of CSV formatted output 
	 * 
	 * @param lineNumber - Generate the DATE_KEY field 
	 * @return A Comma separated string in the format for the USAGE report to
	 * 			for FTP to ExactTarget.
	 */
	private String buildUsageCsvLine(int lineNumber) {

		String csvOut = 
//			Integer.toString(lineNumber + 1)+ ", " +
			dtoDailySum.getDateKey()		+ ", " +
			dtoDailySum.getPeriodDate()		+ ", " +
			dtoDailySum.getUsage()			+ ", " +
			dtoDailySum.getAvgTemp()		+ ", " +
			dtoDailySum.getPowerPlayId();
		return csvOut;
	}


	public static void main(String[] args) throws Exception {
		/**
		 * parse command line arguments
		 *   args(0) = 0 = TBD
		 *   args(0) = 1 = TBD
		 */
		for (String s: args) {
			System.out.println(s);
		}

		UsageCSV usageCsv = new UsageCSV();
		usageCsv.init();

		/**
		 * close our file stream unconditionally
		 */
		usageCsv.fileUtils.closeFileStream();

		System.out.println("CSV Saved " + usageCsv.lineNumber + 
				" records into " + usageCsv.fileUtils.getInFile());
	}
}
