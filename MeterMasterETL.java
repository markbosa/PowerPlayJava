package etlMain;

import java.io.IOException;
import java.util.Map;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;

public class MeterMasterETL {

	private int lineNumber = 0;
	protected int estimatedCnt = 0;
	protected int actualCnt = 0;
	protected int otherCnt = 0;
	private static KeySchemaElement hashKeyElement = null;
	private static KeySchemaElement rangeKeyElement = null;
    private static KeySchema compositeKeySchema = null;
    protected static AmazonDynamoDBUtils dbUtil = null;
    private FileUtils fileUtil = null;
	private DAOMeterData daoMeterData = null;
	private DTOMeterData dtoMeterData = null;
	
	// Meter Attribute variables are defined in DTOMeterData.java 	
	
	private void init() throws Exception {
        // Define the primary Hash Key portion of composite Key
        hashKeyElement = new KeySchemaElement();
        hashKeyElement.setAttributeName("ESIID");
        hashKeyElement.setAttributeType("S");
        
        // Define the Range Key portion of the composite Key
        rangeKeyElement = new KeySchemaElement();
        rangeKeyElement.setAttributeName("USAGE_DATE_TIME");
        rangeKeyElement.setAttributeType("S");
        
        // Define the composite key from two elements
        compositeKeySchema = new KeySchema();
        compositeKeySchema.setHashKeyElement(hashKeyElement);
        compositeKeySchema.setRangeKeyElement(rangeKeyElement);	           

        // Create one instance of each for every instance of this class.
        dbUtil 			= new AmazonDynamoDBUtils();
        fileUtil			= new FileUtils();
        daoMeterData 	= new DAOMeterData();
	}
	
	public static void main(String[] args) throws Exception {
		boolean rc = false;
		/**
		 * parse command line arguments
		 *   args(0) = 0 = TBD
		 *   args(0) = 1 = TBD
		 */
	       for (String s: args) {
	            System.out.println(s);
		}
		
		MeterMasterETL mainETL = new MeterMasterETL();
		mainETL.init();
		
		/**
		 * Set file name access parameters
		 */
//		mainETL.fileUtil.setFileName("IntervalMeterUsage1.CSV");
		mainETL.fileUtil.setFileName("WSilliman5-24-12.csv");

		/**
		 * Set the File directory path
		 */
		if(mainETL.fileUtil.getOperatingSystem() == FileUtils.LINUX )
			mainETL.fileUtil.setFilePathName("~\\AMI\\MeterData\\");
		else
			mainETL.fileUtil.setFilePathName("C:\\Dev\\AMI\\MeterData\\");
		/**
		 * Open the file for read access
		 */
		rc = mainETL.fileUtil.openFile();	            		
		/**
		 * Prove that the file has data - for DEBUG only; remove later
		 */
		// mainETL.dumpMeterFileData();

		if(rc != true) {
			System.out.println("Data file not found - program exiting!");
			System.exit(1);
		}
		
		/**
		 * AWS Interface starts here..
		 */
		try {
			dbUtil.setTableName("SMT_Meter_Data");
			TableDescription tableDesc = null;

			/**
			 * See if the table name requested exists as a DynamodDB database on AWS
			 */
			tableDesc = dbUtil.getTableIfExists(dbUtil.getTableName());
			String tableStatus = tableDesc.getTableStatus();

			/**
			 * If table is defined, use it, otherwise create a new DynamoDB table
			 */
			if (tableStatus.equals(TableStatus.ACTIVE.toString())) { 
				System.out.println("DynamoDB table state: " + tableStatus);
			}
			else {
				// Create a table with a composite key created above; add provisioning 
				CreateTableRequest createTableRequest = 
						new CreateTableRequest().withTableName(dbUtil.getTableName())
						.withKeySchema(compositeKeySchema)
						.withProvisionedThroughput(
								new ProvisionedThroughput()
								.withReadCapacityUnits(7L)
								.withWriteCapacityUnits(7L));


				tableDesc = dbUtil.getDynamoDB().createTable(createTableRequest).getTableDescription();
				System.out.println("Created DynamoDB Table: " + tableDesc);
			}

			// Wait for it to become active
			dbUtil.waitForTableToBecomeAvailable(dbUtil.getTableName());

			// Describe our new table
			DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(dbUtil.getTableName());
			TableDescription tableDescription = dbUtil.getDynamoDB().describeTable(describeTableRequest).getTable();
			System.out.println("Table Description: " + tableDescription);	            


			// reload = TRUE;  Run ETL load program, else test the search & scanning
			boolean reload = true;

			if(reload) {
				mainETL.processMeterInput(dbUtil.getTableName());
				/**
				 * close the file stream unconditionally
				 */
				mainETL.fileUtil.closeFileStream();

				System.out.println(
						"    Actual Count: " + mainETL.actualCnt +
						" Estimated Count: " + mainETL.estimatedCnt +
						"     Other Count: " + mainETL.otherCnt );
				System.out.println("ETL Loaded " + mainETL.lineNumber + 
						" records into " + mainETL.fileUtil.getInFile());
			}
			else {

				MeterSearchClient queryMaster = new MeterSearchClient();
				queryMaster.query.ExecuteScanSearch(dbUtil.getTableName());
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
		}
		/**
		 * Be responsible and close our file stream unconditionally
		 */
		mainETL.fileUtil.closeFileStream();

		System.out.println(
				"    Actual Count: " + mainETL.actualCnt +
				" Estimated Count: " + mainETL.estimatedCnt +
				"     Other Count: " + mainETL.otherCnt );
		System.out.println("ETL Loaded " + mainETL.lineNumber + 
				" records into " + mainETL.fileUtil.getInFile());
	}
	  
	  /**
	   * Main iterator through the meter data which 
	   * 1) Reads data from persistent source (flat file, HTTP stream, etc.)
	   * 2) Performs simple load into AWS / DynamoDB database
	   * 3) There is no TRANSLATION phase here as that will be done in DynamoDB
	   * @param nextMeterr
	   * @return
	   */  
	  public boolean processMeterInput(String dynamoTableName) {
		  boolean rc = false;
		  String strLine;
		  
		  this.lineNumber = 0;
		  //Read File Line By Line
		  try {
			  while ((strLine = this.fileUtil.getBufferedInReader().readLine()) != null)   {
				  // Print the content on the console
				  if((lineNumber % DAOMeterData.DOA_DAYPARTS) == 0) 
					  System.out.println (strLine);

				  /**
				   * Instantiate one meter record instance
				   */
				  DTOMeterData custMeter = new DTOMeterData(); 
				  custMeter.processMeter(custMeter, strLine, otherCnt, actualCnt, estimatedCnt, lineNumber);

				  /**
				   * Add the DTO data to the Cloud
				   */		
				  Map<String, AttributeValue> item = DAOMeterData.newMeterUsageItem( custMeter );
				  PutItemRequest putItemRequest = new PutItemRequest(dynamoTableName, item);
				  PutItemResult putItemResult = dbUtil.getDynamoDB().putItem(putItemRequest);
				  
				  // print out only 1 debug statement per day
				  if((lineNumber % DAOMeterData.DOA_DAYPARTS) == 0) 
					  System.out.println("Line:" + lineNumber +" Result: " + putItemResult);

				  lineNumber++;
			  }
			  rc = true;
		  } catch (IOException e) {
			  System.out.println ("IOException reading line: " + lineNumber 
					  			+ " in file: " + this.fileUtil.getInFile());
			  e.printStackTrace();
		  }
		  return rc;
	  }
		  
	    
	  private void dumpMeterFileData() {			   
		  String strLine;
		  //Read File Line By Line
		  try {
			  while ((strLine = this.fileUtil.getBufferedInReader().readLine()) != null)   {
				  // Print the content on the console
				  System.out.println (strLine);

				  /**
				   * Instantiate one meter record instance
				   */
				  DTOMeterData custMeter = new DTOMeterData();
				  custMeter.processMeter(custMeter, strLine, otherCnt, actualCnt, estimatedCnt, lineNumber);
			  }
		  } catch (IOException e) {
			  System.out.println ("IOException reading file: " + this.fileUtil.getFileName());
			  e.printStackTrace();
		  }
	  }
}
