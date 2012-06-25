/**
 * 
 */
package etlMain;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeAction;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ReturnValue;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemResult;

/**
 * DAO (Data Access Object) for reading and writing to the DynamodDB
 * SMT_Daily_Summary table
 * 
 * @author Mark Bosakowski  
 */
public class DAODailySum {
	public static int DOA_DAYPARTS = 96;
	private int lineNumber = 0;
	private AmazonDynamoDBUtils ldbUtil = null;
	private static KeySchemaElement hashKeyElement = null;
	private static KeySchemaElement rangeKeyElement = null;
	private static KeySchema compositeKeySchema = null;
	protected AmazonDynamoDBUtils dbUtil = null;
	protected DTODailySum dtoDailySum = null;
	private boolean queryDebug = false;
	private QueryResult queryResult = null;


	DAODailySum() {
	}
	
	/**
	 *  Meter Attribute variables are defined in dtoDailySum.java 	
	 */
	protected void init(String tableName) throws Exception {
		// Define the primary Hash Key portion of composite Key
		hashKeyElement = new KeySchemaElement();
		hashKeyElement.setAttributeName("ESIID");
		hashKeyElement.setAttributeType("S");

		// Define the Range Key portion of the composite Key
		rangeKeyElement = new KeySchemaElement();
		rangeKeyElement.setAttributeName("PERIOD_DATE");
		rangeKeyElement.setAttributeType("S");

		// Define the composite key from two elements
		compositeKeySchema = new KeySchema();
		compositeKeySchema.setHashKeyElement(hashKeyElement);
		compositeKeySchema.setRangeKeyElement(rangeKeyElement);	     

		// Create one instance of each for every instance of this class.
		dbUtil = new AmazonDynamoDBUtils();
		dbUtil.setTableName(tableName);
		dbUtil.setCompositeKeySchema(compositeKeySchema);

		// Save table schema key and connection info
		this.setDbUtil(dbUtil);		
		
		// Instantiate one meter record instance to transfer data
		this.dtoDailySum = new DTODailySum();
	}
		
	/*************** AWS DynamoDB binding starts here *************************/
	
	/**
	 * set the local dbUtil instance as we may have MANY simultaneous 
	 * instances with map-reduce jobs
	 * @param newDbUtil
	 */
	public void setDbUtil(AmazonDynamoDBUtils newDbUtil) {
		this.ldbUtil = newDbUtil;
	}
	
	/**
	 * get the local dbUtil instance as we may have MANY simultaneous 
	 * instances with map-reduce jobs
	 * @param newDbUtil
	 */
	public AmazonDynamoDBUtils getDbUtil() {
		return this.ldbUtil;
	}
	
	public boolean connectToTable( ) {	

		this.ldbUtil.setTableName("SMT_Daily_Summary");
		TableDescription tableDesc = null;

		/**
		 * See if the table name requested exists as a DynamodDB database on AWS
		 */
		tableDesc = ldbUtil.getTableIfExists(ldbUtil.getTableName());
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
					new CreateTableRequest().withTableName(ldbUtil.getTableName())
					.withKeySchema(ldbUtil.getCompositeKeySchema())
					.withProvisionedThroughput(
							new ProvisionedThroughput()
							.withReadCapacityUnits(7L)
							.withWriteCapacityUnits(7L));

			tableDesc = ldbUtil.getDynamoDB().createTable(createTableRequest).getTableDescription();
			System.out.println("Created DynamoDB Table: " + tableDesc);
		}

		// Wait for it to become active
		ldbUtil.waitForTableToBecomeAvailable(ldbUtil.getTableName());

		// Describe our new table
		DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(ldbUtil.getTableName());
		TableDescription tableDescription = ldbUtil.getDynamoDB().describeTable(describeTableRequest).getTable();
		System.out.println("Table Description: " + tableDescription);	            
		return true;
	}
	/**
	 * putDailySumItem - Write a new DynamoDB Item to the table
	 * @param strLine - A CSV formatted string with Item values
	 * @return true if successful
	 */  
	public boolean putDailySumItem(String strLine) {
		boolean rc = false;
		int lineNumber = 0;

		// Print the content on the console
		//	if((lineNumber % DAODailySum.DOA_DAYPARTS) == 0) 
		System.out.println("CSV Line:" + strLine);

		try {
			this.dtoDailySum.saveDtoDailySum(strLine);
			/**
			 * Add the DTO data to the Cloud
			 * 
			 * newMeterUsageItem is initialized & populated with DTO instance data
			 */		
			Map<String, AttributeValue> item = DAODailySum.newMeterUsageItem( this.dtoDailySum );
			PutItemRequest putItemRequest = new PutItemRequest(dbUtil.getTableName(), item);
			PutItemResult putItemResult = dbUtil.getDynamoDB().putItem(putItemRequest);

			// print out only 1 debug statement per day
			if((lineNumber % DAODailySum.DOA_DAYPARTS) == 0) 
				System.out.println("Line:" + lineNumber +" Result: " + putItemResult);

			lineNumber++;

			rc = true;
		} catch (IOException e) {
			System.out.println("Line:" + lineNumber +" putDailySumItem Exception:  [" + e + "]");
			e.printStackTrace();
		}	
		return rc;
	}
	

	
	/**
	 *  updateWeatherAttribute - Updates the avgTemp value in an existing item
	 *  
	 * @param newAvgTemp 	- From MySQL database
	 * @param esiid 		- primary key for table
	 * @param dateKey		- range key for table 
	 * @return				- success or failure
	 */  
	public void updateWeatherAttribute(String newAvgTemp, String esiid, String dateKey) {
		/**
		 * Use current meter record instance
		 */
		this.dtoDailySum.setAvgTemp(newAvgTemp);
		this.dtoDailySum.setEsiid(esiid);
		this.dtoDailySum.setDateKey(dateKey);
	
		/**
		 * Add the DTO data item to the Cloud
		 * 
		 * Item is initialized & populated with current DTO instance data
		 */		
		Map<String, AttributeValue> item = DAODailySum.newMeterUsageItem( this.dtoDailySum );

		Key key = new Key().withHashKeyElement(new AttributeValue().withS(esiid))
							.withRangeKeyElement(new AttributeValue().withS(dateKey));

		// update items DynamoDB
		Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();

		updateItems.put("AVG_TEMP",
				new AttributeValueUpdate().withAction(AttributeAction.PUT)
											.withValue(item.get("AVG_TEMP")));
		// set updateItemRequest
		UpdateItemRequest updateItemRequest = 
							new UpdateItemRequest()
									.withTableName(dbUtil.getTableName())
									.withKey(key)
									.withReturnValues(ReturnValue.UPDATED_OLD)
									.withAttributeUpdates(updateItems);
		// update item 
		UpdateItemResult  updateItemResult  = dbUtil.getDynamoDB().updateItem(updateItemRequest);

		// print out only 1 debug statement per week
		if((lineNumber++ % 7) == 0)  {
			System.out.println("New AvgTemp:" + newAvgTemp + " ESIID: " + esiid + " DateKey: " + dateKey);
			System.out.println("Line:" + lineNumber +" Result: " + updateItemResult);
		}
	}
	/**
	 * ExecuteQuery() - Custom query mapped to SMT_Daily_Summary table
	 * @param esiid		- Primary Hash Key
	 * @param startDate	- Between Range Keys - set query conditions
	 * @param endDate
	 * @return - sets the private "queryResult" variable which MUST be accessed
	 * 			 by the caller to retrieve data.
	 */
	public boolean ExecuteQuery(String esiid, String startDate, String endDate) {
		boolean rc = false;

		/**
		 * BETWEEN in AWS requires the first condition to be LESS THAN the 2nd
		 * Since the look-back-engine goes back in time, set the first 
		 * parameter to the end date and move up the calendar
		 */
		Condition rangeKeyCondition = new Condition()
				.withComparisonOperator(ComparisonOperator.BETWEEN.toString())
				.withAttributeValueList(new AttributeValue().withS(endDate),
										new AttributeValue().withS(startDate));	
		
		QueryRequest queryRequest = new QueryRequest()
				.withTableName(this.dbUtil.getTableName())
				.withHashKeyValue(new AttributeValue().withS(esiid))
				.withAttributesToGet(Arrays.asList("ESIID", "PERIOD_DATE", "AVG_TEMP", "DATE_KEY", "POWER_PLAY_ID", "USAGE"))
				.withRangeKeyCondition(rangeKeyCondition)
				.withConsistentRead(true);

		this.setQueryResult( LookBackEngine.dbUtilInput.getDynamoDB().query(queryRequest));
		rc = true;
		return rc;
	}
	/**
	 * @return the queryResult
	 */
	public QueryResult getQueryResult() {

		if(this.isQueryDebug() == true) {
			System.out.println("Query Results: ConsumedCapcityUnits:" + this.queryResult.getConsumedCapacityUnits());
			System.out.println("Query Results:                Count:" + this.queryResult.getCount());
			System.out.println("Query Results:   Last Evaluated Key:" + this.queryResult.getLastEvaluatedKey());
			System.out.println("Query Results:                Items:" + this.queryResult.getItems());
		}
		return queryResult;
	}
	/**
	 * @param queryResult the queryResult to set
	 */
	public void setQueryResult(QueryResult queryResult) {
		this.queryResult = queryResult;
	}
	/**
	 * @return the queryDebug
	 */
	public boolean isQueryDebug() {
		return queryDebug;
	}

	/**
	 * @param queryDebug the queryDebug to set
	 */
	public void setQueryDebug(boolean queryDebug) {
		this.queryDebug = queryDebug;
	}

	/**
	 * newMeterUsageItem - Creates an AWS "item" required for CRUD
	 * 
	 * Usage: The DTO MUST be instantiated and populated BEFORE this method
	 * 			is called. Purpose is to create an item and set the DTO values
	 * 
	 * @param meterInfo - A blank instantiation of a DTO
	 * @return			- The newly instantiated and populated item
	 */
	static Map<String, AttributeValue> newMeterUsageItem( DTODailySum meterInfo )
	{
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		/**
		 *  ESIID is the primary key
		 *  PERIOD_DATE is the range key 
		 */ 
		item.put("ESIID", 				new AttributeValue(meterInfo.getEsiid()));
		item.put("PERIOD_DATE", 		new AttributeValue(meterInfo.getPeriodDate()));
		item.put("DATE_KEY", 			new AttributeValue(meterInfo.getDateKey()));
		item.put("USAGE", 				new AttributeValue(meterInfo.getUsage()));
		item.put("AVG_TEMP", 			new AttributeValue(meterInfo.getAvgTemp()));
		item.put("POWER_PLAY_ID",		new AttributeValue(meterInfo.getPowerPlayId()));
	
		return item;
	}
}
