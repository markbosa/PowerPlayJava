/**
 * MeterSearch.java - A class that encapsulates the logic to query the 
 * SMT_Meter_Table using various search methods.  This class MUST be
 * type-safe and reentrant as it will be run in parallel as a map reduce job.
 * 
 * @author Mark Bosakowski
 * @date 6/12/2012
 */
package etlMain;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;

public class MeterSearch {
	/**
	 * Define the search types
	 */
	public static final int NO_SEARCH                              = 0;
	public static final int SEARCH_BY_ESIID_DATE                   = 1;
	public static final int SEARCH_BY_ESIID_DATE_RANGE             = 2;
	public static final int SEARCH_BY_ESIID_DATE_DAYPART           = 3;
	public static final int SEARCH_BY_ESIID_DATE_DAYPART_RANGE     = 4;
	public static final int SEARCH_BY_ESIID_DATE_DAYPART_MIN       = 5;
	public static final int SEARCH_BY_ESIID_DATE_DAYPART_MAX       = 6;
	public static final int SEARCH_BY_ESIID_DATE_DAYPART_KWH_RANGE = 7;

	private boolean queryDebug = false;
	private int searchType;
	private String[] searchParams = new String[5];
	private ScanResult scanResult = null;
	private QueryResult queryResult = null;

	MeterSearch() {
	}
	
	/**
	 * Search Types are enumerated in MeterSearchTypeInf
	 */
	public boolean setSearchType(int searchType) {
		this.searchType = searchType; 
		return true;
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
	 * Search parameters are a client-server application agreement that both
	 * the number of parameters and their order are fixed.  
	 * 
	 * Searches with a BETWEEN may have several parameters 
	 * 
	 * @param searchParms
	 * @return
	 */
	public int setSearchParameters(String[] searchParms) {
		int i;

		for(i = 0; i < searchParms.length; i++){
			/**
			 * "end" is used as sentinel record which indicates
			 * that there are no more parameters.
			 */
			if(searchParms[i].compareTo("end") == 0)
				break;
			else
				this.searchParams[i] = searchParms[i];
		}
		return i;
	}

	public String getSearchParameter(int index) {
		return this.searchParams[index];
	}
	
	public int setScanResults(ScanResult newScanR) {
		this.scanResult = newScanR;
		return this.scanResult.getCount();
	}

	public List<Map<String, AttributeValue>> getScanResults() {

		if(this.isQueryDebug() == true) {
			System.out.println("Scan Results: ConsumedCapcityUnits:" + this.scanResult.getConsumedCapacityUnits());
			System.out.println("Scan Results:                Count:" + this.scanResult.getCount());
			System.out.println("Scan Results:         ScannedCount:" + this.scanResult.getScannedCount());
			System.out.println("Scan Results:   Last Evaluated Key:" + this.scanResult.getLastEvaluatedKey());
			System.out.println("Scan Results:                Items:" + this.scanResult.getItems());
		}
		return this.scanResult.getItems();
	}

	public boolean ExecuteScanSearch(String targetTable) {

		HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();

		switch(this.searchType) { 
		case SEARCH_BY_ESIID_DATE:
			//	Returns all items with DATE in Range Key (i.e."2012-01-12")
			Condition condition1 = new Condition()
			.withComparisonOperator(ComparisonOperator.BEGINS_WITH.toString())
			.withAttributeValueList(new AttributeValue().withS(this.getSearchParameter(0)));
			scanFilter.put("USAGE_DATE_TIME", condition1);
			break;
		case SEARCH_BY_ESIID_DATE_RANGE:
			/**
				Returns all items in a RANGE from Day 1 to Day 2 
			    Caller MUST set TWO parameters:
			     Parameter 1 - START DATE (i.e."2012-01-12")
			     Parameter 2 - END DATE   (i.e."2012-01-18")
			 */
			Condition condition2 = new Condition()
			.withComparisonOperator(ComparisonOperator.BETWEEN.toString())
			.withAttributeValueList(new AttributeValue()
			.withSS(this.getSearchParameter(0), this.getSearchParameter(1))); 
			scanFilter.put("USAGE_DATE", condition2);
			break;
		case SEARCH_BY_ESIID_DATE_DAYPART:
			Condition condition3 = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(new AttributeValue().withS("2012-01-12"));
			scanFilter.put("USAGE_DATE", condition3);
			break;
		case SEARCH_BY_ESIID_DATE_DAYPART_RANGE:
			Condition condition4 = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(new AttributeValue().withS("2012-01-12"));
			scanFilter.put("USAGE_DATE", condition4);
			break;
		case SEARCH_BY_ESIID_DATE_DAYPART_MIN:
			Condition condition5 = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(new AttributeValue().withS("2012-01-12"));
			scanFilter.put("USAGE_DATE", condition5);
			break;
		case SEARCH_BY_ESIID_DATE_DAYPART_MAX:
			Condition condition6 = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(new AttributeValue().withS("2012-01-12"));
			scanFilter.put("USAGE_DATE", condition6);
			break;
		case SEARCH_BY_ESIID_DATE_DAYPART_KWH_RANGE:
			Condition condition7 = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(new AttributeValue().withS("2012-01-12"));
			scanFilter.put("USAGE_DATE", condition7);
			break;
		default:
			break;
		}

		ScanRequest scanRequest = new ScanRequest(targetTable).withScanFilter(scanFilter);
//		this.scanResult = MeterSearchClient.dbUtil.getDynamoDB().scan(scanRequest);
		;
		this.setScanResults(LookBackEngine.dbUtilInput.getDynamoDB().scan(scanRequest));
		System.out.println("Result: " + scanResult);
		return false;
	}
	
	
	/**
	 * ================= Query Search Methods =================================
	 * 
	 *  ExecuteQuery()
	 *  
	 *  Generate the search conditions by search type then use a common 
	 *  execute query command to get results.  Please note that for EACH search
	 *  type, you MUST set the search parameters BEFORE this method is called.
	 *  Example: For SEARCH_BY_ESIID, the first parameter is the customers
	 *  ESIID which is the hash key value for the table.  The Range Key is in
	 *  the second parameter and specifies which DATE to search on.  
	 *  The 
	 *  
	 * @param targetTable
	 * @return A query result set
	 */
	public boolean ExecuteQuery(String targetTable) {
		boolean rc = false;
		HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();

		switch(this.searchType) { 
		case SEARCH_BY_ESIID_DATE:
			/**
			 * Returns all items with DATE in Range Key (i.e."2012-01-12")
			 * Search Param[0] - The ESIID search key value 
			 * Search Param[1] - The USAGE_DATE_TIME range search key value
			*/
			Condition rangeKeyCondition = new Condition()
		    .withComparisonOperator(ComparisonOperator.BEGINS_WITH.toString())
		    .withAttributeValueList(new AttributeValue().withS(this.getSearchParameter(1)));		

			QueryRequest queryRequest = new QueryRequest()
		    .withTableName(targetTable)
//		    .withHashKeyValue(new AttributeValue().withS("Amazon DynamoDB#DynamoDB ESIID " + this.getSearchParameter(0) ))
		    .withHashKeyValue(new AttributeValue().withS(this.getSearchParameter(0) ))
		    .withAttributesToGet(Arrays.asList("USAGE_KWH", "USAGE_DATE", "USAGE_START_TIME"))
		    .withRangeKeyCondition(rangeKeyCondition)
		    .withConsistentRead(true);
			
			this.setQueryResult( LookBackEngine.dbUtilInput.getDynamoDB().query(queryRequest)); 
//			for (Map<String, AttributeValue> item : this.getQueryResult().getItems()){
//				System.out.println("Result: " + item.toString());
//			}
			rc = true;

			break;
		case SEARCH_BY_ESIID_DATE_RANGE:
			/**
				Returns all items in a RANGE from Day 1 to Day 2 
			    Caller MUST set TWO parameters:
			     Parameter 1 - START DATE (i.e."2012-01-12")
			     Parameter 2 - END DATE   (i.e."2012-01-18")
			 */
			Condition condition2 = new Condition()
			.withComparisonOperator(ComparisonOperator.BETWEEN.toString())
			.withAttributeValueList(new AttributeValue()
			.withSS(this.getSearchParameter(0), this.getSearchParameter(1))); 
			scanFilter.put("USAGE_DATE", condition2);
			break;
		case SEARCH_BY_ESIID_DATE_DAYPART:
			Condition condition3 = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(new AttributeValue().withS("2012-01-12"));
			scanFilter.put("USAGE_DATE", condition3);
			break;
		case SEARCH_BY_ESIID_DATE_DAYPART_RANGE:
			Condition condition4 = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(new AttributeValue().withS("2012-01-12"));
			scanFilter.put("USAGE_DATE", condition4);
			break;
		case SEARCH_BY_ESIID_DATE_DAYPART_MIN:
			Condition condition5 = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(new AttributeValue().withS("2012-01-12"));
			scanFilter.put("USAGE_DATE", condition5);
			break;
		case SEARCH_BY_ESIID_DATE_DAYPART_MAX:
			Condition condition6 = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(new AttributeValue().withS("2012-01-12"));
			scanFilter.put("USAGE_DATE", condition6);
			break;
		case SEARCH_BY_ESIID_DATE_DAYPART_KWH_RANGE:
			Condition condition7 = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(new AttributeValue().withS("2012-01-12"));
			scanFilter.put("USAGE_DATE", condition7);
			break;
		default:
			break;
		}

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
}
