package etlMain;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;

/**
 * MeterSearchClient
 * 
 * This class illustrates the higher level process of building a query
 * against the Meter Database.   This serves as a way to define a common
 * interface for all meter queries.  
 * @author Mark Bosakowski
 *
 */

public class MeterSearchClient {

    protected static AmazonDynamoDBUtils dbUtil = null;
	int spCnt = 5;  // Number of parameters in search parm array
	String searchParams[] = new String[spCnt];
	MeterSearch query = new MeterSearch();

	MeterSearchClient() {
        // Create one instance of each for every instance of this class.
        dbUtil = new AmazonDynamoDBUtils();
	};

	/**
	 * Set Parameter list based on the search type
	 * @return 
	 *
	 */

void testSearchType(int searchType) {	

	switch(searchType) { 
	
	case MeterSearch.NO_SEARCH:
		break;
	case MeterSearch.SEARCH_BY_ESIID_DATE:
		//	Returns all items with DATE in Range Key (i.e."2012-01-12")
		this.query.setSearchType(MeterSearch.SEARCH_BY_ESIID_DATE);
		searchParams[0] = "1/12/2012";
		searchParams[1] = "end";
		query.setSearchParameters(searchParams);
		query.ExecuteScanSearch(dbUtil.getTableName());

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
}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MeterSearchClient searchClient = new MeterSearchClient();
		
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


				// Wait for it to become active
				dbUtil.waitForTableToBecomeAvailable(dbUtil.getTableName());

				// Describe our new table
				DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(dbUtil.getTableName());
				TableDescription tableDescription = dbUtil.getDynamoDB().describeTable(describeTableRequest).getTable();
				System.out.println("Table Description: " + tableDescription);	        
				
				
				/**
				 * Now that we have an AWS connection, run the tests.
				 */
				int i = 0;
				/**
				 * Unit Test:  Verify all search types are operational 
				 * loop through all 8 search types vi test harness.
				 */
				for(i=0; i <= 7; i++){
					searchClient.testSearchType(i);
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

	}

}
