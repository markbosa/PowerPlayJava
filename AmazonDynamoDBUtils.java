package etlMain;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class AmazonDynamoDBUtils {

    /*
     * Important: Be sure to fill in your AWS access credential's in the
     *            AwsCredentials.properties file before you try to run this
     *            sample.
     *            After the constructor is called you MUST supply an AWS 
     *     		  tableName with the setTableName(name) method.
     * http://aws.amazon.com/security-credentials
     */

    static AmazonDynamoDBClient dynamoDB;
    private String tableName = "";
	private static KeySchema compositeKeySchema = null;

    /**
     * The only information needed to create a client are security credential's
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoint's, are performed
     * automatically. Client parameters, such as proxie's, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.PropertiesCredentials
     * @see com.amazonaws.ClientConfiguration
     */
    private static void init() throws Exception {
        AWSCredentials credentials = new PropertiesCredentials(
                AmazonDynamoDBUtils.class.getResourceAsStream("AwsCredentials.properties"));

        dynamoDB = new AmazonDynamoDBClient(credentials);
    }


    public AmazonDynamoDBUtils() {
    	try {
			init();
		} catch (Exception e) {
	        System.out.println("Fatal Error initializing AmmazonDynamoDBUtils");
			e.printStackTrace();
		}
    }

    public AmazonDynamoDBClient getDynamoDB () {
    	return AmazonDynamoDBUtils.dynamoDB;
    }
    
    protected void waitForTableToBecomeAvailable(String tableName) {
        System.out.println("Waiting for " + tableName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {Thread.sleep(1000 * 2);} catch (Exception e) {}
            try {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = dynamoDB.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                System.out.println("  - current state: " + tableStatus);
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) throw ase;
            }
        }

        throw new RuntimeException("Table " + tableName + " never went active");
    }

    /**
     * getTableIfExists(tableName) 
     * 
     * IF the table exists, return the reference to the tableDescription
     * 
     * The caller 
     * @param tableName
     * @return a valid tableDescription or null if not found.
     */
    protected TableDescription getTableIfExists(String tableName) {
    	System.out.println("Checking to see if " + tableName + " has been createad...");
    	TableDescription tableDescription = null;

    	long startTime = System.currentTimeMillis();
    	long endTime = startTime + (10 * 1000);  //(10 * 60 * 1000)

    	while (System.currentTimeMillis() < endTime) {
    		try {Thread.sleep(1000 * 1);} catch (Exception e) {} // was 20
    		try {
    			DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
    			tableDescription = dynamoDB.describeTable(request).getTable();
    			String tableStatus = tableDescription.getTableStatus();

    			System.out.println("  - current table state: " + tableStatus);

    			if (tableStatus.equals(TableStatus.ACTIVE.toString())) 
    				return tableDescription;
    			else
    				return null;
    		} catch (AmazonServiceException ase) {
    			if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) 
    				throw ase;
    		}
    	}

    	throw new RuntimeException("Table " + tableName + " never went active");
    }
    

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * @return the compositeKeySchema
	 */
	public KeySchema getCompositeKeySchema() {
		return compositeKeySchema;
	}

	/**
	 * @param compositeKeySchema the compositeKeySchema to set
	 */
	public void setCompositeKeySchema(KeySchema compositeKeySchema) {
		AmazonDynamoDBUtils.compositeKeySchema = compositeKeySchema;
	}
}
