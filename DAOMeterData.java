/**
 * 
 */
package etlMain;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;


/**
 * This is the DAO (Data Access Object) for retrieving SMT meter persistent data.
 * 
 * The caller of this class will set the persistence type as the source of meter
 * data may be a web-service, a file, an atom feed, RSS or queue.  
 * 
 * Lightweight persistence frameworks such as Hibernate or JPA may be added to 
 * this class at a future date.
 * 
 * @author Mark Bosakowski
 * @param Persistence Type
 *
 */
public class DAOMeterData {

	public static int DOA_FILE = 1;
	public static int DOA_WEB_SERVICE = 2;
	public static int DOA_DAYPARTS = 96;


	DAOMeterData() {
	}


	static Map<String, AttributeValue> newMeterUsageItem( DTOMeterData meterInfo )
	
	  {
		  Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		  /* The USAGE_DATE key is a composite key containing the usage DATE 
		   * concatenated with a specific day-part.    "5/15/2012 17:15".
		   * This will allow the specific day-parts to be queried at the 
		   * current resolution of a 15 minute range.
		   * A space is prepended to the .getUsageStartTime(), allowing for
		   * a natural space delimiter to allow easier parsing of the string 
		   * on scans that retrieve more than one day part.
		   */ 
		  String usageRangeKey = meterInfo.getUsageDate() 
				  + meterInfo.getUsageStartTime();
	
		  item.put("ESIID", 			new AttributeValue(meterInfo.getEsiid()));
		  item.put("USAGE_DATE_TIME", 	new AttributeValue(usageRangeKey));
		  item.put("USAGE_DATE", 		new AttributeValue(meterInfo.getUsageDate()));
		  item.put("USAGE_START_TIME", 	new AttributeValue(meterInfo.getUsageStartTime()));
		  item.put("USAGE_END_TIME",	new AttributeValue(meterInfo.getUsageEndTime()));
		  item.put("USAGE_KWH", 		new AttributeValue(meterInfo.getUsageKwh()));
		  item.put("ESTIMATED_ACTUAL", 	new AttributeValue(meterInfo.getEstimatedOrActual()));
	
		  return item;
	  }
}
