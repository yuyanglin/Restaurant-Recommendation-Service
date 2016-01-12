package db;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class DBConnectionTest {


	@Test
	public void testVisitedRestaurants() {
		DBConnection dbconn = new DBConnection("jdbc:mysql://localhost:3306/unittest?user=root&password=root");
		String userId = "1111";
		// Remove the existing visited data from DB
		Set<String> original_visited = dbconn.getVisitedRestaurants(userId);
		dbconn.UnsetVisitedRestaurants(userId,new ArrayList<String>(original_visited));

		// Check if the UnsetVisitedRestaurants works
		Set<String> visited = dbconn.getVisitedRestaurants(userId);
		assertTrue(visited.isEmpty());

		// Check if the SetVisitedRestaurants works
		List<String> businessIds = Arrays.asList("--qeSYxyn62mMjWvznNTdg");
		dbconn.SetVisitedRestaurants(userId, businessIds);
		visited = dbconn.getVisitedRestaurants(userId);
		for (String businessId : businessIds) {
			assertTrue(visited.contains(businessId));
		}

		// Restore the original visited data
		dbconn.SetVisitedRestaurants(userId, new ArrayList<String>(original_visited));
		dbconn.close();
	}
	
	@Test
	public void testGetRestaurantsNearLoation() throws Exception {
		DBConnection dbconn = new DBConnection(
			"jdbc:mysql://localhost:3306/unittest?user=root&password=root");
		JSONArray restaurants = dbconn.GetRestaurantsNearLoation(30.0, 20.0);
		Set<String> nearby_businessids = new HashSet<>();
        int length = restaurants.length();
        for(int i = 0; i < length; i++){
            JSONObject restaurant = restaurants.getJSONObject(i);
            nearby_businessids.add(restaurant.getString("business_id"));  
        }  
        Set<String> expected = new HashSet<String>(Arrays.asList(
        		"--jFTZmywe7StuZ2hEjxyA", "--pOlFxITWnhzc7SHSIP0A", 
        		"--BlvDO_RG2yElKu9XA1_g", "--Y_2lDOtVDioX5bwF6GIw", 
        		"--zgHBiQpr8H2ZqSdGmguQ", "--qeSYxyn62mMjWvznNTdg", 
        		"-024YEtnIsPQCrMSHCKLQw", "--5jkZ3-nUPZxUvtcbr8Uw", 
        		"-0bl9EllYlei__4dl1W00Q", "--UE_y6auTgq3FXlvUMkbw"));
        assertEquals(expected, nearby_businessids);
		dbconn.close();
	}

}