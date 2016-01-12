package db;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class CategoryImport {
	private static Connection conn = null;

	static {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/laiproject?user=root&password=root");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			if (conn == null) {
				return;
			}
			Statement stmt = conn.createStatement();
			BufferedReader reader = new BufferedReader(new FileReader("src/dataset/categories_output"));
			String line = null;
			String sql = null;
			while ((line = reader.readLine()) != null) {
				String[] values = line.split("\t");
				String count = values[values.length - 1];
				StringBuilder sb = new StringBuilder(line);
				sb.delete(sb.lastIndexOf(count), sb.length());
				sb.delete(sb.lastIndexOf("\t"), sb.length());
				// add values[0, value-1] back to a string
				String[] keys = sb.toString().split(",");
				String firstCategory = keys[0].trim();
				String secondCategory = keys[1].trim();
				if (!firstCategory.equals("Restaurants") && !secondCategory.equals("Restaurants")) {
					sql = "INSERT INTO USER_CATEGORY_HISTORY (`first_id`, `second_id`, `count`) VALUES (\""
							+ firstCategory + "\", \"" + secondCategory + "\" ," + Integer.parseInt(count) + ")";
					stmt.executeUpdate(sql);
					sql = "INSERT INTO USER_CATEGORY_HISTORY (`first_id`, `second_id`, `count`) VALUES (\""
							+ secondCategory + "\", \"" + firstCategory + "\" ," + Integer.parseInt(count) + ")";
					stmt.executeUpdate(sql);
				}
			}
			reader.close();
			System.out.println("Done importing.");
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					/* ignored */}
			}
		} catch (Exception e) { /* report an error */
			System.out.println(e.getMessage());
		}
	}
}