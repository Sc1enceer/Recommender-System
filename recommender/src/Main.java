import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Main {

	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		
		DataBase db = new DataBase("UserRatings.db");
		
        // SQL statement for creating a new table
		if(!db.exists()) {
			
		    String sql = "CREATE TABLE IF NOT EXISTS userRatings (\n"
		            + "    userid integer NOT NULL,\n"
		            + "    itemid integer NOT NULL,\n"
		            + "    rating double NOT NULL,\n"
		            + "    timestamp integer NOT NULL,\n"
		            + "    unique (userid, itemid, rating, timestamp)\n"
		            + ");";
		
			db.createNewTable(sql);
			
			db.batchInsert("comp3208-train-small.csv");
        
		}
		
	}

}
