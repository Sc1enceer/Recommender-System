public class Main {

	@SuppressWarnings({ "static-access" })
	public static void main(String[] args) {
		
		DataBase database = new DataBase("UserRatings.db");
		
		String defaultTable = "userRatings";
		
        // SQL statement for creating a new table
		if(!database.exists()) {
			
		    String sql = "CREATE TABLE IF NOT EXISTS " + defaultTable + " (\n"
		            + "    userid integer NOT NULL,\n"
		            + "    itemid integer NOT NULL,\n"
		            + "    rating double NOT NULL,\n"
		            + "    timestamp integer NOT NULL,\n"
		            + "    unique (userid, itemid, rating, timestamp)\n"
		            + ");";
		
		    database.createNewTable(sql);
			
		    database.batchInsert("comp3208-train.csv");
		}
		
		new Recommender(database);
	}

}
