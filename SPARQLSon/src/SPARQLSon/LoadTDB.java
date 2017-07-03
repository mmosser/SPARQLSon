package SPARQLSon;

public class LoadTDB {

	public static void main(String[] args) {

		String TDBdirectory = "C:/Users/matth/Documents/UC/PROYECTO MAGISTER/Dev-magister/db";
		DatabaseWrapper dbw = new DatabaseWrapper(TDBdirectory);
		dbw.createDataset("C:/Users/matth/Documents/UC/PROYECTO MAGISTER/Dev-magister/input.ttl", "TTL");		
	}

}
