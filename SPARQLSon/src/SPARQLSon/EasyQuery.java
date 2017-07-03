package SPARQLSon;

public class EasyQuery {

	public static void main(String[] args) {

		String TDBdirectory = "C:/Users/matth/Documents/UC/PROYECTO MAGISTER/Dev-magister/db";
		String queryString = "SELECT * WHERE{?x ?y ?z . ?z <http://example.org/label/> ?n}";
		
		String[] variables = {"x", "l"};
		
		long start = System.nanoTime();
		
		DatabaseWrapper dbw = new DatabaseWrapper(TDBdirectory);
		dbw.execQuery(queryString);
		
		long elapsedTime = System.nanoTime() - start;
		
		System.out.println(elapsedTime / 1000000000.0);
		
	}

}
