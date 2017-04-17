package SPARQLSon;

public class EasyQuery {

	public static void main(String[] args) {

		String TDBdirectory = "C:/Users/matth/OneDrive/Documents/UC/PROYECTO MAGISTER/Dev-magister/db";
		// String queryString = "SELECT * WHERE{?x ?y ?z . ?z <http://example.org/label/> ?n}";
		
		String select_everything = "SELECT ?x ?l WHERE {?x <http://example.org/label> ?l}";
		
		String[] variables = {"x", "l"};
		
		long start = System.nanoTime();
		
		DatabaseWrapper dbw = new DatabaseWrapper(TDBdirectory);
		dbw.execQuery(select_everything);
		
		long elapsedTime = System.nanoTime() - start;
		
		System.out.println(elapsedTime / 1000000000.0);
		
	}

}
