package SPARQLSon;

/**
 * This is an example built on top of the Jena ARQ library.
 * See: http://jena.sourceforge.net/ARQ/documentation.html
 */
public class ServiceExampleQuery {
	public static void main(String[] args) throws Exception {
		
		String TDBdirectory = "C:/Users/matth/Documents/UC/PROYECTO MAGISTER/Dev-magister/db";
		
		String query_movie =
			"PREFIX owl: <http://www.w3.org/2002/07/owl#> \n" +
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" +
			"PREFIX movie: <http://data.linkedmdb.org/resource/movie/> \n" +
			"PREFIX dcterms: <http://purl.org/dc/terms/> \n" +

			"SELECT ?film ?label ?subject ?subject2 WHERE {\n" +
			"    SERVICE <http://data.linkedmdb.org/sparql> {\n" +
			"        ?film a movie:film ;\n" +
			"        	rdfs:label ?label ;\n" +
			"        	owl:sameAs ?dbpediaLink .\n" +
			"        FILTER(regex(str(?dbpediaLink), 'dbpedia', 'i')) \n" +
			"    }\n" +
			"    SERVICE <http://dbpedia.org/sparql> {\n" +
			"        ?dbpediaLink dcterms:subject ?subject .\n" +
			"    }\n" +
			"}\n" +
			"LIMIT 100 \n";
		
		String query = 
				  "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> \n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"
				+ "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>"
				+ "\n"
				+ "SELECT DISTINCT ?place ?label ?lat ?long WHERE  {\n"
				+ "  ?place ?link <http://dbpedia.org/resource/Chile> .\n"
				+ "  ?place geo:lat ?lat .\n"
				+ "  ?place geo:long ?long .\n"
				+ "  SERVICE <http://dbpedia.org/sparql> {\n"
				+ "    ?place rdfs:label ?label .\n"
				+ "    FILTER(lang(?label) = 'es') .\n"
    			+ "  }"
				+ "}";
		
		String test = "select * where {?p ?o <http://dbpedia.org/resource/Chile>} limit 10";
		
		System.out.println("--QUERYING-- \n" + query_movie);
		long start = System.nanoTime();
		
		DatabaseWrapper dbw = new DatabaseWrapper(TDBdirectory);
		dbw.execQuery(query_movie);
	    long elapsedTime = System.nanoTime() - start;
		System.out.println("Total: " + elapsedTime / 1000000000.0);		
	}
}