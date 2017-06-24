package SPARQLSon;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.core.DatasetImpl;

/**
 * This is an example built on top of the Jena ARQ library.
 * See: http://jena.sourceforge.net/ARQ/documentation.html
 */
public class ServiceExampleQuery {
	public static void main(String[] args) throws Exception {
		
		String query_movie =
			"PREFIX owl: <http://www.w3.org/2002/07/owl#> \n" +
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" +
			"PREFIX movie: <http://data.linkedmdb.org/resource/movie/> \n" +
			"PREFIX dcterms: <http://purl.org/dc/terms/> \n" +

			"SELECT ?film ?label ?subject WHERE {\n" +
			"    SERVICE <http://data.linkedmdb.org/sparql> {\n" +
			"        ?film a movie:film .\n" +
			"        ?film rdfs:label ?label .\n" +
			"        ?film owl:sameAs ?dbpediaLink .\n" +
			"        FILTER(regex(str(?dbpediaLink), 'dbpedia', 'i')) \n" +
			"    }\n" +
			"    SERVICE <http://dbpedia.org/sparql> {\n" +
			"        ?dbpediaLink dcterms:subject ?subject .\n" +
			"    }\n" +
			"}\n" +
			"LIMIT 50 \n";
		
		System.out.println("--QUERYING-- \n" + query_movie);
		long start = System.nanoTime();
		
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(query_movie), new
				DatasetImpl(ModelFactory.createDefaultModel()));		
		ResultSet results = exec.execSelect();
		
		System.out.println("--RESULTS-- \n");
	    for ( ; results.hasNext() ; ) {
	    	QuerySolution soln = results.nextSolution() ;
	    	System.out.println(soln);
	    }
	    long elapsedTime = System.nanoTime() - start;
		System.out.println("Total: " + elapsedTime / 1000000000.0);		
	}
}