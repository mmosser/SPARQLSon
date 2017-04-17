package SPARQLSon;

import java.util.ArrayList;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;

public class Main {

	public static void main(String[] args) throws Exception {
		
		// Database loading
		
		String TDBdirectory = "C:/Users/matth/OneDrive/Documents/UC/PROYECTO MAGISTER/Dev-magister/db";
		DatabaseWrapper dbw = new DatabaseWrapper(TDBdirectory);
		
		// Examples of query using the BIND_API function
		
		String query_yelp = "SELECT ?x ?n ?b ?r WHERE {?x <http://yago-knowledge.org/resource/isLocatedIn> <http://yago-knowledge.org/resource/Chile> .  ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://yago-knowledge.org/resource/wikicat_Communes_of_Chile> . ?x <http://www.w3.org/2000/01/rdf-schema#label> ?n  " +
				 "BIND_API <https://api.yelp.com/v2/search?term=Burguers&location={n}&sort=2>([\"businesses\"][0][\"name\"], [\"businesses\"][0][\"rating\"]) AS (?b, ?r) " + 
				 "}";
		
		String query_domagoj = "SELECT ?x ?y ?t ?t2 WHERE{?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/City> . " + 
					   "?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/City> . " + 
					   "?x <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?lo . " + 
					   "?y <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?lo2 . " + 
					   "?x <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?la . " + 
					   "?y <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?la2 . " + 
					   "?x <http://www.w3.org/2000/01/rdf-schema#label> ?x2 . " + 
					   "?y <http://www.w3.org/2000/01/rdf-schema#label> ?y2 " + 
					   "FILTER((?la <= -?la2 + 0.25) && (?la >= -?la2 - 0.25) && " + 
					   "(?lo <= -((?lo2/abs(?lo2))*(180 - abs(?lo2))) + 0.25) && (?lo >= -((?lo2/abs(?lo2))*(180 - abs(?lo2))) - 0.25)) " + 
					   "BIND_API <http://api.openweathermap.org/data/2.5/weather?q={x2}&appid=be84c20688b078837610d2010e2cd564>([\"main\"][\"temp\"]) AS (?t) " + 
					   "BIND_API <http://api.openweathermap.org/data/2.5/weather?q={y2}&appid=be84c20688b078837610d2010e2cd564>([\"main\"][\"temp\"]) AS (?t2) " + 
					   "}";
		
		String query_museum = "SELECT ?x ?l ?h ?t WHERE {?x ?y <http://dbpedia.org/ontology/Museum> . ?x <http://dbpedia.org/ontology/location> <http://dbpedia.org/resource/London> . ?x <http://www.w3.org/2000/01/rdf-schema#label> ?l " + 
				              "BIND_API <https://api.yelp.com/v2/search?term={l}&location=London&radius_filter=40000>([\"businesses\"][0][\"is_closed\"]) AS (?h) " + 
							  "BIND_API <https://api.twitter.com/1.1/search/tweets.json?q={l}&result_type=recent>([\"statuses\"][0][\"text\"]) AS (?t) " + 
				              "FILTER(bound(?h) && bound(?t)) }";
		
		String query_ski = "SELECT ?x ?y ?n ?t WHERE {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/class/yago/SkiAreasAndResortsInJapan> . ?x <http://dbpedia.org/ontology/location> ?y . ?y <http://www.w3.org/2000/01/rdf-schema#label> ?n  " + 
						   "BIND_API <http://api.openweathermap.org/data/2.5/weather?q={n},Japan&appid=be84c20688b078837610d2010e2cd564>([\"weather\"][0][\"description\"]) AS (?t) " + 
						   " }";
		
		String query_ski2 = "SELECT ?x ?n ?t WHERE {?x <http://yago-knowledge.org/resource/isLocatedIn> <http://yago-knowledge.org/resource/Hokkaido> . ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://yago-knowledge.org/resource/wikicat_Ski_areas_and_resorts_in_Japan> . ?x <http://www.w3.org/2000/01/rdf-schema#label> ?n  " + 
				   "BIND_API <http://api.openweathermap.org/data/2.5/weather?q={n},Japan&appid=be84c20688b078837610d2010e2cd564>([\"weather\"][0][\"description\"]) AS (?t) " + 
				   " }";
		
		String query_ski_tw = "SELECT ?x ?n ?t WHERE {?x <http://yago-knowledge.org/resource/isLocatedIn> <http://yago-knowledge.org/resource/Hokkaido> . ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://yago-knowledge.org/resource/wikicat_Ski_areas_and_resorts_in_Japan> . ?x <http://www.w3.org/2000/01/rdf-schema#label> ?n  " + 
				   "BIND_API <https://api.twitter.com/1.1/search/tweets.json?q={n}&result_type=recent>([\"statuses\"][0][\"text\"]) AS (?t) " + 
				   " }";
		
		String query_chile = "SELECT ?x ?l ?t WHERE {?x <http://example.org/label> ?l " + 
				   "BIND_API <http://api.openweathermap.org/data/2.5/weather?q={l},Chile&appid=99ac6530dcbd78fa4c02d08ec5297a52>([\"weather\"][0][\"description\"]) AS (?t) " + 
				   " }";
		
		String query_twitter = "SELECT ?l ?t WHERE {?x <http://example.org/label> ?l " + 
				   "BIND_API <https://api.twitter.com/1.1/search/tweets.json?q={l}&result_type=recent>([\"statuses\"][*][\"text\"]) AS (?t) " + 
				   " }";
		
		// Definition of parameters
		
		HashMap<String, String> params1 = new HashMap<String, String>();

		// for openweathermap API call
		
		/*		params1.put("consumerKey", "");
				params1.put("consumerSecret", "");
				params1.put("token", "");
				params1.put("tokenSecret", "");
				params1.put("replace_string", "_");
				params1.put("cache", "false");		*/
				
		// for Twitter API "SPARQLSon" call
		
		params1.put("consumerKey", "OGVqZOelXyt96ed2ZPNnpBUF6");
		params1.put("consumerSecret", "VIdyxn4iF5fyAaAoq0YeApiAI0JALEcF1atLSUWInfBty4lZrW");
		params1.put("token", "207609813-eGvMI0zesiiwZe0MFmwkvq54mOYDiExvkiUZxLET");
		params1.put("tokenSecret", "wG8pwvorSTsmHqulgnQHi40nxfpnc7hWszJMvrwF8CNux");
		params1.put("replace_string", "_");
		params1.put("cache", "false");
		
		
		// Definition of strategies to call the API(s)
		
		GetJSONStrategy strategy_oauth = new OAuthStrategy();
		GetJSONStrategy strategy_basic = new BasicStrategy();
		
		// Storage of strategies and parameters to call the API(s)
		
		ArrayList<GetJSONStrategy> strategy = new ArrayList<>();
		ArrayList<HashMap<String, String>> params = new ArrayList<HashMap<String,String>>();
		
		strategy.add(strategy_oauth);
		/* strategy.add(strategy_basic);	*/
		params.add(params1);
		
		
		// Execution of the query
		
		long start = System.nanoTime();
		dbw.evaluateSPARQLSon(query_twitter, strategy, params);
		long elapsedTime = System.nanoTime() - start;
		
		System.out.println("Total: " + elapsedTime / 1000000000.0);
		System.out.println("API: " + dbw.timeApi / 1000000000.0);
		elapsedTime = elapsedTime - dbw.timeApi;	
		System.out.println("DB: " + elapsedTime / 1000000000.0);
		
		/* strategy_oauth.set_params(params2);
		   JSONObject j = ApiWrapper.getJSON("https://api.twitter.com/1.1/search/tweets.json?q=Sapporo&result_type=recent", params2, strategy_oauth);
		   strategy_oauth.set_params(params1);
		   JSONObject j2 = ApiWrapper.getJSON("https://api.yelp.com/v2/search?term=Museum_of_London&location=London&radius_filter=40000", params1, strategy_oauth);
		   System.out.println(j.toString(4));
		   System.out.println(j2.toString());	*/
		
	}

}
