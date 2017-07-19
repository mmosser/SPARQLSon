package SPARQLSon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.util.FileManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.jayway.jsonpath.JsonPath;


public class DatabaseWrapper {
	
	int ApiCalls;
	String TDBdirectory;
	long timeApi;
	ArrayList<String> cacheKeys;
	HashMap<String, Object> cache; //MM changed JSONObject to Object because of use of jsonpath
	static final int CACHE_SIZE = 400;
	static final boolean FUSEKI_ENABLED = false;
	static final boolean LOG = true;
	
	static final Class[] no_quote_types = {Boolean.class, Integer.class, Double.class, Float.class};

	public DatabaseWrapper(String _directory) {
		this.TDBdirectory = _directory;
		this.cacheKeys = new ArrayList<String>();
		this.cache = new HashMap<>();
		this.timeApi = 0;
		this.ApiCalls = 0;
	}

	public void createDataset(String source, String format) {
		Dataset dataset = TDBFactory.createDataset(this.TDBdirectory);
		Model tdb = dataset.getDefaultModel();
		FileManager.get().readModel(tdb, source, format);
		dataset.close();
	}
	
	public void execQuery(String queryString) {
		Query query = QueryFactory.create(queryString);
		QueryExecution qexec;
		Dataset dataset;
		if(!FUSEKI_ENABLED) {
			dataset = TDBFactory.createDataset(this.TDBdirectory);
			qexec = QueryExecutionFactory.create(query, dataset);
		}
		else {
			qexec = QueryExecutionFactory.sparqlService("http://localhost:3030/ds", query);
		}
		try {
			// Assumption: it's a SELECT query.
			ResultSet rs = qexec.execSelect();
			JSONObject jsonResponse = new JSONObject();
			jsonResponse.put("results", new JSONArray());
			jsonResponse.put("vars", rs.getResultVars());
			// The order of results is undefined. 
			int mapping_count = 0;
			for ( ; rs.hasNext() ; ) {
				JSONObject mappingToJSON = new JSONObject();
				QuerySolution rb = rs.nextSolution() ;
				// Get title - variable names do not include the '?' (or '$')
				for (String v: rs.getResultVars()) {	
					mappingToJSON.put(v, rb.get(v));
				}
				((JSONArray)jsonResponse.get("results")).put(mappingToJSON);
				mapping_count++;
			}
			System.out.println(jsonResponse.toString());
			System.out.println("Mappings: "+mapping_count);
		}
		finally
		{
			// QueryExecution objects should be closed to free any system resources 
			qexec.close() ;
			if (!FUSEKI_ENABLED) {
				dataset.close();
			}
		}
	}

	public void execQuery(String queryString, String[] variables) {
		Query query = QueryFactory.create(queryString);
		Dataset dataset = TDBFactory.createDataset(this.TDBdirectory);
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		try {
			// Assumption: it's a SELECT query.
			ResultSet rs = qexec.execSelect();
			// The order of results is undefined. 
			int mapping_number = 1;
			for ( ; rs.hasNext() ; ) {
				QuerySolution rb = rs.nextSolution() ;
				System.out.println("### Mapping no. " + mapping_number + " ###");
				// Get title - variable names do not include the '?' (or '$')
				for (String v: variables) {	
					System.out.println("Variable: " + v + ", Value: " + rb.get(v));
				}
				System.out.println("######");
				mapping_number++;
			}
		}
		finally
		{
			// QueryExecution objects should be closed to free any system resources 
			qexec.close() ;
			dataset.close();
		}
	}

	public MappingSet execQueryGenURL(String queryString, String apiUrlRequest, String[] jpath, 
			String[] bindName, GetJSONStrategy strategy, 
			HashMap<String, String> params) throws JSONException, Exception {
		System.out.println(queryString);
		Query query = QueryFactory.create(queryString);
		QueryExecution qexec;
		Dataset dataset;
		if(!FUSEKI_ENABLED) {
			dataset = TDBFactory.createDataset(this.TDBdirectory);
			qexec = QueryExecutionFactory.create(query, dataset);
		}
		else {
			qexec = QueryExecutionFactory.sparqlService("http://localhost:3030/ds", query);
		}
		MappingSet ms = new MappingSet();
		int numb_mappings=0;
		ArrayList<String> ms_varnames = new ArrayList<String>();

		try {
			// Assumption: it's a SELECT query.
			ResultSet rs = qexec.execSelect() ;
			List<String> vars_name =  rs.getResultVars();
			for (String vn: vars_name) {
				ms_varnames.add(vn);
			}
			for (String bn: bindName) {
				ms_varnames.add(bn);
			}
			ms.set_var_names(ms_varnames);
			// The order of results is undefined. 
			for ( ; rs.hasNext() ; ) {
				QuerySolution rb = rs.nextSolution() ;	
				HashMap<String, String> mapping = mappQuerySolution(rb, vars_name);				
				String url_req = ApiWrapper.insertValuesURL(apiUrlRequest, rb, params.get("replace_string"));
				Object json = null;
				try {
					long start = System.nanoTime();
					json = retrieve_json(url_req, params, strategy);
					long stop = System.nanoTime();
					this.timeApi += (stop - start);
				}
				catch (Exception name) {
					System.out.println("ERROR: " + name);
					for (int i = 0; i < bindName.length; i++) {
						mapping.put(bindName[i], "UNDEF");
					}
				}
				if (json != null) {
					ArrayList<HashMap<String, String>> mapping_array = new ArrayList<HashMap<String, String>>();
					for (int i = 0; i < bindName.length; i++) {
						try {						
							Object value = JsonPath.parse(json).read(jpath[i]);
							mapping_array = updateMappingArray(mapping_array, value, bindName[i], i, mapping);					
						}
						catch (Exception name) {
							System.out.println("ERROR: " + name);
							// CASE 0.A: json_nav = first argument
							if(i==0){
								mapping.put(bindName[i], "UNDEF");
								mapping_array.add(mapping); // the ArrayList has a size=1				
							}
							// CASE 0.B: json_nav = next arguments
							else {
								for (int k=0; k<mapping_array.size(); k++){
									mapping_array.get(k).put(bindName[i], "UNDEF");
								}
							}
						}
					}
					// Add all the mappings relative to the result rb to the MappingSet to return
					for (int k=0; k<mapping_array.size(); k++){
						ms.addMapping(mapping_array.get(k));
						numb_mappings += +1;
					}
				}
			}
		}
		finally
		{
			// QueryExecution objects should be closed to free any system resources 
			qexec.close();
			if(!FUSEKI_ENABLED) {
				dataset.close();
			}
		}
		return ms;
	}
	
	public HashMap<String, String> mappQuerySolution(QuerySolution rb, List<String> vars_name) {
		HashMap<String, String> mapping = new HashMap<String, String>();
		for(String var: vars_name) {
			if (rb.contains(var))
			{
				if (rb.get(var).isLiteral())
				{
					if(!rb.get(var).asLiteral().getDatatypeURI().equals("http://www.w3.org/2001/XMLSchema#string") && 
					   !rb.get(var).asLiteral().getDatatypeURI().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")) {
						mapping.put(var, rb.get(var).asLiteral().getString());
					}
					else {
						if (!rb.get(var).asLiteral().getLanguage().equals("")) {
							mapping.put(var, "\"" + rb.get(var).asLiteral().getValue().toString() + "\"@" + rb.get(var).asLiteral().getLanguage());
						}
						else {
							mapping.put(var, "\"" + rb.get(var).asLiteral().getValue().toString() + "\"");
						}
					}
				}
				else 
				{
					mapping.put(var, "<" + rb.get(var).toString() + ">");
				}
			}
			else {
				mapping.put(var, "UNDEF");
			}
		}
		return mapping;
	}
	
	public ArrayList<HashMap<String, String>> updateMappingArray(ArrayList<HashMap<String, String>> mapping_array, Object jsonValue, String bindName, int bindName_index, HashMap<String, String> initial_mapping) {
		int mapping_array_size = mapping_array.size();
		// CASE 1: value.class = Array of Elements
		if (jsonValue.getClass().equals(net.minidev.json.JSONArray.class)){
			for (int j=0; j<((net.minidev.json.JSONArray)jsonValue).size(); j++){		
				// CASE 1.A: json_nav = first argument
				if(bindName_index==0){
					Object mapping_clone = initial_mapping.clone();
					mapping_array.add((HashMap<String, String>)mapping_clone); // I initiate by cloning the mapping I had built into all the mapping_array mappings
					mapping_array.get(mapping_array.size()-1).put(bindName, serializeValue(((net.minidev.json.JSONArray)jsonValue).get(j))); // I add to the mapping_array mappings the relative JSON of the JSONArray
				}
				// CASE 1.B: json_nav = next arguments
				else {
					// I assign to each element of mapping_array the first value of the new argument
					if(j==0){
						for (int k=0; k<mapping_array.size(); k++){
							mapping_array.get(k).put(bindName, serializeValue(((net.minidev.json.JSONArray)jsonValue).get(j))); // I add to the mapping_array mappings the relative JSON of the JSONArray
						}
					}
					// For each next values, I first "duplicate" the original mapping_array and then assign the value to the duplicate
					else {
						for (int k=0; k<mapping_array_size; k++){
							Object mapping_clone = mapping_array.get(k).clone();
							mapping_array.add((HashMap<String, String>)mapping_clone);
							mapping_array.get(mapping_array.size()-1).put(bindName, serializeValue(((net.minidev.json.JSONArray)jsonValue).get(j)));
						}
					}
					
				}
			}	
		}
		// CASE 2: value.class = Single Element
		else {
			// CASE 2.A: json_nav = first argument
			if(bindName_index==0){
				initial_mapping.put(bindName, serializeValue(jsonValue));
				mapping_array.add(initial_mapping); // the ArrayList has a size=1				
			}
			// CASE 2.B: json_nav = next arguments
			else {
				for (int k=0; k<mapping_array.size(); k++){
					mapping_array.get(k).put(bindName, serializeValue(jsonValue));
				}
			}
		}
		return mapping_array;
	}
	
	public static String serializeValue(Object value) {
		for (Class c: no_quote_types) {
			if (value.getClass().equals(c)) {
				return value.toString();
			}
		}
		return "\"" + value.toString().replace('\n', ' ').replace("\"", "\\\"") + "\"";
	}
	

	public ResultSet execPostBindQuery(String selectSection, String bodySection, MappingSet ms) {
		ResultSet rs = null;
		String queryString = selectSection + ms.serializeAsValues() + bodySection;
// 		System.out.println("--DEBUG EXECPOSTBINDQUERY-- Querystring: "+ queryString);
		QueryExecution qexec;
		Dataset dataset;
		if(!FUSEKI_ENABLED) {
			dataset = TDBFactory.createDataset(this.TDBdirectory);
			System.out.println(queryString);
			qexec = QueryExecutionFactory.create(queryString, dataset);
		}
		else {
			qexec = QueryExecutionFactory.sparqlService("http://localhost:3030/ds", queryString);
		}
		try {
			rs = qexec.execSelect() ;
			if (LOG) {
				printResultSet(rs);
			}
		}
		finally
		{
			// QueryExecution objects should be closed to free any system resources 
			qexec.close();
			if (!FUSEKI_ENABLED) {
				dataset.close();
			}
		}
		return rs;
	}

	public static void printResultSet(ResultSet rs) {
		int mapping_count = 0;
		JSONObject jsonResponse = new JSONObject();
		jsonResponse.put("results", new JSONArray());
		jsonResponse.put("vars", rs.getResultVars());
		
		for ( ; rs.hasNext() ; ) {
			JSONObject mappingToJson = new JSONObject();
			QuerySolution rb = rs.nextSolution() ;
			// Get title - variable names do not include the '?' (or '$')
			Iterator<String> names = rb.varNames();
			while (names.hasNext()) {	
				String v = names.next();
				mappingToJson.put(v, rb.get(v));
			}
			((JSONArray)jsonResponse.getJSONArray("results")).put(mappingToJson);
			mapping_count++;
		}
		System.out.println(jsonResponse.toString());
		System.out.println("Mappings: " + mapping_count);
	}

	public void evaluateSPARQLSon(String queryString, ArrayList<GetJSONStrategy> strategy, ArrayList<HashMap<String, String>> params, boolean replace) throws JSONException, Exception {
		strategy.get(0).set_params(params.get(0));
		HashMap<String, Object> parsedQuery = SPARQLSonParser.parseSPARQLSonQuery(queryString, replace);
		if (parsedQuery.get("URL")!= null && parsedQuery.get("PATH") != null && parsedQuery.get("ALIAS") != null ) {
			String firstQuery = apply_params_to_first_query(params, parsedQuery);
			if(params.get(0).containsKey("min_api_call") && params.get(0).get("min_api_call").equals("true")) {
				parsedQuery = ApiOptimizer.minimizeAPICall(parsedQuery);
				if (parsedQuery.get("VARS").toString().length()>0) {
					firstQuery = (String) parsedQuery.get("PREFIX") + " SELECT DISTINCT " + parsedQuery.get("VARS") + " WHERE {" + (String) parsedQuery.get("FIRST") + "} ";;
				}
				else {
					firstQuery = (String) parsedQuery.get("PREFIX") + " SELECT * " + " WHERE {" + (String) parsedQuery.get("FIRST") + "} ";;
				}
			}
			MappingSet ms = execQueryGenURL(firstQuery,
					(String) parsedQuery.get("URL"),
					(String[]) parsedQuery.get("PATH"),
					(String[]) parsedQuery.get("ALIAS"),
					strategy.get(0), params.get(0));
			String api_url_string = " +SERVICE +<([\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+)> *\\{ *\\( *(\\$.*$)";	
			Pattern pattern_variables = Pattern.compile(api_url_string);
			Matcher m = pattern_variables.matcher(" " + (String) parsedQuery.get("LAST"));
			if (m.find()) {
				String recursive_query_string = ((String) parsedQuery.get("PREFIX")) + ((String) parsedQuery.get("SELECT")) + ms.serializeAsValues() + (String) parsedQuery.get("LAST");
				evaluateSPARQLSon(recursive_query_string, new ArrayList<GetJSONStrategy>(strategy.subList(1, strategy.size())), new ArrayList<HashMap<String,String>>(params.subList(1, params.size())), false);
			}
			else {
				execPostBindQuery((String) parsedQuery.get("PREFIX") + " " + (String) parsedQuery.get("SELECT"), (String) parsedQuery.get("LAST"), ms);
			}
		}
		else {
			execQuery(queryString);
		}
	}

	public void evaluateSPARQLSon(String queryString, ArrayList<GetJSONStrategy> strategy, ArrayList<HashMap<String, String>> params) throws JSONException, Exception {
		evaluateSPARQLSon(queryString, strategy, params, true);
	}

	public String apply_params_to_first_query(ArrayList<HashMap<String, String>> params, HashMap<String, Object> parsedQuery) throws JSONException, Exception {
		String new_query = "";
		if (params.get(0).containsKey("distinct") && params.get(0).get("distinct").equals("true")) {
			new_query = (String) parsedQuery.get("PREFIX") + " SELECT DISTINCT " + params.get(0).get("distinct_var") + " WHERE { " + (String) parsedQuery.get("FIRST") + " } ";
		}
		else {
			new_query = (String) parsedQuery.get("PREFIX") + " SELECT * WHERE { " + (String) parsedQuery.get("FIRST") + " } ";
		}
		if (params.get(0).containsKey("limit")) {
			new_query += "LIMIT " + params.get(0).get("limit") + " ";
		}
		return new_query;
	}
	
	public Object retrieve_json(String url_req, HashMap<String,String> params, GetJSONStrategy strategy) throws JSONException, Exception {
		if (params.containsKey("cache") && params.get("cache").equals("true")) {
			if (cache.containsKey(url_req)) {
				return cache.get(url_req);
			}
			else {
				this.ApiCalls += 1;
				Object json =  ApiWrapper.getJSON(url_req, params, strategy);
				if (cacheKeys.size() < CACHE_SIZE) {
					cacheKeys.add(url_req);
					cache.put(url_req, json);
				}
				else {
					String removed_key = cacheKeys.remove(0);
					cache.remove(removed_key);
					cache.put(url_req, json);
				}
				return json;
			}
		}
		else {
			this.ApiCalls += 1;
			return ApiWrapper.getJSON(url_req, params, strategy);
		}
	}
	
}
