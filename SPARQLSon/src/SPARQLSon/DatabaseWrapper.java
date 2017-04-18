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
			// System.out.println("FUSEKI ENABLED");
			qexec = QueryExecutionFactory.sparqlService("http://localhost:3030/ds", query);
		}
		try {
			// Assumption: it's a SELECT query.
			ResultSet rs = qexec.execSelect();
			JSONObject jsonResponse = new JSONObject();
			jsonResponse.put("results", new JSONArray());
			jsonResponse.put("vars", rs.getResultVars());
			

			// The order of results is undefined. 
			int mapping_count = 1;
			for ( ; rs.hasNext() ; ) {
				JSONObject mappingToJSON = new JSONObject();

				QuerySolution rb = rs.nextSolution() ;

				// System.out.println("### Mapping no. " + mapping_count + " ###");

				// Get title - variable names do not include the '?' (or '$')
				for (String v: rs.getResultVars()) {	
					// System.out.println("Variable: " + v + ", Value: " + rb.get(v));
					mappingToJSON.put(v, rb.get(v));

				}
				// ((JSONArray)jsonResponse.get("results")).put(mappingToJSON);

				// System.out.println("######");
				mapping_count++;
				((JSONArray)jsonResponse.get("results")).put(mappingToJSON);

			}
			System.out.println(jsonResponse.toString());
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
			int mapping_count = 1;
			for ( ; rs.hasNext() ; ) {

				QuerySolution rb = rs.nextSolution() ;

				System.out.println("### Mapping no. " + mapping_count + " ###");

				// Get title - variable names do not include the '?' (or '$')
				for (String v: variables) {	
					System.out.println("Variable: " + v + ", Value: " + rb.get(v));

				}

				System.out.println("######");
				mapping_count++;

			}
		}
		finally
		{
			// QueryExecution objects should be closed to free any system resources 
			qexec.close() ;
			dataset.close();
		}
	}

//MM: Changes of type: MappingSet -> ArrayList<MappingSet> to execute a query for each element of JSONArray
	public MappingSet execQueryGenURL(String queryString, String apiUrlRequest, String jpath, 
			String[] bindName, GetJSONStrategy strategy, 
			HashMap<String, String> params) throws JSONException, Exception {
		Query query = QueryFactory.create(queryString);
		QueryExecution qexec;
		Dataset dataset;
		if(!FUSEKI_ENABLED) {
			dataset = TDBFactory.createDataset(this.TDBdirectory);
			qexec = QueryExecutionFactory.create(query, dataset);
		}
		else {
			// System.out.println("FUSEKI ENABLED");
			qexec = QueryExecutionFactory.sparqlService("http://localhost:3030/ds", query);
		}
//MM change: I create an array of ms which will be what I return
//		ArrayList<MappingSet> ms_array = new ArrayList<MappingSet>();
//
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
			
// MM change to avoid increase the size of the arrayList -> otherwise, the query is called more times that what we need
			int numb_iteration_rs=0;
//
			for ( ; rs.hasNext() ; ) {
				QuerySolution rb = rs.nextSolution() ;			
				System.out.println("--DEBUG QUERYSOLUTION LOOP-- "+rb+" -- Iteration n° "+(numb_iteration_rs+1));
				HashMap<String, String> mapping = new HashMap<String, String>();
// MM change: I create an array of mapping which is gonna be added to each ms
				ArrayList<HashMap<String, String>> mapping_array = new ArrayList<HashMap<String, String>>();
//					
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
				String url_req = ApiWrapper.insertValuesURL(apiUrlRequest, rb, params.get("replace_string"));
				Object json = null;	//MM changed JSONObject to Object because of use of jsonpath
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

				// TODO: resolve 404 and 429 errors.
				if (json != null) {
					
					for (int i = 0; i < bindName.length; i++) {
				//		System.out.println("--DEBUG BINDNAME-- "+bindName[i]);
						try {
//MM changes dues to use of jsonpath			
							//ArrayList<String> keys = ApiWrapper.getKeys(jpath[i]);
							//Object value = ApiWrapper.getValueJson(json, keys);	
							//Object value = ApiWrapper.getValueJson(json, jpath[0]);
							Object value = JsonPath.parse(json).read(jpath);
						//	System.out.println("--DEBUG CLASS-- \n value: "+value+"\n class:"+value.getClass());

//MM changes					
							if (value.getClass().equals(net.minidev.json.JSONArray.class)){
								for (int j=0; j<((net.minidev.json.JSONArray)value).size(); j++){
									Object mapping_clone = mapping.clone();
									mapping_array.add((HashMap<String, String>)mapping_clone); // I initiate by cloning the mapping I had built into all the mapping_array mappings
									mapping_array.get(j).put(bindName[i], serializeValue(((net.minidev.json.JSONArray)value).get(j))); // I add to the mapping_array mappings the relative JSON of the JSONArray
//									System.out.println("--DEBUG MAPPING: verify the values are correctely storaged-- "
//													+ "\n mapping: "+mapping+""
//													+ "\n mapping_clone: "+mapping_clone+""
//													+ "\n mapping_array: "+mapping_array.get(j));
								}		
							}
							else {
//								System.out.println("--DEBUG THIS IS A CASE WITHOUT JSONARRAY--");
								mapping.put(bindName[i], serializeValue(value));
								mapping_array.add(mapping); // the ArrayList has a size=1				
							}
//							System.out.println("--DEBUG MAPPING_ARRAY-- \n Mapping_array: "+ mapping_array +" / \n Size: "+ mapping_array.size());
// MM changes end	
						}

						catch (Exception name) {
							System.out.println("ERROR: " + name);
// MM changes
							for (int k=0; k<mapping_array.size(); k++){
								mapping_array.get(k).put(bindName[i], "UNDEF");
							}
						}
					}			
					for (int k=0; k<mapping_array.size(); k++){
						ms.addMapping(mapping_array.get(k));
						numb_mappings=numb_mappings+1;
					}					
				}

			numb_iteration_rs = numb_iteration_rs + 1;
// MM changes end			
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
		System.out.println("--DEBUG FINAL MAPPINGSET-- \n ms: "+ms.serializeAsValues()+"\n Number of mappings: "+numb_mappings);
		return ms;
	}

	
//MM TODO?? change the MappingSet into ArrayList<MappingSet>
	public MappingSet execQueryDistinct(String queryString, HashMap<String, String> params) throws JSONException, Exception {
		Query query = QueryFactory.create(queryString);
		Dataset dataset = TDBFactory.createDataset(this.TDBdirectory);
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		MappingSet ms = new MappingSet();
		ArrayList<String> ms_varnames = new ArrayList<String>();
		try {
			// Assumption: it's a SELECT query.
			ResultSet rs = qexec.execSelect() ;
			List<String> vars_name =  rs.getResultVars();
			for (String vn: vars_name) {
				ms_varnames.add(vn);
			}
			ms.set_var_names(ms_varnames);
			// The order of results is undefined. 

			for ( ; rs.hasNext() ; ) {

				QuerySolution rb = rs.nextSolution() ;

				HashMap<String, String> mapping = new HashMap<String, String>();
				for(String var: vars_name) {
					if (rb.contains(var))
					{
						if (rb.get(var).isLiteral())
						{
							if(rb.get(var).asLiteral().getDatatypeURI() != null) {
								mapping.put(var, rb.get(var).asLiteral().getString());
							}
							else {
								if (!rb.get(var).asLiteral().getLanguage().equals(""))
								{
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
				
				ms.addMapping(mapping);

			}
		}
		finally
		{
			// QueryExecution objects should be closed to free any system resources 
			qexec.close() ;
			dataset.close();
		}
		return ms;
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
// 		System.out.println(--DEBUG EXECPOSTBINDQUERY-- Querystring: "+queryString);
		QueryExecution qexec;
		Dataset dataset;
		if(!FUSEKI_ENABLED) {
			dataset = TDBFactory.createDataset(this.TDBdirectory);
			System.out.println(queryString);
			qexec = QueryExecutionFactory.create(queryString, dataset);
		}
		else {
			// System.out.println("FUSEKI ENABLED");
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
			// TODO: Resolve what to do with ResultSet rs, actually is useless
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
			

			//System.out.println("### Mapping no. " + mapping_count + " ###");

			// Get title - variable names do not include the '?' (or '$')
			Iterator<String> names = rb.varNames();
			while (names.hasNext()) {	
				String v = names.next();
				// System.out.println("Variable: " + v + ", Value: " + rb.get(v));
				mappingToJson.put(v, rb.get(v));
			}

			//System.out.println("######");
			((JSONArray)jsonResponse.getJSONArray("results")).put(mappingToJson);
			mapping_count++;

		}
		System.out.println(jsonResponse.toString());
		
		
	}

	public void evaluateSPARQLSon(String queryString, ArrayList<GetJSONStrategy> strategy, ArrayList<HashMap<String, String>> params, boolean replace) throws JSONException, Exception {
		strategy.get(0).set_params(params.get(0));
		HashMap<String, Object> parsedQuery = SPARQLSonParser.parseSPARQLSonQuery(queryString, replace);
		String firstQuery = apply_params_to_first_query(params, parsedQuery);
		MappingSet ms = execQueryGenURL(firstQuery, 
				(String) parsedQuery.get("URL"), 
				(String) parsedQuery.get("PATH"), 
				(String[]) parsedQuery.get("ALIAS"),
				strategy.get(0), params.get(0));

		if (params.get(0).containsKey("distinct") && params.get(0).get("distinct").equals("true")) {
			String new_query = "SELECT * WHERE{ " +  ms.serializeAsValues() + " " + (String) parsedQuery.get("FIRST") + " } ";
			ms = execQueryDistinct(new_query, params.get(0));
		}

		String bind_url_string = " BIND_API <([\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+)>(?=((\\\\[\\\\\"]|[^\\\\\"])*\"(\\\\[\\\\\"]|[^\\\\\"])*\")*(\\\\[\\\\\"]|[^\\\\\"])*$)";
		Pattern pattern_variables = Pattern.compile(bind_url_string);
		Matcher m = pattern_variables.matcher(" " + (String) parsedQuery.get("LAST"));
			if (m.find()) {
					String recursive_query_string = ((String) parsedQuery.get("SELECT")) + ms.serializeAsValues() + (String) parsedQuery.get("LAST");
					evaluateSPARQLSon(recursive_query_string, new ArrayList<GetJSONStrategy>(strategy.subList(1, strategy.size())), new ArrayList<HashMap<String,String>>(params.subList(1, params.size())), false);
				}
			else {
				execPostBindQuery((String) parsedQuery.get("SELECT"), (String) parsedQuery.get("LAST"), ms);
			}
	}

	public void evaluateSPARQLSon(String queryString, ArrayList<GetJSONStrategy> strategy, ArrayList<HashMap<String, String>> params) throws JSONException, Exception {
		evaluateSPARQLSon(queryString, strategy, params, true);
	}

	public String apply_params_to_first_query(ArrayList<HashMap<String, String>> params, HashMap<String, Object> parsedQuery) throws JSONException, Exception {
		String new_query = "";
		if (params.get(0).containsKey("distinct") && params.get(0).get("distinct").equals("true")) {
			new_query = "SELECT DISTINCT " + params.get(0).get("distinct_var") + " WHERE { " + (String) parsedQuery.get("FIRST") + " } ";
		}
		else {
			new_query = "SELECT * WHERE { " + (String) parsedQuery.get("FIRST") + " } ";
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
			return ApiWrapper.getJSON(url_req, params, strategy);
		}
	}
	
}
