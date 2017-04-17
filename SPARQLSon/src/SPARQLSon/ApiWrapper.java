package SPARQLSon;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.sparql.core.QuerySolutionBase;
import org.json.*;


// changes from MM included

public class ApiWrapper {
	
	
	public static JSONObject getJSON(String urlString, HashMap<String,String> params, GetJSONStrategy strategy) throws JSONException, Exception {
		JSONObject json = new JSONObject(strategy.readURL(urlString));
		return json;
	}
	
	// Function to insert a part of the result of the SPARQL request into the API request
	public static String insertValuesURL(String apiUrlRequest, QuerySolution rb, String replace_string) {
		String url = apiUrlRequest;
		Pattern pattern_variables = Pattern.compile("\\{(\\w*?)\\}"); // "\\{" allows to match { as \ and { are meta characters
		Matcher m = pattern_variables.matcher(url);
		while (m.find()) {
		    String s = m.group(1); // s is equal to the first subsequence of the url which matches the pattern 
		    String value = rb.get(s).asLiteral().getValue().toString().replaceAll("[\\ ]+", replace_string);
		    url = m.replaceFirst(value);
		    m = pattern_variables.matcher(url);
		}
//		System.out.println("--DEBUG INSERTVALUESURL-- Url: "+url);
		return url;
	}
	
	
	
	public static ArrayList<String> getKeys(String serializedKeys) {	// MM changed the regex pattern to match the [*]
		ArrayList<String> keys = new ArrayList<String>();
		
		
//OLD:	Pattern pattern_variables = Pattern.compile("\\[([\"]{0,1}\\w*[\"]{0,1})\\]"); //encuentra [], hay "" o no, si lo encuentras es un object sino una regla, agregalo a una lista
//MM: 	Change in the regex to match the [*] key if there is
		Pattern pattern_variables = Pattern.compile("\\[([\"]{0,1}[\\*]{0,1}\\w*[\"]{0,1})\\]"); 

		Matcher m = pattern_variables.matcher(serializedKeys);
		while (m.find()) {
		    String s = m.group(1);
		    keys.add(s);
		}
		return keys;
	}
	
	
	
	public static Object getValueJson(Object json, ArrayList<String> keys) throws JSONException { // MM changed the regex pattern to match the [*]
		if (json.getClass().equals(JSONObject.class)) {
			
			String key = keys.get(0).substring(1, keys.get(0).length() - 1); // with the first element of keys: abc <- "abc"
		
			if (keys.size() == 1) {
//				System.out.println("--DEBUG APIWRAPPER SHOW JSON-- \n"+((JSONObject)json).get(key));
				return ((JSONObject)json).get(key);
			}
			else {
				if (keys.get(1).charAt(0) == '"') { // the second key of keys is a string so we have another json
					ArrayList<String> new_keys = new ArrayList<String>(keys.subList(1, keys.size()));
					return getValueJson(((JSONObject)json).get(key), new_keys);
				}
				else { // the second key of keys is a number or * so we have an array of json
					ArrayList<String> new_keys = new ArrayList<String>(keys.subList(1, keys.size()));
					return getValueJson(((JSONObject)json).getJSONArray(key), new_keys);
				}
			}
		}
		else if(json.getClass().equals(JSONArray.class)) {
//OLD:		String key = keys.get(0).substring(0, keys.get(0).length());
			String key = keys.get(0);
//			System.out.println("--DEBUG APIWRAPPER ARRAY'S KEY-- The key requested is: " + key);
			
//MM:		Change to handle the case of [*]
			if (key.charAt(0)=='*') {
				return(getAllFromJsonArray((JSONArray)json,keys));				
			}
			else {
				if (keys.size() == 1) {
					return ((JSONArray)json).get(Integer.parseInt(key));
				}
				else {	
					if (keys.get(1).charAt(0) == '"') {
						ArrayList<String> new_keys = new ArrayList<String>(keys.subList(1, keys.size()));
						return getValueJson(((JSONArray)json).get(Integer.parseInt(key)), new_keys);
					}
					else {
						ArrayList<String> new_keys = new ArrayList<String>(keys.subList(1, keys.size()));
						return getValueJson(((JSONArray)json).get(Integer.parseInt(key)), new_keys);
					}
				}
			}
		}
		else {
			return null;
		}

	}
	
//MM: New method to handle the case of [*]	
	public static JSONArray getAllFromJsonArray(JSONArray jsonArray, ArrayList<String> keys) throws JSONException {	

		JSONArray result = new JSONArray();

		if (keys.size() == 1) {
			result = jsonArray;
		}
		else {
			ArrayList<String> new_keys = new ArrayList<String>(keys.subList(1, keys.size())); // new_keys init at 2 because we don't want to keep the *
			for(int i=0; i<jsonArray.length(); i++) {
				result.put(i, getValueJson(jsonArray.get(i), new_keys));
			}
		}
//MM: to manage the case with more than one [*]: [...,...] <- [[...],[...]]
		String s = result.toString().replaceAll("\\[{2,}", "[").replaceAll("\\]{2,}", "]").replaceAll("\\],\\[", ",");
		JSONArray result_corrected = new JSONArray(s);
//		
		return (result_corrected);

	}
	

}

