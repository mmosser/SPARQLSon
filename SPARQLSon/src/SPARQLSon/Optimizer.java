package SPARQLSon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Optimizer {
	
	/*
	 * FUNCTION: Transform the parsed query to minimize the number of calls to API by Service
	 * @param {HashMap<String, Object>} parsedQuery
	 * @return {HashMap<String, Object>}
	 */
	public static HashMap<String, Object> minimizeAPICall(HashMap<String, Object> parsedQuery) {
		// Look for inserted variables in the URL of the API-Service call and keep them
		String variable_in_URL = "(.*)=\\{([^\\}]+)\\}.*$";
		ArrayList<String> inserted_variables = new ArrayList<String>();
		Pattern pattern_variables = Pattern.compile(variable_in_URL);
		Matcher m = pattern_variables.matcher((String)parsedQuery.get("URL"));
		while(m.find()){
			inserted_variables.add(m.group(2));
			m = pattern_variables.matcher(m.group(1));
		}
		// Parse the first part of the query into (basic and SPARQL-Service) sections. Each section contains a list of parsed triplets.
		ArrayList<TripletParser> firstQueryTriplets = TripletParser.getParsedQuery((String)parsedQuery.get("FIRST"));
		// Eject the triplets including the injected_variables and stock the variables linked by those triplets
		HashMap<String, Object> opt = ejectIncludingTriplets(inserted_variables, firstQueryTriplets);
		// Eject the triplets constraining the linked_variables (no other variable in the triplet)
		ArrayList<TripletParser> triplets_to_put_last = ejectConstrainingTriplets(
				(ArrayList<String>)opt.get("linked_variables"),
				(ArrayList<TripletParser>)opt.get("selected_triplets"));
		// Stock in a String the variables to Select before calling the API
		String needed_variables_string = "";
		for (String var: inserted_variables) {
			var = "?" + var;
			needed_variables_string += var + " ";
		}
		for (String var: (ArrayList<String>)opt.get("linked_variables")) {
			needed_variables_string += var + " ";
		}
		// Add to the parsed Query the variables to Select before calling the API through Service
		parsedQuery.put("VARS", needed_variables_string);
		// Add to the last part of the query the triplets which are needed to Select the other variables later
		parsedQuery.put("LAST", TripletParser.reverseParsedQuery(triplets_to_put_last) + (String)parsedQuery.get("LAST"));
		return parsedQuery;
	}
	
	/*
	 * FUNCTION: Eject the triplets including the specified variables and stock the other variables linked by those triplets
	 * @param {ArrayList<String>} vars
	 * @param {ArrayList<TripletParser>} list_parsed_triplets
	 * @return {HashMap<String, Object>}
	 */
	public static HashMap<String, Object> ejectIncludingTriplets(ArrayList<String> vars, ArrayList<TripletParser> list_parsed_triplets) {
		ArrayList<String> linked_variables = new ArrayList<String>();
		ArrayList<TripletParser> selected_triplets = (ArrayList<TripletParser>)list_parsed_triplets.clone();
		// Iterate over the inserted variables
		for (String var: vars) {
			var = "?" + var;
			// Iterate over the (basic and SPARQL-Service) sections of the first part of the query
			for (int section=0; section< list_parsed_triplets.size(); section++) {
				// Iterate over the list of triplets in the section
				for (int triplet=0; triplet<list_parsed_triplets.get(section).triplets.size(); triplet++) {
					int element = 0;
					// Iterate over the elements of the triplet
					while (element<3) {
						if(var.equals(list_parsed_triplets.get(section).triplets.get(triplet)[element])) {
							for (int other_element=0; other_element<3; other_element++) {
								if(other_element!=element && list_parsed_triplets.get(section).triplets.get(triplet)[other_element].startsWith("?")) {
									// Stock the variables linked to inserted variables by a triplet
									linked_variables.add(list_parsed_triplets.get(section).triplets.get(triplet)[other_element]);
								}
							}
							// Eliminate the triplet which contains the inserted variable from the triplets to add next to the Last query section
							selected_triplets.get(section).triplets.remove(triplet);
							element=3;
						}
						else {
							element +=1;
						}
					}
				}
			}
		}
		HashMap<String, Object> result = new HashMap<String, Object>();
		result.put("linked_variables", linked_variables);
		result.put("selected_triplets", selected_triplets);
		return result;
	}
	
	/*
	 * FUNCTION: Eject the triplets constraining the specified variables (no other variable in the triplet)
	 * @param {ArrayList<String>} vars
	 * @param {ArrayList<TripletParser>} list_parsed_triplets
	 * @return {ArrayList<TripletParser>}
	 */
	public static ArrayList<TripletParser> ejectConstrainingTriplets(ArrayList<String> vars, ArrayList<TripletParser> list_parsed_triplets) {
		ArrayList<TripletParser> selected_triplets = (ArrayList<TripletParser>)list_parsed_triplets.clone();
		// Iterate over the linked variables
		for (String var: vars) {
			// Iterate over the (basic and SPARQL-Service) sections of the first part of the query
			for (int section=0; section< list_parsed_triplets.size(); section++) {
				// Iterate over the list of triplets in the section
				for (int triplet=0; triplet<list_parsed_triplets.get(section).triplets.size(); triplet++) {
					int element = 0;
					// Iterate over the elements of the triplet
					while (element<3) {
						if(var.equals(list_parsed_triplets.get(section).triplets.get(triplet)[element])) {
							boolean constraint_triplet = true;
							for (int other_element=0; other_element<3; other_element++) {
								if(other_element!=element && selected_triplets.get(section).triplets.get(triplet)[other_element].startsWith("?")) {
									// The linked variable is linked to another variable in the triplet
									constraint_triplet = false;								
								}
							}
							// The triplet contains only the linked variable as a variable
							if (constraint_triplet) {
								selected_triplets.get(section).triplets.remove(triplet);
							}
							element = 3;
						}
						else {
							element +=1;
						}
					}
				}
			}
		}
		return selected_triplets;
	}

}
