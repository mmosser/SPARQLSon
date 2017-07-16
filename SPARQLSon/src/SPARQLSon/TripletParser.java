package SPARQLSon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TripletParser {
	
	/*
	 * PROPERTIES
	 */
	public String section_type;
	public ArrayList<String[]> triplets;
	public String service_uri;
	
	/*
	 * CONSTRUCTORS
	 */
	
	public TripletParser(String service_uri, ArrayList<String[]> triplets) {
		if(service_uri!=null) {
			this.section_type = "sparql_service";
			this.service_uri = service_uri;
		}
		else {
			this.section_type = "basic";
			this.service_uri = null;
		}
		this.triplets = triplets;
		this.service_uri = service_uri;
	}
	
	public TripletParser(String service_uri, String[] triplet) {
		if(service_uri!=null) {
			this.section_type = "sparql_service";
			this.service_uri = service_uri;
		}
		else {
			this.section_type = "basic";
			this.service_uri = null;
		}
		this.triplets = new ArrayList<String[]>();
		this.triplets.add(triplet);
	}
	
	public TripletParser(String service_uri, String queryPart) {
		if(service_uri!=null) {
			this.section_type = "sparql_service";
			this.service_uri = service_uri;
		}
		else {
			this.section_type = "basic";
			this.service_uri = null;
		}
		this.triplets = new ArrayList<String[]>();
		String triplet_regex = "(<[\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+>|\\?\\w+|\\w+:\\w+) (<[\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+>|\\?\\w+|\\w+:\\w+) (<[\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+>|\\?\\w+|\\w+:\\w+)";
		Pattern pattern_triplet = Pattern.compile(triplet_regex);
		Matcher matcherTriplet = pattern_triplet.matcher(queryPart);
		while(matcherTriplet.find()) {
			System.out.println("TRIPLET: "+matcherTriplet.group(1)+" "+ matcherTriplet.group(2)+ " "+ matcherTriplet.group(3));
			String[] triplets = new String[]{matcherTriplet.group(1), matcherTriplet.group(2), matcherTriplet.group(3)};
			this.triplets.add(triplets);
		}
	}
	
	public TripletParser(String queryPart) {
		this.section_type = "basic";
		this.service_uri = null;
		this.triplets = new ArrayList<String[]>();
		String triplet_regex = "(<[\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+>|\\?\\w+|\\w+:\\w+) (<[\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+>|\\?\\w+|\\w+:\\w+) (<[\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+>|\\?\\w+|\\w+:\\w+)";
		Pattern pattern_triplet = Pattern.compile(triplet_regex);
		Matcher matcherTriplet = pattern_triplet.matcher(queryPart);
		while(matcherTriplet.find()) {
			System.out.println("TRIPLET: "+matcherTriplet.group(1)+" "+ matcherTriplet.group(2)+ " "+ matcherTriplet.group(3));
			String[] triplets = new String[]{matcherTriplet.group(1), matcherTriplet.group(2), matcherTriplet.group(3)};
			this.triplets.add(triplets);
		}
	}
	
	/*
	 * METHODS
	 */
	public void addTriplet(String[] triplet) {
		this.triplets.add(triplet);
	}
	
	public static ArrayList<TripletParser> getParsedQuery(String firstQuerySection) {
		ArrayList<TripletParser> parsedFirstQuery = new ArrayList<TripletParser>();
		String api_url_string = "(.*) +SERVICE +<([\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+)> *\\{([^\\}]*)\\} *(.*$)";
		Pattern pattern_variables = Pattern.compile(api_url_string);
		String triplet_regex = "(<[\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+>|\\?\\w+|\\w+:\\w+) (<[\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+>|\\?\\w+|\\w+:\\w+) (<[\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+>|\\?\\w+|\\w+:\\w+)";
		Pattern pattern_triplet = Pattern.compile(triplet_regex);
		String query_string = firstQuerySection;
		Matcher m = pattern_variables.matcher(query_string);
		while(m.find()) {
			Matcher matcherTriplet = pattern_triplet.matcher(m.group(4));
			if(matcherTriplet.find()) {
				TripletParser basic_section = new TripletParser(m.group(4));
				parsedFirstQuery.add(0, basic_section);
			}
			TripletParser sparql_service_section = new TripletParser(m.group(2),  m.group(3));
			parsedFirstQuery.add(0, sparql_service_section);
			
			query_string = m.group(1);
			m = pattern_variables.matcher(query_string);
		}
		Matcher matcherTriplet = pattern_triplet.matcher(query_string);
		if(matcherTriplet.find()) {
			TripletParser basic_section = new TripletParser(query_string);
			parsedFirstQuery.add(0, basic_section);
		}
		return parsedFirstQuery;
	}
	
	public static String reverseParsedQuery(ArrayList<TripletParser> parsedTriplets) {
		String query= "";
		for (int i=0; i<parsedTriplets.size();i++) {
			String triplets_string = "";
			for (int j=0; j< parsedTriplets.get(i).triplets.size(); j++) {
				for (int k=0; k<3; k++) {
					triplets_string += parsedTriplets.get(i).triplets.get(j)[k] + " ";
				}
				triplets_string += ". ";
			}
			if(parsedTriplets.get(i).section_type == "sparql_service") {
				query += "SERVICE <" + parsedTriplets.get(i).service_uri + "> {" + triplets_string + "} " ;
			}
			else {
				query += triplets_string;	
			}
		}
		return query;
	}

	
	public static void main(String[] args) throws Exception {
		
		String test =
				  "  ?place ?link <http://dbpedia.org/resource/Chile> ."
				+ "  <http://dbpedia.org/resource/Chile> geo:lat blabla:coucou ."
				+ "  place:l geo:long plouf:opoe ."
				+ "  SERVICE <http://dbpedia.org/sparql> {"
				+ "    ?place ?yipo ?label ."
				+ "	   <http://dbpedia.org/resource/Chile> <http://dbpedia.org/resource/Chile> <http://dbpedia.org/resource/Chile>"
    			+ "  }"
				+ "  ?place geo:lat ?lat ."
				+ "  ?place geo:long ?long ."
				+ "  SERVICE <http://dbpedia.org/sparql> {"
				+ "    ?place rdfs:label ?label ."
				+ "    plouf:opk ?rdfs:label ?label ."
				+ "	   FILTER(lang(?label) = 'es') ."
				+ "	   pojfiunfo;lkco,cin;^zù"
				+ "}";
		
		ArrayList<TripletParser> result = getParsedQuery(test);
		for (int i=0; i< result.size(); i++) {
			System.out.println("-- SECTION n°"+i+" --");
			System.out.println("TYPE: " + result.get(i).section_type);
			for (int j=0; j<result.get(i).triplets.size(); j++) {
				System.out.println("TRIPLET n°"+j+": ");
				for (int k=0; k<result.get(i).triplets.get(j).length; k++) {
					System.out.println(" "+result.get(i).triplets.get(j)[k]);
				}
			}
		}
	}

}
