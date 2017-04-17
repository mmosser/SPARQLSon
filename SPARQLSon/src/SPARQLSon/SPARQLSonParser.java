package SPARQLSon;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SPARQLSonParser {

	public static HashMap<String, Object> parseSPARQLSonQuery(String queryString, boolean replace) {		
		String[] firstParse = getSelectSection(queryString, replace);
		HashMap<String, Object> querySections = getBindSection(firstParse[1]);
		querySections.put("SELECT", firstParse[0]);
		return querySections;
	}
	
	public static String[] getSelectSection(String queryString, boolean replace) {
		String newQueryString = queryString.trim();
		if(replace) {
			newQueryString = newQueryString.replaceAll("\\s+(?=((\\\\[\\\\\"]|[^\\\\\"])*\"(\\\\[\\\\\"]|[^\\\\\"])*\")*(\\\\[\\\\\"]|[^\\\\\"])*$)", " ");
		}
		int cutIndex = newQueryString.indexOf('{');
		String selectSection = newQueryString.substring(0, cutIndex + 1);
		String postSelectSection = newQueryString.substring(cutIndex + 1, newQueryString.length());
		String[] retArray = {selectSection, postSelectSection};
		return retArray;
	}
	
	public static HashMap<String, Object> getBindSection(String postSelectSection) {
		String bind_url_string = " BIND_API <([\\w\\-\\%\\?\\&\\=\\.\\{\\}\\:\\/\\,]+)>(?=((\\\\[\\\\\"]|[^\\\\\"])*\"(\\\\[\\\\\"]|[^\\\\\"])*\")*(\\\\[\\\\\"]|[^\\\\\"])*$)";
		Pattern pattern_variables = Pattern.compile(bind_url_string);
		Matcher m = pattern_variables.matcher(postSelectSection);
		HashMap<String, Object> bindSections = new HashMap<String, Object>();
		if (m.find()) {
		    bindSections.put("URL", m.group(1));
		}
		String[] dividedQuery = postSelectSection.split(bind_url_string, 2);
		bindSections.put("FIRST", dividedQuery[0]);
		String post_bind_string = dividedQuery[1].trim();
		String navigation_string = "(\\))(?=((\\\\[\\\\\"]|[^\\\\\"])*\"(\\\\[\\\\\"]|[^\\\\\"])*\")*(\\\\[\\\\\"]|[^\\\\\"])*$)";
		pattern_variables = Pattern.compile(navigation_string);
		m = pattern_variables.matcher(post_bind_string);
		int loc = -1;
		if (m.find()) {
		    loc = m.start();
		}
		String json_nav_string = post_bind_string.substring(1, loc);
		String post_nav_string = post_bind_string.substring(loc + 1).trim().replaceAll("^AS ", "").trim();
		String[] json_navs = json_nav_string.split(",[\\s]*(?=((\\\\[\\\\\"]|[^\\\\\"])*\"(\\\\[\\\\\"]|[^\\\\\"])*\")*(\\\\[\\\\\"]|[^\\\\\"])*$)");
		
		m = pattern_variables.matcher(post_nav_string);
		loc = -1;
		if (m.find()) {
		    loc = m.start();
		}
		String aliases_string = post_nav_string.substring(1, loc);
		String[] aliases = aliases_string.split(",[\\s]*(?=((\\\\[\\\\\"]|[^\\\\\"])*\"(\\\\[\\\\\"]|[^\\\\\"])*\")*(\\\\[\\\\\"]|[^\\\\\"])*$)");
		for (int i = 0; i < aliases.length; i++) {
			aliases[i] = aliases[i].substring(1);
		}
		String post_aliases_string = post_nav_string.substring(loc + 1).trim();
		bindSections.put("LAST", post_aliases_string);
		bindSections.put("ALIAS", aliases);
		bindSections.put("PATH", json_navs);
		return bindSections;
	}
	
}
