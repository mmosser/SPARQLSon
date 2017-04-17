package SPARQLSon;

import java.util.ArrayList;
import java.util.HashMap;

public class MappingSet {
	
	ArrayList<String> var_names;
	ArrayList<HashMap<String, String>> mappings;
	
	public MappingSet() {
		this.var_names = new ArrayList<String>();
		this.mappings = new ArrayList<HashMap<String, String>>();
	}
// MM new constructor "copy"
	public MappingSet(MappingSet ms){
		this.var_names = ms.var_names;
		this.mappings = ms.mappings;
	}

	public void addMapping(HashMap<String, String> mapping) {
		mappings.add(mapping);
	}
	
	public void clearMapping() {
		mappings = new ArrayList<HashMap<String, String>>();
	}
	
	public void set_var_names(ArrayList<String> _var_names) {
		this.var_names = _var_names;
	}
	
	public String serializeAsValues() {
		String values_string = "VALUES (";
		for (String name: var_names) {
			values_string += ("?" + name + " ");
		}
		values_string += ") {";
		for (HashMap<String, String> mapping: mappings) {
			String value_string = "(";
			for(String name: var_names) {
				value_string += mapping.get(name) + " ";
			}
			value_string += ") ";
			values_string += value_string;
		}
		values_string += "} ";
		return values_string;
	}
}
