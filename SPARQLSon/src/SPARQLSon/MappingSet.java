package SPARQLSon;

import java.util.ArrayList;
import java.util.HashMap;

public class MappingSet {
	/*
	 * PROPERTIES
	 */
	ArrayList<String> var_names;
	ArrayList<HashMap<String, String>> mappings;
	/*
	 * CONSTRUCTORS
	 */
	public MappingSet() {
		this.var_names = new ArrayList<String>();
		this.mappings = new ArrayList<HashMap<String, String>>();
	}

	public MappingSet(MappingSet ms){
		this.var_names = ms.var_names;
		this.mappings = ms.mappings;
	}
	/*
	 * METHODS
	 */
	
	/* 
	 * FUNCTION: Add a Mapping
	 * @param {HashMap<String, String>} mapping
	 * @return {}
	 */
	public void addMapping(HashMap<String, String> mapping) {
		mappings.add(mapping);
	}
	
	/* 
	 * FUNCTION: Add all the distinct mappings of a MappingSet
	 * @param {MappingSet} ms_temp
	 * @return {}
	 */
	public void addDistinctMappingsFromMappingSet(MappingSet ms_temp) {
		for (int k=0; k<ms_temp.mappings.size(); k++){
			Boolean isDistinct = true;
			for (int l =0; l<mappings.size(); l++) {
				if(ms_temp.mappings.get(k).equals(mappings.get(l))) {
					isDistinct=false;
				}
			}
			if (isDistinct) {
				addMapping(ms_temp.mappings.get(k));
			}
		}
	}

	/* 
	 * FUNCTION: Clear mappings
	 * @return {}
	 */
	public void clearMapping() {
		mappings = new ArrayList<HashMap<String, String>>();
	}
	
	/* 
	 * FUNCTION: Set the var_names
	 * @param {ArrayList<String>} _var_names
	 * @return {}
	 */
	public void set_var_names(ArrayList<String> _var_names) {
		this.var_names = _var_names;
	}
	
	/* 
	 * FUNCTION: Serialize the MappingSet as a VALUE SPARQL Bloc
	 * @return {}
	 */
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
