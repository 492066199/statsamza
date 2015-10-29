package com.wbuve.stat;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Maps;

public class JsonUtil {
	private final static String nil = "nil"; 
	private final static String reqtime = "reqtime";
	private final static String count = "count";
	private final static String source = "source";
	public final static Map<String, Map<Integer, String>> sourceTemplateMap = Maps.newHashMap();
	private static String template = ""; 
	static{
		for (int i = 0; i < 7; i++) {
			Map<Integer, String> numberSourceMap= sourceTemplateMap.get(source);
			
			if(numberSourceMap == null){
				numberSourceMap = Maps.newHashMap();
				sourceTemplateMap.put(source, numberSourceMap);
			}
			numberSourceMap.put(i, source + i);
			
			for (String t : StatStreamTask.sourceOption) {
				StringBuilder sb = new StringBuilder(source);
				sb.append(i);
				sb.append('_');
				sb.append(t);
				Map<Integer, String> numberMap = sourceTemplateMap.get(t);
				if (numberMap == null) {
					numberMap = Maps.newHashMap();
					sourceTemplateMap.put(t, numberMap);
				}
				numberMap.put(i, sb.toString());
			}
		}
		template = buildTemplateString();
	}
	
	public static String buildTemplateString(){
		JSONObject o = new JSONObject();
		try {
			for(String t : StatStreamTask.dimensions){
				o.put(t, nil);
			}
			
			for (String t : StatStreamTask.sourceOption) {
				if(t.equals(count)){
					Map<Integer, String> numberMap = sourceTemplateMap.get(t);
					Set<Entry<Integer, String>> set = numberMap.entrySet();
					Iterator<Entry<Integer, String>> itr= set.iterator();
					while (itr.hasNext()) {
						Entry<Integer, String> entry = itr.next();
						o.put(entry.getValue(), 0);
					}
				}else {
					Map<Integer, String> numberMap = sourceTemplateMap.get(t);
					Set<Entry<Integer, String>> set = numberMap.entrySet();
					Iterator<Entry<Integer, String>> itr= set.iterator();
					while (itr.hasNext()) {
						Entry<Integer, String> entry = itr.next();
						o.put(entry.getValue(), nil);
					}
				}		
			}
			
			Map<Integer, String> numberMap = sourceTemplateMap.get(source);
			Set<Entry<Integer, String>> set = numberMap.entrySet();
			Iterator<Entry<Integer, String>> itr= set.iterator();
			while (itr.hasNext()) {
				Entry<Integer, String> entry = itr.next();
				o.put(entry.getValue(), nil);
			}
		} catch (JSONException e) {
			// TODO: handle exception
		}
		return o.toString();
		
	}
	
	public static JSONObject buildJsonObject() throws JSONException{
		JSONObject o = new JSONObject(template);
		o.put(reqtime, System.currentTimeMillis() / 1000);
		return o;
	} 
	
	public static void main(String[] args) throws JSONException {
		System.out.println(buildJsonObject().toString());
	}
}
