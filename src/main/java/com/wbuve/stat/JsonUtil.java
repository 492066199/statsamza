package com.wbuve.stat;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public enum JsonUtil {
	INS;
	private final String nil = "_"; 
	private final String reqtime = "reqtime";
	private String template_short = "";
	private static final String total = "total";
	private String template = ""; 

	{
		template = buildDimensions();
		template_short = buildSource();
	}
	
	private String buildDimensions(){
		JSONObject o = new JSONObject();
		try {
			for(String t : StatStreamTask.dimensions){
				o.put(t, nil);
			}
			for(String t : StatStreamTask.metrics){
				o.put(t, 0);
			}
			o.put(total, 1);
		}catch (JSONException e){
			
		}
		return o.toString();
	}
	
	
	private String buildSource(){
		JSONObject o = new JSONObject();
		return o.toString();
	}
	
	public JSONObject buildDimensionsJson() throws JSONException{
		JSONObject o = new JSONObject(template);
		o.put(reqtime, System.currentTimeMillis() / 1000);
		return o;
	} 
	
	
	public JSONObject buildSourceJson() throws JSONException{
		JSONObject o = new JSONObject(template_short);
		return o;
	} 
	
	public static void main(String[] args) throws JSONException {
		System.out.println(JsonUtil.INS.buildDimensionsJson().toString());
		System.out.println(JsonUtil.INS.buildSourceJson().toString());
	}
}
