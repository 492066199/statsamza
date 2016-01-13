package com.wbuve.template;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public enum JsonUtil {
	INS;
	private String template = ""; 

	{
		template = buildDimensions();
	}
	
	private String buildDimensions(){
		JSONObject o = new JSONObject();
		try {
			for(String t : Constant.dimensions){
				o.put(t, Constant.nil);
			}
			for(String t : Constant.metrics){
				o.put(t, 0);
			}
			o.put("hc", "0");
		}catch (JSONException e){
			
		}
		return o.toString();
	}
	
	public JSONObject buildDimensionsJson() throws JSONException{
		JSONObject o = new JSONObject(template);
		o.put(Constant.reqtime, System.currentTimeMillis() / 1000);
		return o;
	} 
	
	public static void main(String[] args) throws JSONException {
		System.out.println(JsonUtil.INS.buildDimensionsJson().toString());
	}
}
