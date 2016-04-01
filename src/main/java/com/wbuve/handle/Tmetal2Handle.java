package com.wbuve.handle;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.samza.system.SystemStream;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.wbuve.stat.StatStreamTask;
import com.wbuve.template.Constant;

public class Tmetal2Handle  {
	
	public UserRelationLogHandle userRelationLogHandle = new UserRelationLogHandle();
	public Map<SystemStream, List<String>> handle(String value, JSONObject base, StatStreamTask stream)
			throws Exception {
		
		Map<SystemStream, List<String>> handleMap = Maps.newHashMap();
		int feedsnum = base.getInt("feedsnum");
		String uid = base.getString("uid");
		long reqtime = base.getLong("reqtime");
		String platform = base.getString("platform");
		String service_name = base.getString("service_name");
		String is_unread_pool = base.getString("is_unread_pool");
		String from = base.getString("from");
		String loadmore = base.getString("loadmore");
		String version = base.getString("version");
		String serviceName = base.getString("service_name");
		
		base.put("category_imp", Constant.nil);
		base.put("product_imp", Constant.nil);
		
		if(value == null){	
			return null;
		}
				
		StringBuilder categoryImp = new StringBuilder(); 
		StringBuilder productImp = new StringBuilder(); 
		List<String> rs = Lists.newArrayList();
		List<String> tmate2s = Lists.newArrayList();

		List<JSONObject> tmetal2Jsons = handletmetal2s(value);
		
		for(JSONObject tmatel2Json : tmetal2Jsons){
			JSONObject t = new JSONObject();
			t.put("uid", uid);
			t.put("reqtime", reqtime);
			t.put("platform", platform);
			t.put("service_name", service_name);
			t.put("version", version);			
			t.put("is_unread_pool", is_unread_pool);
			t.put("from", from); 
			t.put("loadmore", loadmore); 			
			t.put("category", tmatel2Json.optString("category","_"));
			t.put("product", tmatel2Json.optString("product","_"));
			t.put("type", tmatel2Json.optString("type","_"));
			t.put("channel", tmatel2Json.optString("channel","_"));
			t.put("position", tmatel2Json.optString("position","_"));
			
			String tmetaL3 = tmatel2Json.optString("tmeta_l3", "");
			int cardnum = calctmetal3CardNum(tmetaL3);
			t.put("cardnum", cardnum);
			
			String tCategory = tmatel2Json.optString("category", "");
			if(!tCategory.isEmpty()){
				categoryImp.append('|');
				categoryImp.append(tCategory);
			}

			String tProduct = tmatel2Json.optString("product", "");
			if(!tProduct.isEmpty()){
				productImp.append('|');
				productImp.append(tProduct);
			}
			
			if(tProduct.indexOf("AddFans") > -1 && serviceName.equals("main_feed")){
				String r = userRelationLogHandle.handleMainFeed(base, tmatel2Json);
				if(r != null){
					rs.add(r);
				}
			}
			
			tmate2s.add(t.toString());
		}
		
		if(categoryImp.length() > 0){
			base.put("category_imp", categoryImp.substring(1));
		}

		if(productImp.length() > 0){
			base.put("product_imp", productImp.substring(1));
		}
		
		if(tmate2s.size() > 0){
			if(feedsnum != tmate2s.size() && service_name != "user_recommendation"){
				if(RandomUtils.nextInt(10000) == 1){
					System.out.println("not equel message:" + value + " base:" + base.toString());
				}
			}
			handleMap.put(stream.boStream, tmate2s);
		}
		
		if(rs.size() > 0){
			handleMap.put(stream.userStream, rs);		
		}
		
		return handleMap;
	}
	
	
	
	private int calctmetal3CardNum(String tmetal3Str) {
		if(tmetal3Str == null || tmetal3Str.isEmpty()){
			return 0;
		}
		List<String> s = Lists.newArrayList(Splitter.on(Constant.RS).omitEmptyStrings().split(tmetal3Str));
		return s.size();
	}


	private List<JSONObject> handletmetal2s(String value) {
		List<JSONObject> tmetal2Jsons = Lists.newArrayList();
		
		for(String tmetal2 : Splitter.on(Constant.SOH).omitEmptyStrings().split(value)){
			JSONObject tmetal2json = new JSONObject();
			for(String cs : Splitter.on(Constant.GS).omitEmptyStrings().split(tmetal2)){
				int sp = cs.indexOf(':');
				String subkey = cs.substring(0, sp);
				String subvalue = cs.substring(sp + 1, cs.length());
				try {
					tmetal2json.put(subkey, subvalue);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
			tmetal2Jsons.add(tmetal2json);
		}
		return tmetal2Jsons;
	}
}
