package com.wbuve.handle;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.samza.system.SystemStream;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.wbuve.stat.StatStreamTask;
import com.wbuve.template.Constant;

public class Tmetal2Handle implements IHandleData {
	public static final String category = "category";
	public UserRelationLogHandle userRelationLogHandle = new UserRelationLogHandle();
	
	@Override
	public Map<SystemStream, List<String>> handleImpl(String value, JSONObject base, StatStreamTask stream)
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
		
		List<String> rs = Lists.newArrayList();
		if(value == null){
			base.put("category_imp", Constant.nil);
			return null;
		}
		
		List<String> vArray = Lists.newArrayList(Splitter.on(Constant.SOH).omitEmptyStrings().split(value));
		StringBuilder categoryImp = new StringBuilder(); 
		List<JSONObject> objs = handleVArray(vArray, categoryImp);
		if(categoryImp.length() > 0){
			base.put("category_imp", categoryImp.substring(1));
		}else {
			base.put("category_imp", Constant.nil);
		}
		
		String serviceName = base.getString("service_name");
		if(serviceName.equals("main_feed")){
			String productR = base.getString("product_r");
			if(productR.indexOf("AddFans") > - 1){
				String r = userRelationLogHandle.handleMainFeed(base, objs);
				if(r != null){
					rs.add(r);
				}
			}
		}
		
		List<String> tmate2s = Lists.newArrayList();
		for(JSONObject tmate2 : objs){
			JSONObject t = new JSONObject();
			t.put("uid", uid);
			t.put("reqtime", reqtime);
			t.put("platform", platform);
			t.put("service_name", service_name);
			
			t.put("is_unread_pool", is_unread_pool);
			t.put("from", from); 
			t.put("loadmore", loadmore); 
			
			t.put("category", tmate2.optString("category","_"));
			t.put("product", tmate2.optString("product","_"));
			t.put("type", tmate2.optString("type","_"));
			
			t.put("channel", tmate2.optString("channel","_"));
			t.put("position", tmate2.optString("position","_"));
			tmate2s.add(t.toString());
		}
		
		if(tmate2s.size() > 0){
			if(feedsnum != tmate2s.size()){
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
	
	
	
	private List<JSONObject> handleVArray(List<String> vArray, StringBuilder categoryImp) {
		List<JSONObject> objs = Lists.newArrayList();
		for(String v : vArray){
			JSONObject obj = new JSONObject();
			List<String> csArray = Lists.newArrayList(Splitter.on(Constant.GS).omitEmptyStrings().split(v));
			for(String cs : csArray){
				int sp = cs.indexOf(':');
				String subkey = cs.substring(0, sp);
				String subvalue = cs.substring(sp + 1, cs.length());
				if(subkey.equals(category)){
					categoryImp.append('|');
					categoryImp.append(subvalue);
				}
				try {
					obj.put(subkey, subvalue);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
			objs.add(obj);
		}
		return objs;
	}
	
}
