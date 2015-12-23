package com.wbuve.handle;

import java.text.SimpleDateFormat;
import java.util.List;

import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.wbuve.template.Constant;

public class UserRelationLogHandle {
	private static final String account_containerid = "2306720002_587_2";
	private static final String account_appkey = "631438945";
	private static final String main_containerid = "000001";
	private static final String main_appkey = "3206318534";
	private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public String handleAccountFeed(JSONObject base, List<JSONObject> objs) {
		FilterTool filter = new FilterTool(){

			@Override
			public String getId() {
				return null;
			}

			@Override
			public String getIdType() {
				return null;
			}

			@Override
			public String getResourceid() {
				return "resourceid";
			}

			@Override
			public boolean filter(String type) {
				return true;
			}};
			
		List<String> rs = handle(base, objs, filter, account_appkey, account_containerid);
		if(rs == null){
			return null;
		}else {
			return Joiner.on('\t').join(rs).toString();
		}
				
	}
	
	public String handleMainFeed(JSONObject base, List<JSONObject> objs) {
		FilterTool filter = new FilterTool(){

			@Override
			public String getId() {
				return null;
			}

			@Override
			public String getIdType() {
				return "account_type";
			}

			@Override
			public String getResourceid() {
				return "itemid";
			}

			@Override
			public boolean filter(String type) {
				if(type == null){
					return false;
				}
				if(type.equals("011010")){
					return true;
				}else{
					return false;
				}					
			}};
			
		List<String> rs = handle(base, objs, filter, main_appkey, main_containerid);
		if(rs != null){
			return Joiner.on('\t').join(rs).toString();
		}
		return null;
	}




	private List<String> handle(JSONObject base, List<JSONObject> objs,
			FilterTool filter, String appkey,
			String containerid) {
		
		List<String> rs = Lists.newArrayList();
		long time = System.currentTimeMillis() / 1000;
		int count = 0;
		time = base.optLong("reqtime", time);
		String timeStr = format.format(time * 1000);
		String uid = base.optString("uid");
		StringBuilder sba = new StringBuilder();
		for(JSONObject obj : objs){
			String l3value = obj.optString("tmeta_l3", null);
			boolean r = filter.filter(obj.optString("type", null));
			
			String category_t = obj.optString("category", "bo");
			
			if(l3value != null && r){
				List<String> l3oarray = Lists.newArrayList(Splitter.on(Constant.RS).omitEmptyStrings().split(l3value));
				for(String l3o : l3oarray){
					List<String> l3cs = Lists.newArrayList(Splitter.on(Constant.US).omitEmptyStrings().split(l3o));
					String ss1 = "";  
					String ss2 = "";  
					String ss3 = "";  
					
					for(String l3c : l3cs){						
						int sp = l3c.indexOf(':');
						String l3ckey = l3c.substring(0, sp);
						String l3cvalue = l3c.substring(sp + 1, l3c.length());
						
						if(l3ckey.trim().equals(filter.getResourceid())){
							ss1 = l3cvalue;							
						}
						
						if(l3ckey.trim().equals(filter.getId())){
							ss2 = l3cvalue;
						}
						
						if(l3ckey.trim().equals(filter.getIdType())){
							ss3 = l3cvalue;
						}
						
						if(filter.getIdType() == null){
							ss3 = category_t;
						}
					}
					
					if(ss1.isEmpty()){
						continue;
					}
					
					StringBuilder sbc = new StringBuilder(80);
					sbc.append(ss1);
					sbc.append('|');
					sbc.append(ss2);
					sbc.append('|');
					sbc.append(ss3);
					sba.append(sbc).append(',');
					count ++;
				}
			}
		}
		
		rs.add(timeStr);
		rs.add(uid);
		if(sba.length() > 0){
			rs.add(sba.substring(0, sba.length() - 1).toString());
		}else {
			return null;
		}
		rs.add(appkey);
		rs.add(containerid);
		rs.add("");
		rs.add(String.valueOf(count));
		rs.add("");
		rs.add("");
		return rs;
	}
	
	public interface FilterTool{
		public String getId();
		
		public String getIdType();
		
		public String getResourceid();
		
		public boolean filter(String type);
	}
}
