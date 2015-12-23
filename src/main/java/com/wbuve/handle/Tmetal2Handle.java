package com.wbuve.handle;

import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.wbuve.template.Constant;

public class Tmetal2Handle implements IHandleData {
	public static final String category = "category";
	public UserRelationLogHandle userRelationLogHandle = new UserRelationLogHandle();
	
	@Override
	public List<String> handleImpl(String value, JSONObject base)
			throws Exception {
		
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
		
		if(serviceName.equals("account_feed")){
			String r = userRelationLogHandle.handleAccountFeed(base, objs);
			if(r != null){
				rs.add(r);
			}
		}
		if(rs.size() > 0){
			return rs;		
		}
		return null;
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
