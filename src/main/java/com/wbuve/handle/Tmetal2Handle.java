package com.wbuve.handle;

import java.util.List;

import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.wbuve.template.Constant;

public class Tmetal2Handle implements IHandleData {
	public static final String category = "category";
	
	@Override
	public List<String> handleImpl(String value, JSONObject base)
			throws Exception {
		if(value == null){
			base.put("category_imp", Constant.nil);
			return null;
		}
		
		List<String> vArray = Lists.newArrayList(Splitter.on(Constant.SOH).omitEmptyStrings().split(value));
		StringBuilder categoryImp = new StringBuilder(); 
		handleVArray(vArray, categoryImp);
		if(categoryImp.length() > 0){
			base.put("category_imp", categoryImp.substring(1));
		}else {
			base.put("category_imp", Constant.nil);
		}
		return null;		
	}
	
	
	
	private void handleVArray(List<String> vArray, StringBuilder categoryImp) {
		for(String v : vArray){
			List<String> csArray = Lists.newArrayList(Splitter.on(Constant.GS).omitEmptyStrings().split(v));
			for(String cs : csArray){
				int sp = cs.indexOf(':');
				String subkey = cs.substring(0, sp);
				String subvalue = cs.substring(sp + 1, cs.length());
				if(subkey.equals(category)){
					categoryImp.append('|');
					categoryImp.append(subvalue);
				}
			}
		}
	}

}
