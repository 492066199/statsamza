package com.wbuve.handle;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.wbuve.template.Constant;

public class CategoryHandle implements IHandleData{

	@Override
	public List<String> handleImpl(String value, JSONObject base) throws JSONException {
		if(value == null){
			return null;
		}
		
		List<String> rs = Lists.newArrayList();
		
		JSONObject object = new JSONObject(base.toString());
		object.remove("product_r");
		object.remove("version");
		object.remove("loadmore");
		object.remove("feedsnum");
		object.remove("hc");
		object.remove("category_r");
		object.remove("total");
		String value1 = object.optString("category_imp", Constant.nil);
		object.remove("category_imp");

		Map<String, Integer> maps = Maps.newHashMap();
		Map<String, Integer> mapsImp = Maps.newHashMap();
		
		List<String> categorys = Lists.newArrayList(Splitter.on('|').omitEmptyStrings().split(value));
		for(String category: categorys){
			Integer count = maps.get(category);
			if(count == null){
				maps.put(category, 1);
			}else {
				maps.put(category, count + 1);
			}
		}
		if(!value1.equals(Constant.nil)){
			List<String> categorysImp = Lists.newArrayList(Splitter.on('|').omitEmptyStrings().split(value1));
			for(String categoryImp: categorysImp){
				Integer countImp = mapsImp.get(categoryImp);
				if(countImp == null){
					mapsImp.put(categoryImp, 1);
				}else {
					mapsImp.put(categoryImp, countImp + 1);
				}
			}
		}
		
		String baseStr = object.toString();
		baseStr = baseStr.substring(0, baseStr.length() - 1);
		Set<Entry<String, Integer>> sets= maps.entrySet();
		for(Entry<String, Integer> set : sets){
			StringBuilder sb = new StringBuilder(baseStr);
			sb.append(",\"category\":\"");
			sb.append(set.getKey());
			sb.append("\",\"req_count\":");
			sb.append(set.getValue());
			
			sb.append(",\"res_count\":");
			
			Integer res_count = mapsImp.get(set.getKey());
			if(res_count == null){
				sb.append(0);
			}else {
				sb.append(res_count);
			}
			sb.append('}');
			rs.add(sb.toString());
		}
		return rs;
	}

}
