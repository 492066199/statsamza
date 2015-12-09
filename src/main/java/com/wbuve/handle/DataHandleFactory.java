package com.wbuve.handle;

import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.wbuve.template.Constant;

public enum DataHandleFactory {
	INS;
	private final Map<String ,IHandleData> handles  = Maps.newHashMap();
	public final List<String> handleSort = Lists.newArrayList();
	
	{
		handles.put(Constant.category, new CategoryHandle());
		handles.put(Constant.tmeta_l2, new Tmetal2Handle());
		
		handleSort.add(Constant.tmeta_l2);
		handleSort.add(Constant.category);
	}
	
	public List<String> handle(String key, String value, JSONObject base) throws Exception {
		IHandleData handle = handles.get(key);
		if(handle == null)
			return null;
		return handle.handleImpl(value, base);
	}
}
