package com.wbuve.handle;

import java.util.List;

import org.codehaus.jettison.json.JSONObject;

public interface IHandleData {
	List<String> handleImpl(String value, JSONObject base) throws Exception;
}
