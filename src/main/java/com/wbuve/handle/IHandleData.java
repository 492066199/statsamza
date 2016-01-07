package com.wbuve.handle;

import java.util.List;
import java.util.Map;

import org.apache.samza.system.SystemStream;
import org.codehaus.jettison.json.JSONObject;

import com.wbuve.stat.StatStreamTask;

public interface IHandleData {
	Map<SystemStream, List<String>> handleImpl(String value, JSONObject base, StatStreamTask stream) throws Exception;
}
