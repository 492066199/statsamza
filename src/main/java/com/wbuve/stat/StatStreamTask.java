package com.wbuve.stat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class StatStreamTask implements StreamTask{
	public final SystemStream dimStream = new SystemStream("kafka1", "uve_stat_handle_s1");
	public final SystemStream sourceStream = new SystemStream("kafka1", "uve_stat_handle_s2");
	private static final char FS = 28;
	private static final String dimensionsKey = "492066199";
	private static final String reqtime = "reqtime";
	private static final String category = "category_r";
	public static final Set<String> dimensions = Sets.newHashSet(			 
			"platform",
			"version",
			"from",
			"loadmore",
			"service_name",
			"product_r"
			); 
	
	public static final Set<String> metrics = Sets.newHashSet(
			"uid",
			"feedsnum"
			);
	
	@Override
	public void process(IncomingMessageEnvelope envelope,
			MessageCollector collector, TaskCoordinator coordinator)
			throws Exception {
		String message = (String) envelope.getMessage();
		if(message == null){
			return;
		}			
		Map<String, JSONObject> result = parseStatLog(message);
		if(result == null){
			return;
		}
		JSONObject dimens = result.get(dimensionsKey);
		if(dimens == null){
			return;
		}
		collector.send(new OutgoingMessageEnvelope(dimStream, dimens.toString()));
		result.remove(dimensionsKey);
		
		Collection<JSONObject> cc = result.values();
		String temp = dimens.toString().replace('}', ',');
		for (JSONObject o : cc) {
			String os = o.toString();
			String or = os.substring(1, os.length());
			collector.send(new OutgoingMessageEnvelope(sourceStream, (temp + or)));
		}
	}
	
	public Map<String, JSONObject> parseStatLog(String msg){
		try {
			JSONObject result = JsonUtil.INS.buildDimensionsJson();
			Map<String, JSONObject> registerMap = Maps.newHashMap();
			registerMap.put(dimensionsKey, result);
			
			msg = removeScribeIp(msg);
			String secondLevelMsg = handleFirstLevel(msg, result);
			if(secondLevelMsg != null){
				handleSecondLevel(secondLevelMsg, registerMap);
			}
			
			return registerMap;
		} catch (Exception e) {
			System.err.println("ERROR msg:" + msg);
			e.printStackTrace();
			return null;
		}
	}
	
	private String handleSecondLevel(String secondLevelMsg, Map<String, JSONObject> registerMap) {
		
		return null;
	}

	private String handleFirstLevel(String msg, JSONObject result) throws NumberFormatException, JSONException {
		String tmeta2 = null;
		List<String> fields = Lists.newArrayList(Splitter.on(FS).omitEmptyStrings().split(msg));
		for(String field : fields){
			int sp = field.indexOf(':');
			String key = field.substring(0, sp);
			String value = field.substring(sp + 1, field.length());
			key = key.trim();
			
			if(reqtime.equals(key)){
				result.put(key, Long.parseLong(value));
			}
			
			if(category.equals(key)){
				result.put(key, value);
				handleCategoryKey(value, result);
			}
			
			if(dimensions.contains(key)){
				result.put(key, value.trim());
			}
			
			if(metrics.contains(key)){
				if(key.equals("uid")){
					result.put(key, Long.parseLong(value.trim()));
				}else if(key.equals("feedsnum")){
					Integer feedsnum = Integer.parseInt(value.trim());
					result.put(key, feedsnum);
					if(feedsnum > 0){
						result.put("hc", "1");
					}else {
						result.put("hc", "0");
					}
				}
			}
			
			if(key.equals("tmeta_l2")){
				tmeta2 = value;
			}
		}		
		System.out.println(result.toString());
		return tmeta2;
	}

	private void handleCategoryKey(String value, JSONObject result) throws JSONException {
		List<String> categorys = Lists.newArrayList(Splitter.on('|').omitEmptyStrings().split(value));
		Map<String, Integer> cmap = Maps.newHashMap();
		for(String c : categorys){
			String key = c.trim();
			Integer count = cmap.get(key);
			if(count == null){
				cmap.put(key, Integer.valueOf(1));
			}else {
				cmap.put(key, count + 1);
			}
		}
		Set<Entry<String, Integer>> entrys = cmap.entrySet();
		for (Entry<String, Integer> entry : entrys) {
			result.put(entry.getKey(), String.valueOf(entry.getValue()));
		}
	}

	private String removeScribeIp(String msg) {
	    int index = msg.indexOf('|');
		return msg.substring(index + 1);
	}

	public static void main(String[] args) throws IOException, JSONException {
		File file = new File("test2");
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String tmp = null;
		while((tmp = br.readLine()) != null){
			new StatStreamTask().parseStatLog(tmp);
		}
		br.close();
	}
}
