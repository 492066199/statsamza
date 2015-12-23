package com.wbuve.stat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
import com.wbuve.handle.DataHandleFactory;
import com.wbuve.template.Constant;
import com.wbuve.template.JsonUtil;

public class StatStreamTask implements StreamTask{
	public final SystemStream dimStream = new SystemStream("kafka1", "uve_stat_handle_s1");
	public final SystemStream sourceStream = new SystemStream("kafka1", "uve_stat_req");
	public final SystemStream hcStream = new SystemStream("kafka1", "uve_stat_hc");
	public final SystemStream userStream = new SystemStream("kafka", "uve_user_recommendation_log");
	
	@Override
	public void process(IncomingMessageEnvelope envelope,
			MessageCollector collector, TaskCoordinator coordinator)
			throws Exception {
		String message = (String) envelope.getMessage();
		if(message == null){
			return;
		}			
		Map<SystemStream, List<String>> resultMap = parseStatLog(message);
		if(resultMap == null){
			return;
		}
		
		Set<Entry<SystemStream, List<String>>> resultSets = resultMap.entrySet();
		
		for(Entry<SystemStream, List<String>> r : resultSets){
			List<String> resultList = r.getValue();
			if(resultList != null){
				for(String sr : resultList){
					collector.send(new OutgoingMessageEnvelope(r.getKey(), sr));
				}
			}
		}
	}
	
	public Map<SystemStream, List<String>> parseStatLog(String msg){
		try {
			Map<SystemStream, List<String>> resultMap = Maps.newHashMap();			
			msg = removeScribeIp(msg);
			
			JSONObject base = JsonUtil.INS.buildDimensionsJson();
			Map<String, String> firstLevelBack = handleFirstLevel(msg, base);
			
			for(String handleKey : DataHandleFactory.INS.handleSort){
				List<String> branch = DataHandleFactory.INS.handle(handleKey, firstLevelBack.get(handleKey), base);
				if(branch != null){
					if(Constant.tmeta_l2.equals(handleKey)){
						resultMap.put(userStream, branch);
					}else {
						resultMap.put(sourceStream, branch);
					}
					
				}
			}
			
			resultMap.put(this.dimStream, Lists.newArrayList(base.toString()));
			return resultMap;
		} catch (Exception e) {
			System.err.println("ERROR msg:" + msg);
			e.printStackTrace();
			return null;
		}
	}

	private Map<String, String> handleFirstLevel(String msg, JSONObject result) throws NumberFormatException, JSONException {
		Map<String, String> firstLevelBack = Maps.newHashMap();
		List<String> fields = Lists.newArrayList(Splitter.on(Constant.FS).omitEmptyStrings().split(msg));
		for(String field : fields){
			int sp = field.indexOf(':');
			String key = field.substring(0, sp);
			String value = field.substring(sp + 1, field.length());
			key = key.trim();
			
			if(Constant.reqtime.equals(key)){
				result.put(key, Long.parseLong(value));
			}
			
			if(Constant.category.equals(key)){
				firstLevelBack.put(key, value);
				result.put(key, value);
			}
			
			if(Constant.dimensions.contains(key)){
				result.put(key, value.trim());
			}
			
			if(Constant.metrics.contains(key)){
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
			
			if(key.equals(Constant.tmeta_l2)){
				firstLevelBack.put(key, value);
			}
		}		
		return firstLevelBack;
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
