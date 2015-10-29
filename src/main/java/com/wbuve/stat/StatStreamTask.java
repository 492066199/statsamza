package com.wbuve.stat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
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

public class StatStreamTask implements StreamTask {
	public static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "test");
	private static final int skip = 1;
	
	private static final String reqtime = "reqtime";
	private static final String source = "source";
	
	public static final Set<String> dimensions = Sets.newHashSet(
			"uid", 
			"platform",
			"version",
			"from",
			"loadmore",
			"mode",
			"feedtype",
			"unread_status"
			); 
	
	public static final Set<String> sourceOption = Sets.newHashSet(
			"count",
			"data",
			"error"
			);
	
	
	
	@Override
	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
			throws Exception {
		String message = (String) envelope.getMessage();
		if(message == null){
			return;
		}			
		String result = parseStatLog(message);
		if(result != null){
			collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, result));
		}
	}
	
	public static String parseStatLog(String msg){
		try {
			JSONObject result = JsonUtil.buildJsonObject();
			Map<String, Integer> registerMap = Maps.newHashMap();
			Integer sourceIndex = 0;
			List<String> st = Lists.newArrayList(Splitter.on('|').omitEmptyStrings().split(msg));
			int count = st.size();
			for(int i = skip; i < count; i++){
				List<String> st1 = Lists.newArrayList(Splitter.on(':').omitEmptyStrings().split(st.get(i)));
				int st1Count = st1.size(); 
				String key = st1.get(0).trim();
				if(st1Count == 2){
					String value = st1.get(1).trim();
					if(reqtime.equals(key)){
						result.put(key, Long.parseLong(value));
					}
					
					if(dimensions.contains(key)){
						result.put(key, value);
					}
					
					if(source.equals(key)){
						String serviceId = value;
						Integer index = registerMap.get(serviceId);
						if(index == null){
							index = sourceIndex;
							registerMap.put(serviceId, index);
							sourceIndex++;
						}
						
						key = JsonUtil.sourceTemplateMap.get(source).get(index);
						result.put(key, serviceId);
					}
				}
				
				if(st1Count > 2 && source.equals(key)){
					String serviceId = st1.get(1).trim();
					String option = st1.get(2).trim();
					String value = st1.get(3).trim();
					
					Integer index = registerMap.get(serviceId);
					if(index == null){
						index = sourceIndex;
						registerMap.put(serviceId, index);
						sourceIndex ++;
					}
					
					if(sourceOption.contains(option)){
						key = JsonUtil.sourceTemplateMap.get(option).get(index);
						if(option.equals("count")){
							result.put(key, Integer.parseInt(value));
						}else {
							result.put(key , value);
						}
					}
					
				}
			}
			
			String feedtype = result.optString("feedtype");
			if(feedtype.startsWith("10009")){
				result.put("feedtype", "friend");
			}else if (feedtype.equals("main")) {
				
			}else if (feedtype.equals("nil")){
				result.put("feedtype", "nil");
			}else if (!feedtype.isEmpty()){
				result.put("feedtype", "other");
			}
			
			return result.toString();
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	
	
	public static void main(String[] args) throws Exception {
		File file = new File("tmp");
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String s = null;
		while ((s = br.readLine()) != null) {
			System.out.println(parseStatLog(s));
		}
		br.close();
	}
}