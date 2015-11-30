package com.wbuve.stat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
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

public class StatStreamTask implements StreamTask{
	public final SystemStream dimStream = new SystemStream("kafka", "uve_stat_handle_1");
	public final SystemStream sourceStream = new SystemStream("kafka", "uve_stat_handle_2");
	private static final int skip = 1;
	private static final String dimensionsKey = "492066199";
	private static final String reqtime = "reqtime";
	private static final String source = "source";
	private static final String sourceId = "sourceId";
	private static final String dataunits = "dataunits";
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
						if(key.equals("uid")){
							result.put("uid", Long.parseLong(value));
						}else {
							result.put(key, value);
						}
						
					}
					
					if(source.equals(key)){
						String serviceId = value;
						JSONObject seJson = registerMap.get(serviceId);
						if(seJson == null){
							seJson = JsonUtil.INS.buildSourceJson();
							seJson.put(sourceId, serviceId);
							registerMap.put(serviceId, seJson);
						}
					}
					
					if(dataunits.equals(key)){
						result.put(key, Integer.parseInt(value));
					}
				}
				
				if(st1Count > 2 && source.equals(key)){
					String serviceId = st1.get(1).trim();
					String option = st1.get(2).trim();
					String value = st1.get(3).trim();
					
					JSONObject seJson = registerMap.get(serviceId);
					if(seJson == null){
						seJson = new JSONObject();
						seJson.put(sourceId, serviceId);
						registerMap.put(serviceId, seJson);
					}
					
					if(sourceOption.contains(option)){
						if(option.equals("count")){
							seJson.put("feednum", Integer.parseInt(value));
						}else {
							seJson.put(option , value);
						}
					}
				}
			}
			
			String feedtype = result.optString("feedtype");
			if(feedtype.startsWith("10009")){
				result.put("feedtype", "friend");
			}else if (!feedtype.isEmpty() && !feedtype.equals("nil") && !feedtype.equals("main")){
				result.put("feedtype", "other");
			}
			
			Integer dataunits_t = result.getInt(dataunits); 
			if(dataunits_t > 0){
				result.put("hc", "1");
			}else {
				result.put("hc", "0");
			}
			
			return registerMap;
		} catch (Exception e) {
			System.err.println("ERROR msg:" + msg);
			e.printStackTrace();
			return null;
		}
	}
	
	public static void main(String[] args) throws IOException, JSONException {
		File file = new File("test1");
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		Map<String, List<JSONObject>> map = Maps.newHashMap();
		String tmp = null;
		int count = 0;
		while((tmp = br.readLine()) != null){
			JSONObject obj = new JSONObject(tmp);
			String uid = obj.getString("uid");
			if(map.get(uid) == null){
				map.put(uid, new LinkedList<JSONObject>());
			}
			
			List<JSONObject> j = map.get(uid);
			boolean f = true;
			for(JSONObject i : j){
				if(i.equals(obj)){
					f = false;
				}
			}
			
			if(f){
				j.add(obj);
			}			
//			System.out.print(obj.getString("mode"));
//			System.out.print();
//			System.out.print(obj.getString("loadmore"));
//			System.out.print(obj.getString("feedtype"));
//			System.out.print(obj.getString("from"));
//			System.out.print(obj.getString("unread_status"));
//			System.out.print(obj.getString("version"));
//			System.out.print(obj.getString("platform"));
//			System.out.print(obj.getLong("reqtime"));
		}
		Collection<List<JSONObject>> sj= map.values();
		for(List<JSONObject> k : sj){
			count = count + k.size();
		}
		br.close();
		System.out.println(count);
	}
}
