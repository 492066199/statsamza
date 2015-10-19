package com.wbuve.stat;

import java.util.List;

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

public class StatStreamTask  implements StreamTask {
	public static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "stat_compute");
	public static final char level_1_FS = 0x1c;

	@Override
	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
			throws Exception {
		String message = (String) envelope.getMessage();
		String result = parseStatLog(message);
		if(result != null){
			collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, result));
		}
	}
	
	public String parseStatLog(String message) throws JSONException{
		JSONObject json = new JSONObject();
		int count = 0;
		List<String> title = Lists.newArrayList(Splitter.on(level_1_FS).omitEmptyStrings().split(message));
		
		try {
			for (int i = 0; i < title.size(); i++) {
				String t = title.get(i);
				if (t.indexOf("tmeta_l2:") > -1) {
					
				} else {		
					List<String> tt = Lists.newArrayList(Splitter.on(':').omitEmptyStrings().split(t));
					String key = tt.get(0);
					if((count = key.indexOf('|')) > -1){
						key = key.substring(count + 1, key.length());
						json.put(key, Long.parseLong(tt.get(1)));
					}else {
						json.put(key, tt.get(1));
					}
				}
			}		
		} catch (Exception e) {
			return null;
		}
		return json.toString();
	}
}
