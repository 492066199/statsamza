package com.wbuve.template;

import java.util.Set;

import com.google.common.collect.Sets;

public class Constant {
	public static final char FS = 28;
	public static final char SOH = 1;
	public static final char GS = 29;
	public static final char RS = 30;
	public static final char US = 31;
	public static final String reqtime = "reqtime";
	public static final String category = "category_r";
	public static final String platform = "platform";
	public static final String from = "from";
	public static final String tmeta_l2 = "tmeta_l2";
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
	public static final String total = "total";
	public static final String nil = "_"; 

}
