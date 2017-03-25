package cn.uway.summary.lte;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.external.SwitchManager;
import cn.uway.framework.parser.ParseOutRecord;
import cn.uway.summary.util.SummaryUtil;

public class LteLabelToImpalaSummaryParser extends SummaryDBParser{
	private static final Logger LOGGER = LoggerFactory.getLogger(LteLabelToImpalaSummaryParser.class);
	private  List cityIDList = new ArrayList();
	/**缓存江苏电信用户imsi key:imsi,value:imsi**/
	private static HashMap<String,String> jiangsuImsiUser = new HashMap<String,String>();
	/**缓存已知的所有购物顷向类型（小类）**/
	private static HashMap<String,String> shoping_tendency_type = new HashMap<String,String>();
	private  String jiangsuImisFile = "./template/parser/summary/jiangsuImisFile.txt";
	private  String shopingTendencyFile = "./template/parser/summary/shopingTendencyFile.txt";
	private SwitchManager sm = null;
	private static final String[] fields ={"career_name","work_place","mall","tourist","entertainment","sport","hotel","trip_mode","manage_money_way","shoping_tendency"};
	public LteLabelToImpalaSummaryParser(){
		jiangsuImsiUser = SummaryUtil.readTxtFileToMap(jiangsuImisFile);
		shoping_tendency_type = SummaryUtil.readTxtFileToMap(shopingTendencyFile);
	}
	
	/**
	 * 获取下一条解析记录 直接从metaData对象读取数据源的列数、并且将所有的数据都以string的形式存储
	 */
	@Override
	public ParseOutRecord nextRecord() throws Exception {
		this.totalNum++;
		this.singleTable_recordsNum++;
		Map<String, String> data = new HashMap<String, String>();;
		for (int i = 1; i <= columnNum; i++) {
			data.put(metaData.getColumnName(i), replace(resultSet.getString(i)));
		}
		if (!data.isEmpty()) {
			this.parseSucNum++;
		}
		String cityId = data.get("city_id");
		String imsi = data.get("imsi");
		if(imsi.length() < 15 ) return null;
		String imsi_11 = String.valueOf(Long.valueOf(imsi)/10000);
		Map map = sm.getCacheMap("imsiCity", imsi_11);
		/**关联用户归属地与省份**/
		if(null != map){
			data.put("city_id",map.get("belong_area_id").toString());
			data.put("province_name",map.get("belong_province_name").toString());
			data.put("city_name",map.get("belong_area_name").toString());
		}else{
			Map cityIDMap = sm.getCacheCityIdCityNameMap("imsiCity",cityId);
			if(null != cityIDMap){
				data.put("province_name",cityIDMap.get("belong_province_name").toString());
				data.put("city_name",cityIDMap.get("belong_area_name").toString());
			}else{
				//再关联不上就填空
				data.put("city_id",null);
				data.put("province_name",null);
				data.put("city_name",null);
			}
		}

		//江苏电信用户打职业属性标签
		String j_imsi = jiangsuImsiUser.get(imsi.trim());
		if(StringUtils.isNotEmpty(j_imsi)){
			data.put("career_name","白领");
			data.put("work_place","江苏电信");
		}
		
		//是否本地,1本地，0省外
		cityId = data.get("city_id");
		if(!StringUtils.isEmpty(cityId) && cityIDList.contains(cityId)){
			data.put("islocal", "1");
		}else{
			data.put("islocal", "0");
		}
		
		/**
		 * 购物顷向
		 * **/
		convertChopingTendency(shoping_tendency_type,data);
		
		conversionData(data);
		parseField(data);
		ParseOutRecord outRecord = new ParseOutRecord();
		outRecord.setRecord(data);
		outRecord.setType(dataType);
		return outRecord;
	}
	
	/**
	 * 购物顷向转换
	 * @param shoping_tendency_type
	 * @param data
	 */
	private void convertChopingTendency(Map<String,String> shoping_tendency_type ,Map<String,String> data){
		int[] shopingCode = new int[shoping_tendency_type.size()];
		String shopingTencendy = data.get("shoping_tendency");
		if(StringUtils.isNotEmpty(shopingTencendy)){
			//用户购物顷向属性
			String shopingspi[] = shopingTencendy.split(" ");
			for(int i=0; i<shopingspi.length; i++){
				String shoping = shopingspi[i].trim();
				//具有某个购物顷向
				if(shoping_tendency_type.containsKey(shoping)){
					int index =Integer.valueOf( shoping_tendency_type.get(shoping));
					if(index-1 < shoping_tendency_type.size()){
						shopingCode[index-1] = 1;
					}
				}
			}
		}
		
		StringBuffer val = new StringBuffer();
		for(int n=0;n<shopingCode.length;n++){
			val.append(shopingCode[n]);
		}
		data.put("shoping_tendency", val.toString());
	}
	
	/**
	 * 错误值转换
	 * @param map
	 */
	private void parseField(Map<String,String> map){
		String careerName= map.get("career_name");
		if(!StringUtils.isEmpty(careerName) && careerName.contains("医患")){
			map.put("career_name","医生");
		}	
	}
	
	/**
	 * 把重复的值过滤，例：阳大润发超市 阳大润发超市 宿迁宝龙广场 苏宁易购宿迁宝龙店 宿迁宝龙广场 沭阳大润发超市
	 * 阳大润发超市有两个，过滤后只留一个
	 * @param data
	 */
	private void conversionData(Map<String, String> data){
		for(int i = 0 ; i < fields.length ; i++){
			String orgData = data.get(fields[i]);
			String converData = "";
			if(null == orgData){
				continue;
			}
			converData = formatScene(orgData);
			data.put(fields[i], converData);
		}
	}
	
	private String formatScene(String name){
		if(null == name){
			return "";
		}
		String val = "";
		StringBuffer temp = new StringBuffer();
		
		String namespi[] = name.split(" ");
		if(null != namespi && namespi.length > 0){
			for(int i=0 ; i < namespi.length ; i++){
				if(temp.toString().contains(namespi[i])){
					continue;
				}else{
					temp.append(namespi[i]).append(",");
				}
			}
		}
		int index = temp.lastIndexOf(",");
		if(index != -1){
			val = temp.toString().substring(0,index);
		}
		return val;
	}
	

	
	public SwitchManager getSm() {
		return sm;
	}

	public void setSm(SwitchManager sm) {
		this.sm = sm;
	}

	public List getCityIDList() {
		return cityIDList;
	}

	public void setCityIDList(List cityIDList) {
		this.cityIDList = cityIDList;
	}

	public String getJiangsuImisFile() {
		return jiangsuImisFile;
	}

	public void setJiangsuImisFile(String jiangsuImisFile) {
		this.jiangsuImisFile = jiangsuImisFile;
	}


	
	
}
