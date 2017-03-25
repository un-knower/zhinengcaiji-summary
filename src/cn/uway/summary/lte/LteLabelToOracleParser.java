package cn.uway.summary.lte;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import cn.uway.framework.parser.AdaptFileExportDBParser;
import cn.uway.framework.parser.ParseOutRecord;

public class LteLabelToOracleParser extends AdaptFileExportDBParser{

	
	
	@Override
	public ParseOutRecord nextRecord() throws Exception {
		this.totalNum++;
		Map<String,String> data = new HashMap<String,String>(stroeMapSize);
		int i = 1;
		try {
			for(; i <= columnNum; i++){
				data.put(metaData.getColumnName(i), replace(resultSet.getString(i)));
			}
			if(!data.isEmpty()){
				this.parseSucNum++;
			}
			data.put("stamp_time", dateTime);
			
			parseField(data);
		} catch (ArrayIndexOutOfBoundsException e) {
			//添加驱动包里面索引越界异常捕获，将异常数据丢弃掉。
			LOGGER.warn("数据异常，现在是第{}条记录，第{}列，总列数{}，列名{}", new Object[]{this.totalNum, i, columnNum, metaData.getColumnName(i)});
			return null;
		}
		ParseOutRecord outRecord = new ParseOutRecord();
		outRecord.setType(this.parserTemplate.getDataType());
		outRecord.setRecord(data);
		return outRecord;
	}
	
	
	/**
	 * 处理值过大
	 * @param map
	 */
	private void parseField(Map<String,String> map){
		String provinceName= map.get("province_name");
		if(null != provinceName && provinceName.length() > 8){
			map.put("province_name",provinceName.substring(0,8));
		}
		String cityName= map.get("city_name");
		if(null != cityName && cityName.length() > 32){
			map.put("city_name",cityName.substring(0,32));
		}
		String terninalName= map.get("terminal_name");
		if(null != terninalName && terninalName.length() > 32){
			map.put("terminal_name",terninalName.substring(0,32));
		}
		String careerName= map.get("career_name");
		if(null != careerName && careerName.length() > 150){
			map.put("career_name",careerName.substring(0,150));
		}
		if(!StringUtils.isEmpty(careerName) && careerName.contains("医患")){
			map.put("career_name","医生");
		}
		String workPlace= map.get("work_place");
		if(null != workPlace && workPlace.length() > 150){
			map.put("work_place",workPlace.substring(0,150));
		}
		String mall= map.get("mall");
		if(null != mall && mall.length() > 150){
			map.put("mall",mall.substring(0,150));
		}
		String tourist= map.get("tourist");
		if(null != tourist && tourist.length() > 150){
			map.put("tourist",tourist.substring(0,150));
		}
		String enteratnment= map.get("entertainment");
		if(null != enteratnment && enteratnment.length() > 150){
			map.put("entertainment",enteratnment.substring(0,150));
		}
		String sport= map.get("sport");
		if(null != sport && sport.length() > 150){
			map.put("sport",sport.substring(0,150));
		}
		
		String hotel= map.get("hotel");
		if(null != hotel && hotel.length() > 150){
			map.put("hotel",hotel.substring(0,149));
		}
		
		String tripMode= map.get("trip_mode");
		if(null != tripMode && tripMode.length() > 150){
			map.put("trip_mode",tripMode.substring(0,150));
		}
		
		String mmway= map.get("manage_money_way");
		if(null != mmway && mmway.length() > 150){
			map.put("manage_money_way",mmway.substring(0,150));
		}
		String sit= map.get("shoping_tendency");
		if(null != sit && sit.length() > 150){
			map.put("shoping_tendency",sit.substring(0,150));
		}
		
	}
}
