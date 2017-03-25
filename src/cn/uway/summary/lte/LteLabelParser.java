package cn.uway.summary.lte;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import cn.uway.framework.external.SwitchManager;
import cn.uway.framework.parser.AdaptFileExportDBParser;
import cn.uway.framework.parser.ParseOutRecord;

public class LteLabelParser extends AdaptFileExportDBParser{
	private  List cityIDList = new ArrayList();
	private SwitchManager sm = null;
	/**银行**/
	private static final String BANK_INDEX = "银";
	private static final String BANK_INDEX2 = "储蓄";
	/**证券**/
	private static final String BOND_INDEX = "券";
	/**保险**/
	private static final String INSURANCE_INDEX = "险";
	
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
			// 增加 COLLECTTIME STAMP_TIME字段
			data.put("COLLECTTIME", getDateString(new Date()));
			data.put("STAMP_TIME", dateTime);
			
			
			String cityId = data.get("CITY_ID");
			String imsi = data.get("IMSI");
			String imsi_11 = String.valueOf(Long.valueOf(imsi)/10000);
			Map map = sm.getCacheMap("imsiCity", imsi_11);
			/**imsi关联city_id  lable_oracle_to_hbase用**/
			if(StringUtils.isEmpty(cityId) && null != map){
				data.put("CITY_ID",map.get("belong_area_id").toString());
			}
			/**关联用户归属地与省 **/
			if(null != map){
				data.put("PROVINCE_NAME",map.get("belong_province_name").toString());
				data.put("CITY_NAME",map.get("belong_area_name").toString());
			}
			//是否本地,1本地，0省外
			cityId = data.get("CITY_ID");
			if(!StringUtils.isEmpty(cityId) && cityIDList.contains(cityId)){
				data.put("ISLOCAL", "1");
			}else{
				data.put("ISLOCAL", "0");
			}
			
			//理财方式
			parseManageMoneyWay(data);
		} catch (ArrayIndexOutOfBoundsException e) {
			//添加驱动包里面索引越界异常捕获，将异常数据丢弃掉。
			LOGGER.warn("数据异常，现在是第{}条记录，第{}列，总列数{}，列名{}", new Object[]{this.totalNum, i, columnNum, metaData.getColumnName(i)});
			return null;
		}
		ParseOutRecord outRecord = new ParseOutRecord();
		outRecord.setType(this.parserTemplate.getDataType());
		outRecord.setRecord(data);
		return null;
	}
	
	/**
	 * 理财方式字段转换
	 * 规则:分三大类，银行，保险，券商   每类取值有1、0;1代表有此方式，0代表没此方式，例：用户有银行理财，券商，无保险，值为:101
	 * @param data
	 */
	private void parseManageMoneyWay(Map<String,String> data ){
		String managerMoneyWay = data.get("MANAGE_MONEY_WAY");
		char code[] = {'0','0','0'};
		if(!StringUtils.isEmpty(managerMoneyWay)){
			String mw[] = managerMoneyWay.split(",");
			for(int i = 0;i < mw.length ; i++){
				if(mw[i].contains(BANK_INDEX) || mw[i].contains(BANK_INDEX2)){
					code[0] = '1';
					continue;
				}
				if(mw[i].contains(INSURANCE_INDEX) ){
					code[1] = '1';
					continue;
				}
				if(mw[i].contains(BOND_INDEX) ){
					code[2] = '1';
				}
				if(code.toString().equals("111")){
					break;
				}
			}
		}
		String codeStr = new String(code);
		data.put("MANAGE_MONEY_WAY",codeStr);
	}

	public List getCityIDList() {
		return cityIDList;
	}

	public void setCityIDList(List cityIDList) {
		this.cityIDList = cityIDList;
	}

	public SwitchManager getSm() {
		return sm;
	}

	public void setSm(SwitchManager sm) {
		this.sm = sm;
	}
	
	
}
