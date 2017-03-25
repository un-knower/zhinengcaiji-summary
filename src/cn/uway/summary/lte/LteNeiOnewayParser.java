package cn.uway.summary.lte;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.accessor.AccessOutObject;
import cn.uway.framework.accessor.JdbcAccessOutObject;
import cn.uway.framework.orientation.GridOrientation;
import cn.uway.framework.parser.AbstractParser;
import cn.uway.framework.parser.ParseOutRecord;
import cn.uway.framework.parser.database.DatabaseParseTempletParser;
import cn.uway.framework.parser.database.DatabaseParserTemplate;
import cn.uway.util.ArrayMap;
import cn.uway.util.ArrayMapKeyIndex;
import cn.uway.util.DbUtil;
import cn.uway.util.StringUtil;

public class LteNeiOnewayParser extends AbstractParser{
	public static class WayCellInfo {
		/**
		 * 各字段的值 
		 */
		public ArrayMap<String, String> fieldsValue;
		
		/**
		 * 和主小区的距离
		 */
		public Double distance;
		
		/**
		 * 邻区列表
		 */
		public List<WayCellInfo> neiCellList;
		
		public WayCellInfo(ArrayMapKeyIndex<String> keyIndexsMap, int nInitSize) {
			fieldsValue = new ArrayMap<String, String>(keyIndexsMap, false, nInitSize);
			neiCellList = new LinkedList<WayCellInfo>();
		}
		
		public String getKey() {
			String enbID = fieldsValue.get("NE_ENB_ID");
			String cellID = fieldsValue.get("NE_CELL_ID");
			
			if (enbID == null || cellID == null)
				throw new IllegalAccessError("错误的数据，代码可能有BUG．");
			
			return enbID + "_" + cellID;
		}
	}
	
	public static class WayCounterFieldInfo {
		/**
		 * 字段名称
		 */
		public String name;
		
		/**
		 * 字段类型(0:公共字段，　1:主小区信息字段		2:邻区信息字段)
		 */
		public int fieldType;
		
		public WayCounterFieldInfo(String fieldName, boolean isCommonField, boolean isNeiField) {
			this.name = fieldName;
			if (isNeiField && isCommonField)
				throw new IllegalArgumentException("isNeiField和isCommonField不能同时为true");
			else if (isCommonField)
				fieldType = 0;
			else if (isNeiField)
				fieldType = 2;
			else
				fieldType = 1;
		}
	}
	
	public static class WayCounter {
		public int nTimesWayNumber;
		public Double distance;
	}
	
	public class WayCounterDataType {
		public static final int e_OneWayList = 1;
		public static final int e_OneWayCounter = 2;
		public static final int e_TwoWayList = 3;
		public static final int e_TwoWayCounter = 4;
	};
	
	/**
	 * 日志
	 */
	protected static final Logger LOGGER = LoggerFactory.getLogger(LteNeiOnewayParser.class);
	
	/**
	 * 数据库解析模版
	 */
	protected DatabaseParserTemplate parserTemplate;

	/**
	 * 数据库连接
	 */
	protected Connection connection;

	/**
	 * 结果集
	 */
	protected ResultSet resultSet;
	
	/**
	 * 数据库数据时间
	 */
	protected Date dbDataTime;
	
	/**
	 * 采集表元信息
	 */
	protected ResultSetMetaData metaData;

	protected PreparedStatement statement;

	/**
	 * 采集字段数
	 */
	protected int columnNum;

	/**
	 * 初始化map大小 默认为16 根据采集字段数的多少取2的整数倍
	 */
	protected int stroeMapSize = 16;

	protected final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	protected String dateTime;
	
	/**
	 * 键值连接字符
	 */
	protected static final String KEY_JOIN_CHAR = "|";
	
	/**
	 * 公共字段的标识符
	 */
	private static final String COMMON_FIELD_TAG = "CF_";
	
	/**
	 * 邻区字段的标识符
	 */
	private static final String NEI_FIELD_TAG = "NEI_";
	
	/**
	 * 主小区Map迭代器
	 */
	private Iterator<Map.Entry<String, WayCellInfo>> iterWayCellInfo; 
	
	/**
	 * 小区信息基础字段 
	 */
	private List<String> cellBaseFields;
	
	/**
	 * 输出记录缓存列表
	 */
	private List<ParseOutRecord> outRecordListCache = new LinkedList<ParseOutRecord>();
	
	/**
	 * 小区信息列表(key:ne_enb_id + ne_cell_id, value: cellInfo)
	 */
	private Map<String, WayCellInfo> cellInfoMap = new HashMap<String, WayCellInfo>();
	
	/**
	 * onWayCounter(key:BC key, value WayCounter);
	 */
	private Map<String, WayCounter> oneWayOfBCCounter = new HashMap<String, WayCounter>();
	
	/**
	 * twoWayCounter(key:BD key, value WayCounter);
	 */
	private Map<String, WayCounter> twoWayOfBDCounter = new HashMap<String, WayCounter>();
	
	/**
	 * 
	 */
	private Long nCalculatedCellsNumber;
	
	/**
	 * Parser关闭方法 Connection在接入器中有关闭，此处不用关闭
	 */
	public void close(){
		this.endTime = new Date();
		DbUtil.close(resultSet, statement, null);
	}

	/**
	 * 一次解析出所有的数据 暂时不实现
	 */
	public List<ParseOutRecord> getAllRecords(){
		return null;
	}

	/**
	 * 获取真实数据时间 数据库解析一般是周期性任务 可以直接取任务中的数据时间 如果有特殊情况 子类重载该方法
	 */
	public Date getDataTime(ParseOutRecord outRecord){
		return this.task.getDataTime();
	}
	
	/**
	 * Parser初始化 <br>
	 * 1、完成模版解析及转换<br>
	 * 2、数据库的查询
	 */
	public void parse(AccessOutObject accessOutObject) throws Exception{
		this.startTime = new Date();
		this.task = accessOutObject.getTask();
		this.currentDataTime = this.task.getDataTime();
		// 数据时间固定减一天．(因为每次汇总都是汇总前一天的数据,参考sql)
		this.dbDataTime = new Date(this.currentDataTime.getTime() - 24*60*60*1000L);
		this.parserTemplate = DatabaseParseTempletParser.parse(templates);
		JdbcAccessOutObject outObject = (JdbcAccessOutObject)accessOutObject;
		connection = outObject.getConnection();
		String sql = StringUtil.convertCollectPath(parserTemplate.getSql(), this.task.getDataTime());
		LOGGER.debug("开始从邻区网元表中加载数据，SQL:{}", sql);
		statement = connection.prepareStatement(sql);
		resultSet = statement.executeQuery();
		metaData = resultSet.getMetaData();
		this.columnNum = metaData.getColumnCount();
		this.dateTime = getDateString(task.getDataTime());
		
		outRecordListCache.clear();
		cellInfoMap.clear();
		oneWayOfBCCounter.clear();
		twoWayOfBDCounter.clear();
		nCalculatedCellsNumber = 0L;
		
		LOGGER.debug("数据查询成功，开始读取到本地内存.");
		cellBaseFields = new LinkedList<String>();
		int nFieldSize = metaData.getColumnCount();
		// RecordSet索引对应的字段
		WayCounterFieldInfo[] WayCounterFieldIndex = new WayCounterFieldInfo[nFieldSize];
		// 用于给ArrayMap创建索引用．
		ArrayMapKeyIndex<String> keyIndexsMap = new ArrayMapKeyIndex<String>();
		for (int i=1; i<=nFieldSize; ++i) {
			String columnName = metaData.getColumnName(i);
			if (columnName.startsWith(COMMON_FIELD_TAG)) {
				WayCounterFieldIndex[i-1] = new WayCounterFieldInfo(columnName.substring(COMMON_FIELD_TAG.length()), true, false);
				continue;
			} else if (columnName.startsWith(NEI_FIELD_TAG)) {
				WayCounterFieldIndex[i-1] = new WayCounterFieldInfo(columnName.substring(NEI_FIELD_TAG.length()), false, true);
				continue;
			} else {
				WayCounterFieldIndex[i-1] = new WayCounterFieldInfo(columnName, false, false);
				cellBaseFields.add(columnName);
				keyIndexsMap.addKey(columnName);
			}
		}
		
		// 加载数据到cellInfoMap
		int nCellInfoFieldNumber = cellBaseFields.size();
		Long nRecordCount = 0L;
		while (resultSet.next()) {
			String neKey = resultSet.getString(1);		//主小区的key
			//String neiKey = resultSet.getString(2);	//邻小区的key
			Double distance = resultSet.getDouble(3);		//主小区到邻区的距离
			++nRecordCount;

			// 主小区
			WayCellInfo neCellInfo = cellInfoMap.get(neKey);
			boolean bNewMainCell = false;
			if (neCellInfo == null) {
				neCellInfo = new WayCellInfo(keyIndexsMap, nCellInfoFieldNumber);
				cellInfoMap.put(neKey, neCellInfo);
				bNewMainCell = true;
			}
			
			// 邻小区
			WayCellInfo neiCellInfo = new WayCellInfo(keyIndexsMap, nCellInfoFieldNumber);
			for (int i = 4; i<=nFieldSize; ++i) {
				final WayCounterFieldInfo fieldInfo =  WayCounterFieldIndex[i-1];
				if (!bNewMainCell && fieldInfo.fieldType == 1)
					continue;
				
				String value = resultSet.getString(i);
				switch (fieldInfo.fieldType) {
					case 0:
						// 其它公共属性暂时不用处理
						break;
					case 1:
						neCellInfo.fieldsValue.put(fieldInfo.name, value);
						break;
					case 2:
						neiCellInfo.fieldsValue.put(fieldInfo.name, value);
						break;
					default:
						
				}
			}
			neiCellInfo.distance = distance;
			neCellInfo.neiCellList.add(neiCellInfo);
		}
		
		iterWayCellInfo = cellInfoMap.entrySet().iterator();
		
		LOGGER.debug("数据查询成功.共加载到{}条邻区信息. 一共{}个主小区信息.", nRecordCount, cellInfoMap.size());
	}

	/**
	 * 判断是否还有更多的记录 DbParser直接使用resultSet.next()即可
	 */
	public boolean hasNextRecord() throws Exception{
		if (outRecordListCache.size()>0)
			return true;
		
		if (computeNextCell())
			return true;
		
		// oneWay统计
		if (oneWayOfBCCounter.size()>0 && buildOneWayCounterParseOutRecord())
			return true;
		
		// twoWay最后统计
		if (twoWayOfBDCounter.size()>0 && buildTwoWayCounterParseOutRecord())
			return true;
		
		return false;
	}
	
	/**
	 * 获取下一条解析记录 直接从metaData对象读取数据源的列数、并且将所有的数据都以string的形式存储
	 */
	public ParseOutRecord nextRecord() throws Exception{
		return outRecordListCache.remove(0);
	}
	
	public boolean computeNextCell() {
		if (!iterWayCellInfo.hasNext())
			return false;
		
		//oneWayOfBCCounter.clear();
		//twoWayOfBDCounter.clear();
		Long nOneWayListNum = 0l;
		Long nTwoWayListNum = 0l;
		Entry<String, WayCellInfo> entry = iterWayCellInfo.next();
		String mainCellAKey = entry.getKey();
		WayCellInfo mainCellA = entry.getValue();
		
		//one way 运算 (A为源小区； B，C是A的邻区（B，C为不同小区）， B与C的PCI相同，计数One Way路径1次；)
		for (WayCellInfo neiCellB : mainCellA.neiCellList) {
			String neiCellBKey = neiCellB.getKey();
			String pciB = neiCellB.fieldsValue.get("PCI");
			if (pciB==null || pciB.length() < 1)
				continue;
			
			for (WayCellInfo neiCellC : mainCellA.neiCellList) {
				String neiCellCKey = neiCellC.getKey();
				if (neiCellCKey.equals(neiCellBKey))
					continue;
				
				String pciC = neiCellC.fieldsValue.get("PCI");
				if (pciC==null || pciC.length() < 1 || !pciC.equals(pciB))
					continue;
				
				Double AB_Distance = neiCellB.distance;
				Double AC_Distance = neiCellC.distance;
				Double BC_Distance = null;
				// 计算 BC之间的距离
				String bLon = neiCellB.fieldsValue.get("LONGITUDE");
				String bLat = neiCellB.fieldsValue.get("LATITUDE");
				String cLon = neiCellC.fieldsValue.get("LONGITUDE");
				String cLat = neiCellC.fieldsValue.get("LATITUDE");	
				if (!StringUtil.isEmpty(bLon) && !StringUtil.isEmpty(bLat) 
						&& !StringUtil.isEmpty(cLon) && !StringUtil.isEmpty(cLat)) {
					BC_Distance = GridOrientation.CalcDistance(Double.valueOf(bLon), Double.valueOf(bLat), Double.valueOf(cLon), Double.valueOf(cLat));
				}
				
				// 计算OneWay B 和 C小区的路径次数;
				String oneWayOfBCKey = neiCellBKey + KEY_JOIN_CHAR + neiCellCKey;
				WayCounter counter = oneWayOfBCCounter.get(oneWayOfBCKey);
				if (counter == null) {
					counter = new WayCounter();
					counter.distance = BC_Distance;
					oneWayOfBCCounter.put(oneWayOfBCKey, counter);
				}
				++counter.nTimesWayNumber;
				
				// 生成记录
				Map<String, String> record = this.createExportPropertyMap(WayCounterDataType.e_OneWayList);
				record.put("DISTANCE_A_B", toString(AB_Distance));
				record.put("DISTANCE_A_C", toString(AC_Distance));
				record.put("DISTANCE_B_C", toString(BC_Distance));
				
				fillRecordValue(mainCellA, record, "A_");
				fillRecordValue(neiCellB, record,  "B_");
				fillRecordValue(neiCellC, record,  "C_");
				
				record.put("START_TIME", getDateString(this.dbDataTime));
				record.put("TIME_STAMP", getDateString(this.startTime));
				ParseOutRecord outRecord = new ParseOutRecord();
				outRecord.setType(WayCounterDataType.e_OneWayList);
				outRecord.setRecord(record);
				outRecordListCache.add(outRecord);
				++nOneWayListNum;
			}
		}
		
		//two way 运算 (A为主小区，B,C是A的邻区（B，C为不同小区），D为C的邻区（A，B，C，D为不同小区），B与D的PCI相同，计数（B，D）Two Way1次。（即按B,D使用GROUP BY 统计）)
		for (WayCellInfo neiCellB : mainCellA.neiCellList) {
			String pciB = neiCellB.fieldsValue.get("PCI");
			String neiCellBKey = neiCellB.getKey();
			if (pciB==null || pciB.length() < 1)
				continue;
			
			for (WayCellInfo neiCellC : mainCellA.neiCellList) {
				String neiCellCKey = neiCellC.getKey();
				if (neiCellCKey.equals(mainCellAKey) || neiCellCKey.equals(neiCellBKey))
					continue;
				
				WayCellInfo mainCellC = cellInfoMap.get(neiCellCKey);
				if (mainCellC == null)
					continue;
				
				for (WayCellInfo neiCellD : mainCellC.neiCellList) {
					String neiCellDKey = neiCellD.getKey();
					if (neiCellDKey.equals(neiCellCKey) || neiCellDKey.equals(neiCellBKey) || neiCellDKey.equals(mainCellAKey))
						continue;
					
					String pciD = neiCellD.fieldsValue.get("PCI");
					if (pciD==null || pciD.length() < 1 || !pciD.equals(pciB))
						continue;
					
					Double AB_Distance = neiCellB.distance;
					Double AC_Distance = neiCellC.distance;
					Double BC_Distance = null;
					Double BD_Distance = null;
					Double AD_Distance = null;
					Double CD_Distance = neiCellD.distance;
					// 计算 BC之间的距离
					{
						String bLon = neiCellB.fieldsValue.get("LONGITUDE");
						String bLat = neiCellB.fieldsValue.get("LATITUDE");
						String cLon = neiCellC.fieldsValue.get("LONGITUDE");
						String cLat = neiCellC.fieldsValue.get("LATITUDE");	
						if (!StringUtil.isEmpty(bLon) && !StringUtil.isEmpty(bLat) 
								&& !StringUtil.isEmpty(cLon) && !StringUtil.isEmpty(cLat)) {
							BC_Distance = GridOrientation.CalcDistance(Double.valueOf(bLon), Double.valueOf(bLat), Double.valueOf(cLon), Double.valueOf(cLat));
						}
					}
					// 计算 BD之间的距离
					{
						String bLon = neiCellB.fieldsValue.get("LONGITUDE");
						String bLat = neiCellB.fieldsValue.get("LATITUDE");
						String dLon = neiCellD.fieldsValue.get("LONGITUDE");
						String dLat = neiCellD.fieldsValue.get("LATITUDE");	
						if (!StringUtil.isEmpty(bLon) && !StringUtil.isEmpty(bLat) 
								&& !StringUtil.isEmpty(dLon) && !StringUtil.isEmpty(dLat)) {
							BD_Distance = GridOrientation.CalcDistance(Double.valueOf(bLon), Double.valueOf(bLat), Double.valueOf(dLon), Double.valueOf(dLat));
						}
					}
					// 计算 AD之间的距离
					{
						String aLon = mainCellA.fieldsValue.get("LONGITUDE");
						String aLat = mainCellA.fieldsValue.get("LATITUDE");
						String dLon = neiCellD.fieldsValue.get("LONGITUDE");
						String dLat = neiCellD.fieldsValue.get("LATITUDE");	
						if (!StringUtil.isEmpty(aLon) && !StringUtil.isEmpty(aLat) 
								&& !StringUtil.isEmpty(dLon) && !StringUtil.isEmpty(dLat)) {
							AD_Distance = GridOrientation.CalcDistance(Double.valueOf(aLon), Double.valueOf(aLat), Double.valueOf(dLon), Double.valueOf(dLat));
						}
					}
					
					// 计算twoWay B 和 D小区的路径次数;
					String twoWayOfBDKey = neiCellBKey + KEY_JOIN_CHAR + neiCellDKey;
					WayCounter counter = twoWayOfBDCounter.get(twoWayOfBDKey);
					if (counter == null) {
						counter = new WayCounter();
						counter.distance = BD_Distance;
						twoWayOfBDCounter.put(twoWayOfBDKey, counter);
					}
					++counter.nTimesWayNumber;
					
					// 生成记录
					Map<String, String> record = this.createExportPropertyMap(WayCounterDataType.e_TwoWayList);
					record.put("DISTANCE_A_B", toString(AB_Distance));
					record.put("DISTANCE_A_C", toString(AC_Distance));
					record.put("DISTANCE_B_C", toString(BC_Distance));
					record.put("DISTANCE_B_D", toString(BD_Distance));
					record.put("DISTANCE_A_D", toString(AD_Distance));
					record.put("DISTANCE_C_D", toString(CD_Distance));
					
					fillRecordValue(mainCellA, record, "A_");
					fillRecordValue(neiCellB, record,  "B_");
					fillRecordValue(neiCellC, record,  "C_");
					fillRecordValue(neiCellD, record,  "D_");
					
					record.put("START_TIME", getDateString(this.dbDataTime));
					record.put("TIME_STAMP", getDateString(this.startTime));
					ParseOutRecord outRecord = new ParseOutRecord();
					outRecord.setType(WayCounterDataType.e_TwoWayList);
					outRecord.setRecord(record);
					outRecordListCache.add(outRecord);
					++nTwoWayListNum;
				}
			}
		}
		
		if (nCalculatedCellsNumber < 10 || nOneWayListNum > 1000 || nTwoWayListNum > 1000) {
			LOGGER.debug("日志抽样打印小区:{}，邻区个数：{} oneWay[List:{}, counter:{}] twoWay[List:{}, counter:{}]"
					, new Object[] {mainCellA.getKey(), mainCellA.neiCellList.size(), nOneWayListNum, oneWayOfBCCounter.size(), nTwoWayListNum, twoWayOfBDCounter.size()});
		}
		
		if (((++nCalculatedCellsNumber) % 1000) == 0) {
			LOGGER.debug("已成功计算完成{}个小区，总共{}个小区需要计算．", nCalculatedCellsNumber, this.cellInfoMap.size());
		}
		
		if (outRecordListCache.size() > 0)
			return true;
		
		// 如果没有记录，则计算下一个小区
		return computeNextCell();
	}
	
	public boolean buildOneWayCounterParseOutRecord() {
		if (oneWayOfBCCounter.size() > 0) {
			LOGGER.debug("开始计算OneWay路径次数.");
			Iterator<Entry<String, WayCounter>>  iter = oneWayOfBCCounter.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, WayCounter> entry = iter.next();
				String key = entry.getKey();
				WayCounter counter = entry.getValue();
				
				int nPos = key.indexOf(KEY_JOIN_CHAR);
				if (nPos < 0 || nPos >= key.length())
					continue;
				
				String neiCellBKey = key.substring(0, nPos);
				String neiCellCKey = key.substring(nPos+1);
				WayCellInfo neiCellB = cellInfoMap.get(neiCellBKey);
				WayCellInfo neiCellC = cellInfoMap.get(neiCellCKey);
				if (neiCellB == null || neiCellC == null)
					continue;
				
				// 生成记录
				Map<String, String> record = this.createExportPropertyMap(WayCounterDataType.e_OneWayCounter);
				record.put("DISTANCE_B_C", toString(counter.distance));
				record.put("TIMES_B_C", toString(counter.nTimesWayNumber));
				fillRecordValue(neiCellB, record,  "B_");
				fillRecordValue(neiCellC, record,  "C_");
				
				record.put("START_TIME", getDateString(this.dbDataTime));
				record.put("TIME_STAMP", getDateString(this.startTime));
				ParseOutRecord outRecord = new ParseOutRecord();
				outRecord.setType(WayCounterDataType.e_OneWayCounter);
				outRecord.setRecord(record);
				outRecordListCache.add(outRecord);
				
			}
			oneWayOfBCCounter.clear();
		}
		
		return outRecordListCache.size() > 0;
	}
	
	public boolean buildTwoWayCounterParseOutRecord() {
		if (twoWayOfBDCounter.size() > 0) {
			LOGGER.debug("开始计算TwoWay路径次数.");
			Iterator<Entry<String, WayCounter>>  iter = twoWayOfBDCounter.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, WayCounter> entry = iter.next();
				String key = entry.getKey();
				WayCounter counter = entry.getValue();
				
				int nPos = key.indexOf(KEY_JOIN_CHAR);
				if (nPos < 0 || nPos >= key.length())
					continue;
				
				String neiCellBKey = key.substring(0, nPos);
				String neiCellDKey = key.substring(nPos+1);
				WayCellInfo neiCellB = cellInfoMap.get(neiCellBKey);
				WayCellInfo neiCellD = cellInfoMap.get(neiCellDKey);
				if (neiCellB == null || neiCellD == null)
					continue;
				
				// 生成记录
				Map<String, String> record = this.createExportPropertyMap(WayCounterDataType.e_TwoWayCounter);
				record.put("DISTANCE_B_D", toString(counter.distance));
				record.put("TIMES_B_D", toString(counter.nTimesWayNumber));
				fillRecordValue(neiCellB, record,  "B_");
				fillRecordValue(neiCellD, record,  "D_");
				
				record.put("START_TIME", getDateString(this.dbDataTime));
				record.put("TIME_STAMP", getDateString(this.startTime));
				ParseOutRecord outRecord = new ParseOutRecord();
				outRecord.setType(WayCounterDataType.e_TwoWayCounter);
				outRecord.setRecord(record);
				outRecordListCache.add(outRecord);
				
			}
			twoWayOfBDCounter.clear();
		}
		
		return outRecordListCache.size() > 0;
	}
	
	/**
	 * 填充小区信息记录
	 * @param neCell	小区信息对象
	 * @param record	记录对象
	 * @param targetFieldPrefix	目标字段前缀
	 */
	public void fillRecordValue(WayCellInfo neCell, Map<String, String> record, String targetFieldPrefix) {
		if (neCell == null || record == null)
			return;
		
		for (String baseFieldName : cellBaseFields) {
			String keyName = baseFieldName;
			String fieldName = keyName;
			if (targetFieldPrefix != null)
				fieldName = targetFieldPrefix + keyName;
			
			String value = neCell.fieldsValue.get(keyName);
			record.put(fieldName, value);
		}
	}
	
	/**
	 * 将java.util.Date 转换为字符串类型
	 * 
	 * @param date
	 * @return 日期字符串
	 */
	protected String getDateString(Date date){
		if(date == null)
			return "";
		return this.dateFormat.format(date);
	}
	
	protected final static String toString(Double value) {
		if (value == null)
			return null;
		
		return value.toString();
	}
	
	protected final static String toString(Integer value) {
		if (value == null)
			return null;
		
		return value.toString();
	}
}