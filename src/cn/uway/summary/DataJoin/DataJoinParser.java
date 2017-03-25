package cn.uway.summary.DataJoin;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.xfire.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.accessor.AccessOutObject;
import cn.uway.framework.accessor.JdbcAccessOutObject;
import cn.uway.framework.connection.DatabaseConnectionInfo;
import cn.uway.framework.parser.AbstractParser;
import cn.uway.framework.parser.ParseOutRecord;
import cn.uway.framework.warehouse.exporter.template.DatabaseExporterBean;
import cn.uway.framework.warehouse.exporter.template.DbExportTemplateBean;
import cn.uway.framework.warehouse.exporter.template.DbTableTemplateBean;
import cn.uway.framework.warehouse.exporter.template.ExportTemplateBean;
import cn.uway.summary.DataJoin.DataJoinParserTemplate.JoinerTD;
import cn.uway.summary.DataJoin.DataJoinParserTemplate.JoinerTR;
import cn.uway.summary.DataJoin.DataJoinParserTemplate.JoinerTR.RELATE_JOIN;
import cn.uway.util.DbUtil;
import cn.uway.util.FileUtil;
import cn.uway.util.StringUtil;


public class DataJoinParser extends AbstractParser {
	/**
	 * 日志
	 */
	protected static final Logger LOGGER = LoggerFactory.getLogger(DataJoinParser.class);
	
	/**
	 * 数据库解析模版
	 */
	protected DataJoinParserTemplate parserTemplate;

	/**
	 * 数据库连接 connection 在接入器中关闭)
	 */
	protected Connection connection;

	/**
	 * 采集字段数
	 */
	protected int columnNum;

	/**
	 * 初始化map大小 默认为16 根据采集字段数的多少取2的整数倍
	 */
	protected int stroeMapSize = 16;

	protected final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * 自适应输出表和字段模板标识
	 */
	protected final static String ADAPTATION_TABLE = "adaptation";
	
	protected final static String IGP_TASK_ID_COLUMN_NAME = "IGP_TASK_ID";
	
	protected final static String LEVEL_TYPE_COLUMN_NAME = "NE_LEVEL";
	
	/**
	 * 模板存储路径
	 */
	protected final static String LOCAL_PARSE_TEMPLATE_SAVE_PATH = "./template/parser/summary/nbi";
	
	/**
	 * 模板更新时间间隔
	 */
	protected final static int TEMPLATE_UPDATE_PERIOD = 1 * 60 * 60 * 1000;
	
	/**
	 * 采集任务时间(string类型的)
	 */
	protected String dateTime;
	
	/**
	 * 输出的表名
	 */
	protected String exportTableName;
	
	/**
	 * 汇总粒度
	 */
	protected int summaryGranularity = 0;
	
	/**
	 * 数据集
	 */
	protected DataJoinSet recordsDataSet = null;
	
	protected Integer level_type = null;
	
	protected String task_str = null;
	
	/**
	 * 错误原因 (将在parese方法应该抛出的异常，改到hasNextRecord中抛出，因为在parse方法抛异常，无法在igp_data_gather_obj_status表中插入日志)
	 */
	//protected String errCause;

	/**
	 * Parser关闭方法 Connection在接入器中有关闭，此处不用关闭
	 */
	public void close(){
		this.endTime = new Date();
		
		/**
		 * 销毁记录，回收内存
		 */
		if (recordsDataSet != null && recordsDataSet.records != null) {
			while (recordsDataSet.records.size()> 0) {
				DataJoinRecord record = recordsDataSet.records.remove(0);
				record.destory();
			}
		}
		
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
	 * 判断是否还有更多的记录 DbParser直接使用resultSet.next()即可
	 */
	public boolean hasNextRecord() throws Exception{
		// 将在parse方法应该产生的异常，移到这儿，让igp_data_gather_obj_status能插入日志
		if (this.cause != null)
			throw new Exception(this.cause);
		
		if (recordsDataSet == null)
			return false;
		
		return recordsDataSet.records.size() > 0;
	}

	/**
	 * 获取下一条解析记录 直接从metaData对象读取数据源的列数、并且将所有的数据都以string的形式存储
	 */
	public ParseOutRecord nextRecord() throws Exception{
		this.totalNum++;
		
		DataJoinRecord record = recordsDataSet.records.remove(0);
		String valueLine = record.getRecordLineValue();
		Map<String,String> data = new HashMap<String,String>(stroeMapSize);
		try {
			int prePos = 0;
			int pos = 0;
			pos = valueLine.indexOf(",");
			int nFieldIndex = 0;
			while (pos<=valueLine.length()) {
				if (pos > prePos) {
					String propertyName = recordsDataSet.columnHeader.get(nFieldIndex);
					String value = valueLine.substring(prePos, pos);
					data.put(propertyName, value);
				} 
				++nFieldIndex;
				
				if (pos>=valueLine.length())
					break;
				
				prePos = pos + 1;
				pos = valueLine.indexOf(',', prePos);
				if (pos < 0) {
					pos = valueLine.length();
				}
			}
			
			if(!data.isEmpty()){
				this.parseSucNum++;
			}
			// 增加OMCID COLLECTTIME STAMPTIME字段
			String omc = data.get("OMC");
			if(StringUtil.isEmpty(omc))
				data.put("OMC", String.valueOf(task.getExtraInfo().getOmcId()));
			data.put("COLLECTTIME", getDateString(new Date()));
			data.put("STAMPTIME", dateTime);
			
			/**
			 * 网络级别
			 */
			if (this.level_type != null) {
				data.put(LEVEL_TYPE_COLUMN_NAME, this.level_type.toString());
			}
			
			/**
			 * 采集任务ID
			 */
			data.put(IGP_TASK_ID_COLUMN_NAME, this.task_str);
			
			ParseOutRecord outRecord = new ParseOutRecord();
			outRecord.setRecord(data);
			return outRecord;
		}catch (Exception e) { 
			LOGGER.error("DataJoinParser::nextRecord() 产生异常", e);
		} finally {
			if (record != null) {
				record.destory();
			}
		}
		
		return null;
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
		this.columnNum = 0;
		this.dateTime = getDateString(task.getDataTime());
		this.recordsDataSet = null;
		this.templates = this.updateParseTemplate();
		this.cause = null;
		this.level_type = null;
		this.task_str = ((Long)this.task.getId()).toString();		
		this.parserTemplate = DataJoinParserTemplate.parse(templates);
		if (this.parserTemplate == null) {
			this.cause = "模板解析错误.";
			return;
		}
		
		JdbcAccessOutObject outObject = (JdbcAccessOutObject)accessOutObject;
		connection = outObject.getConnection();
		
		LOGGER.debug("DataJoinParser::parse() join begin... task time:{}", this.dateTime);
		DataJoinner jonner = new DataJoinner();
		for (JoinerTR tr : parserTemplate.trLists) {
			DataJoinSet mergeDataset = null;
			for (JoinerTD td : tr.tdList) {
				String sql = prepareSql(td.sql);
				
				LOGGER.debug("开始加载td数据. sql={}", sql);
				DataJoinSet tdDataSet = jonner.LoadDataFromSql(connection, sql, tr.primaryKeys);
				if (tdDataSet == null) {
					this.cause =  "td数据加载失败.";
					if (mergeDataset != null) 
						mergeDataset.clear();
					
					return;
				}
				LOGGER.debug("td数据加载成功. 数据条数:{}", tdDataSet.records.size());
				if (mergeDataset == null) {
					mergeDataset = tdDataSet;
					continue;
				}
				
				if (tr.relateJoin == RELATE_JOIN.e_left) {
					mergeDataset = jonner.leftJoin(mergeDataset, tdDataSet, tr.primaryKeys, true);
				} else if (tr.relateJoin == RELATE_JOIN.e_right) {
					mergeDataset = jonner.rightJion(mergeDataset, tdDataSet, tr.primaryKeys, true);
				} else if (tr.relateJoin == RELATE_JOIN.e_inner) {
					mergeDataset = jonner.innerJoin(mergeDataset, tdDataSet, tr.primaryKeys, true);
				} else if (tr.relateJoin == RELATE_JOIN.e_full) {
					mergeDataset = jonner.fullJoin(mergeDataset, tdDataSet, tr.primaryKeys, true);
				} else {
					this.cause = "未知的td join关系.";
					if (mergeDataset != null) 
						mergeDataset.clear();
					if (tdDataSet != null) {
						tdDataSet.clear();
					}
					
					return;
				}
			}
			
			if (recordsDataSet == null) {
				recordsDataSet = mergeDataset;
				continue;
			}
			
			// 合并数据
			LOGGER.debug("开始将tr数据集合并到主集中. tr id={}", tr.id);
			recordsDataSet = jonner.UnionRecord(recordsDataSet, mergeDataset, tr.primaryKeys, true);
		}
			
		if (recordsDataSet == null) {
			this.cause = "DataJoinParser::parse() 数据集为null.";
			return;
		}
		
		// 数据排序
		recordsDataSet.sort();
		
		LOGGER.debug("数据连接成功. 数据条数：{}", recordsDataSet.records.size());
		this.columnNum = recordsDataSet.columnHeader.size();
		
		// 设置模板
		List<ExportTemplateBean> exportBeanList = this.getCurrentJob().getExportTemplateBeans();
		List<ExportTemplateBean> validExportBeanList = new ArrayList<ExportTemplateBean>(exportBeanList.size()/2);
		for (ExportTemplateBean bean : exportBeanList) {
			if (!(bean instanceof DbExportTemplateBean)) {
				validExportBeanList.add(bean);
				continue;
			}
			
			DbExportTemplateBean dbBean = (DbExportTemplateBean)bean;
			if (dbBean.getDataType() == ParseOutRecord.DEFAULT_DATA_TYPE) {
				DbTableTemplateBean tableBean = dbBean.getTable();
				if (ADAPTATION_TABLE.equalsIgnoreCase(tableBean.getTableName())) {
					// 重设入库表名，即模板名
					this.exportTableName = FileUtil.getFileName(templates).trim();
					if (this.exportTableName.toUpperCase().startsWith("GATHER_")) {
						this.exportTableName = this.exportTableName.substring(7);
						
						String tableName = this.exportTableName.toUpperCase();
						String[] suffixs = {"_D", "_W", "_M",};
						for (String suffix : suffixs) {
							int pos = tableName.lastIndexOf(suffix);
							if (pos >0) {
								if (pos < (tableName.length()-2)) {
									try {
										this.level_type = Integer.parseInt(this.exportTableName.substring(pos+3));
									} catch (NumberFormatException e) {
										this.cause = "入库的表名不符合规范. " + e.getMessage();
										return;
									}
									this.exportTableName = this.exportTableName.substring(0, pos+2);
								}
								
								break;
							}
							
							++summaryGranularity;
						}
					}
					
					LOGGER.debug("使用自适应模板, export id={} 入库表名:{}",  dbBean.getId(), this.exportTableName);
					
					//根据入库的表，构建入库模板
					buildExportTemplate(dbBean);
				}
			}
			
			validExportBeanList.add(dbBean);
		}
		
		if (validExportBeanList.size()>0) {
			this.getCurrentJob().setExportTemplateBeans(validExportBeanList);
		}
	}

	/**
	 * 获取初始化map的大小<br>
	 * 如果columnNum是2的整数倍 则返回columnNum*2,否则大于columnNum的最小的2的整数倍数字
	 * 
	 * @param columnNum
	 * @return 初始化map的大小
	 */
	protected int getInitialMapSize(int columnNum){
		if(columnNum < 16)
			return 16;
		// 数据库字段最多值能有1024个 即2的10次方
		for(int i = 10; i > 3; i--){
			int maxNum = 1 << i;
			if(maxNum <= columnNum){
				return maxNum << 1;
			}
		}
		return 16;
	}

	/**
	 * 将java.util.Date 转换为字符串类型
	 * 
	 * @param date
	 * @return 日期字符串
	 */
	protected static String getDateString(Date date){
		if(date == null)
			return "";
		return dateFormat.format(date);
	}
	
	protected String prepareSql(String sql) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(this.getCurrentDataTime());
		sql = sql.replace("@@YY", String.format("%04d", cal.get(Calendar.YEAR)));
		sql = sql.replace("@@MM", String.format("%02d", cal.get(Calendar.MONTH) + 1));
		sql = sql.replace("@@DD", String.format("%02d", cal.get(Calendar.DAY_OF_MONTH)));
		sql = sql.replace("@@HH", String.format("%02d", cal.get(Calendar.HOUR_OF_DAY)));
		sql = sql.replace("@@MI", String.format("%02d", cal.get(Calendar.MINUTE)));
		sql = sql.replace("@@SS", String.format("%02d", cal.get(Calendar.SECOND)));
		sql = sql.replace("&gt;", ">");
		sql = sql.replace("&lt;", "<");
		
		return sql;
	}
	
	protected void buildExportTemplate(DbExportTemplateBean dbBean) throws Exception {
		DbTableTemplateBean tableBean = dbBean.getTable();
		DatabaseExporterBean dbTargetBean = (DatabaseExporterBean) dbBean.getExportTargetBean();
		DatabaseConnectionInfo connectionInfo = dbTargetBean.getConnectionInfo();
		
		// 重设置入库表名
		tableBean.setTableName(this.exportTableName);
		
		//移除掉默认的column;
		tableBean.removeColumn(ADAPTATION_TABLE);
		
		// 根据数据库存在的字段，设置column name
		Connection connExport = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			Class.forName(connectionInfo.getDriver());
			connExport = DriverManager.getConnection(connectionInfo.getUrl(), connectionInfo.getUserName(), connectionInfo.getPassword());
			statement = connExport.prepareStatement("select * from " + this.exportTableName);
			resultSet = statement.executeQuery();
			ResultSetMetaData metaData = resultSet.getMetaData();
			int metaColumncount = metaData.getColumnCount();
			ArrayList<String> queryColumns = recordsDataSet.getColumnHeader();
			for (int i=1; i<=metaColumncount; ++i) {
				String dbColumnName = metaData.getColumnName(i);
				String format = null;
				
				// 根据入库的字段名，查找和sql结果集相同的属性名
				int queryColumnIndex = 0;
				for (String queryColumnName : queryColumns) {
					if (dbColumnName.equalsIgnoreCase(queryColumnName)) {
						break;
					}
					++queryColumnIndex;
				}
				if (queryColumnIndex>= queryColumns.size())
					continue;
				
				switch (metaData.getColumnType(i)) {
					case Types.DATE :
					case Types.TIMESTAMP :
					case Types.TIME :
						format = "yyyy-mm-dd hh24:mi:ss";
						break;
					default:
						break;
				}
				tableBean.setColumn(dbColumnName, queryColumns.get(queryColumnIndex), format);
			}
			
			tableBean.setColumn(IGP_TASK_ID_COLUMN_NAME, IGP_TASK_ID_COLUMN_NAME);
			if (this.level_type != null) {
				tableBean.setColumn(LEVEL_TYPE_COLUMN_NAME, LEVEL_TYPE_COLUMN_NAME, null);
			}
			
			removeHistoryData(connExport);
		} catch (Exception e) {
			LOGGER.error("DataJoinParser::buildExportTemplate() 错误. 入库表名:{}", this.exportTableName, e);
			throw e;
		} finally {
			DbUtil.close(resultSet, statement, connExport);
		}
	}
	
	protected void removeHistoryData(Connection conn) {
		Date startTime = truncate(this.task.getDataTime(), this.task.getPeriod());
		String beginDateTime =getDateString(startTime);
		String endDateTime = getDateString(getEndDate(startTime, this.task.getPeriod()));
		
		StringBuilder sb = new StringBuilder();
		sb.append("delete from ").append(this.exportTableName);
		sb.append("  where ").append(IGP_TASK_ID_COLUMN_NAME).append(" = ");
		sb.append(this.task_str);
		sb.append(" and (START_TIME>=to_date('").append(beginDateTime).append("', 'yyyy-mm-dd hh24:mi:ss')");
		sb.append(" and START_TIME< to_date('").append(endDateTime).append("', 'yyyy-mm-dd hh24:mi:ss'))");
		
		String removeSql = sb.toString();
		LOGGER.debug("开始执行汇总前数据清除工作. task time:{} sql:{}", this.dateTime, removeSql);
		
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(removeSql);
			boolean bRet = statement.execute();
			LOGGER.debug("执行汇总前入库数据清除工作完成. 执行结果：{}, task time:{}", bRet, this.dateTime);
		} catch (SQLException e) {
			LOGGER.error("执行反入库命令失败 sql=" + removeSql, e);
		} finally {
			DbUtil.close(null, statement, null);
			statement = null;
		}
	}
	
	/**
	 * 依据指定的粒度，截断（清除）部分时间字段数据
	 * 关于周粒度：
	 * 1、每周开始时间为星期日
	 * 2、跨年周算做第2年的周（2013-12-29算2014年第1周）
	 * @param date
	 * @param gran
	 * @return
	 */
	protected static Date truncate(Date date, int period) {
		Calendar calender = Calendar.getInstance();
		calender.setTime(date);
		
		switch (period) {
			case 60 : 		//hour
				calender.set(Calendar.MINUTE, 0);
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case 1440 :		//day
				calender.set(Calendar.HOUR_OF_DAY, 0);
				calender.set(Calendar.MINUTE, 0);
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case 10080 :	//week
				calender.set(Calendar.DAY_OF_WEEK, 2);
				calender.set(Calendar.HOUR_OF_DAY, 0);
				calender.set(Calendar.MINUTE, 0);
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case 44640 :	//month
				calender.set(Calendar.DAY_OF_MONTH, 1);
				calender.set(Calendar.HOUR_OF_DAY, 0);
				calender.set(Calendar.MINUTE, 0);
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			default :
				return calender.getTime();
		}
	}
	
	protected static Date getEndDate(Date date, int period) {
		Calendar calender = Calendar.getInstance();
		calender.setTime(date);
		
		switch (period) {
			case 60 : 		//hour
				calender.add(Calendar.HOUR, 1);
				return calender.getTime();
			case 1440 :		//day
				calender.add(Calendar.DAY_OF_YEAR, 1);
				return calender.getTime();
			case 10080 :	//week
				calender.add(Calendar.WEEK_OF_YEAR, 1);
				return calender.getTime();
			case 44640 :	//month
				calender.add(Calendar.MONTH, 1);
				return calender.getTime();
			default :
				calender.add(Calendar.MINUTE, period);
				return calender.getTime();
		}
	}
	
	protected String updateParseTemplate() {
		String tpl = this.templates;
		if (tpl.startsWith("template/"))
			tpl = this.templates.substring(9);
		
		LOGGER.debug("正在更新采集模板，采集任务模板配置为：{}", tpl);
		String localTpl = null;
		if (tpl.startsWith("http:/")) {
			/**
			 * <pre>
			 * 解析url
			 * 原解析模板配置样式：
			 * 		http://192.168.15.226:8088/Services/SQLBuilderService.asmx
			 * 				?op=GetNbiSQL&nbisqltype=GATHER_MOD_CITY_D
			 * 
			 * </pre>
			 */
			String invokeMethod = null;
			List<String> invokeSoapParamsList = new LinkedList<String>();
			String invokeUrl = null;
			String urlExtendParam = null;
			int nParamPos = tpl.indexOf("?");
			if (nParamPos <= 0) {
				LOGGER.error("采集任务:{} SOAP 解析模板配置错误. 未指定soap接口方法方法和参数. soap url={}", this.task.getId(), tpl);
				return null;
			}
			
			invokeUrl = tpl.substring(0, nParamPos) + "?wsdl";
			urlExtendParam = tpl.substring(nParamPos+1);
			String [] params = urlExtendParam.split("\\&");
			
			// 最少有一个方法名和一个参数名(所以必定url应该有一个&)
			if (params == null || params.length < 1) {
				LOGGER.error("采集任务:{} SOAP 解析模板配置错误. 未指定soap接口方法方法和参数. soap url={}", this.task.getId(), tpl);
				return null;
			}
			
			for (String param : params) {
				if (param.equals("wsdl"))
					continue;
				
				int pos = param.indexOf("=");
				if (pos < 1 || pos >= param.length()) {
					LOGGER.error("采集任务:{} SOAP 解析模板配置错误. url格式不正确. soap url={}", this.task.getId(), tpl);
					return null;						
				}
				
				String key = param.substring(0, pos).trim();
				String value = param.substring(pos+1).trim();
				
				if (key.equalsIgnoreCase("op")) {
					invokeMethod = value;
				} else if (key.equalsIgnoreCase("nbisqltype")) {
					localTpl = LOCAL_PARSE_TEMPLATE_SAVE_PATH + "/" + value + ".xml";
					invokeSoapParamsList.add(value);
				} else {
					invokeSoapParamsList.add(value);
				}
			}
			
			if (invokeMethod == null) {
				LOGGER.error("采集任务:{} SOAP 解析模板配置错误. 未指定soap方法名. soap url={}", this.task.getId(), tpl);
				return null;
			}
			
			if (localTpl == null) {
				LOGGER.error("采集任务:{} SOAP 解析模板配置错误. 未指定关键的参数nbisqltype. soap url={}", this.task.getId(), tpl);
				return null;
			}
			
			if (invokeSoapParamsList.size()<1) {
				LOGGER.error("采集任务:{} SOAP 解析模板配置错误. 未指定soap接口参数. soap url={}", this.task.getId(), tpl);
				return null;
			}
			
			//创建模板存储目录
			File fileLocalTemplateDir = new File(LOCAL_PARSE_TEMPLATE_SAVE_PATH);
			if (!(fileLocalTemplateDir.exists() && fileLocalTemplateDir.isDirectory())) {
				fileLocalTemplateDir.mkdirs();
			}
			
			// 检查本地模板是否存在，且模板是否需要更新(上次成功下载，和本次文件不足1小时不下载)
			File fileLocalTemplate = new File(localTpl);
			boolean bNeedUpdate = true;
			boolean bLocalTemplateExist = false;
			if (fileLocalTemplate.exists() && fileLocalTemplate.isFile()) {
				bLocalTemplateExist = true;
				if (fileLocalTemplate.lastModified() > (System.currentTimeMillis()-TEMPLATE_UPDATE_PERIOD)) {
					bNeedUpdate = false;
				}
			}
			
			if (!bNeedUpdate) {
				LOGGER.debug("采集任务:{} SOAP 解析模板上次下载与当前时间，不足1小时，本次使用本地缓存的模板. soap url={}", this.task.getId(), tpl);
				return localTpl;
			}
			
			// 将soap返回的内容先存储一个临时文件
			String tplTempFile = localTpl + ".download";
			File fileLocalTempTemplate = new File(tplTempFile);
			if (fileLocalTempTemplate.exists() && fileLocalTempTemplate.isFile()) {
				fileLocalTempTemplate.delete();
			}
			
			// 通过访问soap接口，下载模板
			boolean bValidDownload = false;
			LOGGER.debug("采集任务:{} 开始下载模板. soap url={}", this.task.getId(), tpl);
			if (downLoadSoapTemplate(invokeUrl, invokeMethod, invokeSoapParamsList, tplTempFile)) {
				LOGGER.debug("采集任务:{} 模板下载成功. 开始检验模板  soap url={}", this.task.getId(), tpl);
				bValidDownload = (DataJoinParserTemplate.parse(tplTempFile) != null);
				LOGGER.debug("采集任务:{} 模板检验结果:{}", this.task.getId(), bValidDownload);
			} else {
				LOGGER.debug("采集任务:{} 模板下载失败. soap url={}", this.task.getId(), tpl);
			}
			
			// 检查下载的模正确性
			if (!bValidDownload) {
				if (!bLocalTemplateExist) {
					LOGGER.error("采集任务:{} 更新soap服务器模板失败. soap url={}", this.task.getId(), tpl);
					return null;
				} else { 
					LOGGER.error("采集任务:{} 更新soap服务器模板失败，本次采集使用本地模板. soap url={}", this.task.getId(), tpl);
					return localTpl;
				}
			}
			
			// 将新下载的模板更新本地旧模板.
			if (fileLocalTemplate.exists() && fileLocalTemplate.isFile()) {
				fileLocalTemplate.delete();
			}
			
			// 将模板时间文件名换成正式的文件名
			fileLocalTempTemplate.renameTo(fileLocalTemplate);
		} else {
			localTpl = this.templates;
		}
		
		return localTpl;
	}
	
	/**
	 * 访问soap接口，下载模板
	 * @param invokeUrl					soap url
	 * @param invokeMethod				soap method name
	 * @param invokeSoapParamsList		soap invoke params 
	 * @param downloadFileName			save to local filename
	 * @return
	 */
	protected boolean downLoadSoapTemplate(String invokeUrl, String invokeMethod, List<String> invokeSoapParamsList, String downloadFileName) {
		Object[] invokeParam = new Object[invokeSoapParamsList.size()];
		int i = 0;
		for (String p : invokeSoapParamsList) {
			invokeParam[i++] = p;
		}
		
		Client client = null;
		String soapInvokeResult = null;
		FileOutputStream ofs = null;
		try {
			client = new Client(new URL(invokeUrl));
			Object[] result1 = client.invoke(invokeMethod, invokeParam);
			soapInvokeResult = (String)result1[0];
			
			File fileLocalTempTemplate = new File(downloadFileName);
			if (fileLocalTempTemplate.exists() && fileLocalTempTemplate.isFile()) {
				fileLocalTempTemplate.delete();
			}
			
			ofs = new FileOutputStream(fileLocalTempTemplate);
			ofs.write(soapInvokeResult.getBytes());
			
			return true;
		} catch (MalformedURLException e) {
			LOGGER.error("调用soap服务器产生异常.", e);
		} catch (Exception e) {
			LOGGER.error("调用soap服务器产生异常.", e);
		} finally {
			if (client != null) {
				client.close();
			}
			
			if (ofs != null) {
				try {
					ofs.close();
				} catch (IOException e) {}
				ofs = null;
			}
		}
		
		return false;
	}
}
