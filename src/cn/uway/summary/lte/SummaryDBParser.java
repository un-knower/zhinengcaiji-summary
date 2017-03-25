package cn.uway.summary.lte;

import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.parser.DBParser;
import cn.uway.framework.parser.ParseOutRecord;
import cn.uway.framework.parser.database.DatabaseParserTemplate;
import cn.uway.summary.extradatacache.SceneCache;
import cn.uway.summary.lte.format.SqlFormatter;
import cn.uway.util.DbUtil;
import cn.uway.util.StringUtil;
import cn.uway.util.TimeUtil;

/**
 * 用于汇总sql的处理(从话单中间表->各统计表或者各详情表)
 * 
 * @author tylerlee @ 2016年3月22日
 */
public class SummaryDBParser extends DBParser {

	private static final Logger LOGGER = LoggerFactory.getLogger(SummaryDBParser.class);

	public void initQuery() throws SQLException {
		// 如果不为空，先释放资源，便于垃圾回收
		DbUtil.close(resultSet, statement, null);
		DatabaseParserTemplate template = parserTemplatesList.get(templetIndex);
		String sql = StringUtil.convertCollectPath(template.getSql().trim(), currentDataTime);
		String formatSql = "";
		if (!sql.contains("{labelId}")) {
			formatSql = SqlFormatter.formatWeekWeightSql(task, sql, template.getTypeId());
		} else {
			List<String> sqlList = SqlFormatter.formatWeekWeightSqls(task, sql, template.getTypeId());
			// 组装成一个sql
			StringBuilder sb = new StringBuilder();
			int sizeMinus = sqlList.size() - 1;
			for (int i = 0; i < sizeMinus; i++) {
				sb.append(sqlList.get(i)).append(" union all ");
			}
			formatSql = sb.toString() + sqlList.get(sizeMinus);
		}
		String prefixSql = template.getPrefixSql();
		if (prefixSql != null && !"".equals(prefixSql)) {
			List<String> sqls = SqlFormatter.fill(task, prefixSql, template.getTypeId());
			if(sqls.size() == 0)
			{
				LOGGER.debug("汇总任务："+task.getId()+"不需要汇总监控范围外的小时,当前任务时间："
			+TimeUtil.getDateString_yyyyMMddHHmmss(task.getDataTime()));
				return;
			}
			formatSql= sqls.get(0)+formatSql;
		}
		LOGGER.debug("汇总语句格式化完毕.sql={}", formatSql);
		long start = System.currentTimeMillis();
		statement = connection.prepareStatement(formatSql);
		resultSet = statement.executeQuery();
		metaData = resultSet.getMetaData();
		this.columnNum = metaData.getColumnCount();
		this.templetIndex++;
		this.singleTable_recordsNum = 0;
		this.templetId = template.getId();
		long end = System.currentTimeMillis();
		long time = (end - start) / 1000;
		LOGGER.debug("汇总语句执行完毕，耗时{}Sec.sql={}", time, formatSql);
		// 设置dataType
		setDataType(template.getDataType());
	}

	/**
	 * 获取下一条解析记录 直接从metaData对象读取数据源的列数、并且将所有的数据都以string的形式存储
	 */
	public ParseOutRecord nextRecord() throws Exception {
		this.totalNum++;
		this.singleTable_recordsNum++;
		// Map<String,String> data = new HashMap<String,String>(stroeMapSize);
		Map<String, String> data = new HashMap<String, String>();;
		for (int i = 1; i <= columnNum; i++) {
			data.put(metaData.getColumnName(i), replace(resultSet.getString(i)));
		}
		if (!data.isEmpty()) {
			this.parseSucNum++;
		}
		if (data.containsKey("scene_key")) {
			// 回填场景名
			data.put("scene_name", SceneCache.get(data.get("scene_key")));
		}
		// 增加OMCID COLLECTTIME STAMPTIME字段
		data.put("MMEID", String.valueOf(task.getExtraInfo().getOmcId()));
		data.put("COLLECTTIME", getDateString(new Date()));
		data.put("STAMPTIME", dateTime);
		ParseOutRecord outRecord = new ParseOutRecord();
		outRecord.setRecord(data);
		outRecord.setType(dataType);
		return outRecord;
	}

	public boolean isTableExists(String sql) throws SQLException {
		return true;
	}
	
	/**
	 * 当查询结果为空时不添加补采任务
	 */
	@Override
	public void rememberTempletIds() {}
}
