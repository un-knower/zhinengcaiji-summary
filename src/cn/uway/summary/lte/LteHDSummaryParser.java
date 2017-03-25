package cn.uway.summary.lte;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.accessor.AccessOutObject;
import cn.uway.framework.accessor.JdbcAccessOutObject;
import cn.uway.framework.parser.AbstractParser;
import cn.uway.framework.parser.ParseOutRecord;
import cn.uway.framework.parser.database.DatabaseParseMultiTempletParser;
import cn.uway.framework.parser.database.DatabaseParserMultiTemplate;
import cn.uway.summary.lte.format.SqlFormatter;

/**
 * Lte话单数据汇总解析类(从话单原始表->话单中间表)
 * 
 * @author tylerlee @ 2016年3月18日
 */
public class LteHDSummaryParser extends AbstractParser {

	protected static final Logger LOGGER = LoggerFactory.getLogger(LteHDSummaryParser.class);

	protected List<DatabaseParserMultiTemplate> parserTemplate;

	protected Connection connection;

	@Override
	public void parse(AccessOutObject accessOutObject) throws Exception {
		this.startTime = new Date();
		this.task = accessOutObject.getTask();
		this.currentDataTime = this.task.getDataTime();
		// this.dateTime = new SimpleDateFormat("yyyy/MM/dd").format(task.getDataTime());
		this.parserTemplate = DatabaseParseMultiTempletParser.parse(this.templates);
		// this.busType = this.parserTemplate.getBusType();
		JdbcAccessOutObject outObject = (JdbcAccessOutObject) accessOutObject;
		this.connection = outObject.getConnection();
		executeSummary();
	}

	protected void executeSummary() throws Exception {
		List<String> sqlList = new ArrayList<String>();
		for (DatabaseParserMultiTemplate t : parserTemplate) {
			List<String> sqls = t.getSqlList();
			for (String sql : sqls) {
				if (sql.contains("{labelId}")) {
					sqlList.addAll(SqlFormatter.formatLabelRuleSqls(task, sql, t.getTypeId()));
				} else {
					sqlList.add(SqlFormatter.formatLabelRuleSql(task, sql, t.getTypeId()));
				}
			}
		}
		int cdNum = 0;
		cdNum = sqlList.size() >= 50 ? 50 : sqlList.size();
		CountDownLatch cdl = new CountDownLatch(sqlList.size());
		ExecutorService es = Executors.newFixedThreadPool(cdNum);
		for (int j = 0; j < sqlList.size(); j++) {
			String sql = sqlList.get(j);
			String info = "执行汇总模板总数为:" + sqlList.size() + ";第" + (j + 1) + "个的语句.";
			LOGGER.debug(info + ".sql=" + sql);
			SummaryThread st = new SummaryThread(cdl, info, sql);
			es.submit(st);
		}
		es.shutdown();
		cdl.await();
		LOGGER.debug("所有语句执行完毕.");
	}

	@Override
	public boolean hasNextRecord() throws Exception {
		return false;
	}

	@Override
	public ParseOutRecord nextRecord() throws Exception {
		return null;
	}

	@Override
	public List<ParseOutRecord> getAllRecords() {
		return null;
	}

	@Override
	public void close() {

	}

	@Override
	public Date getDataTime(ParseOutRecord outRecord) {
		return null;
	}

	class SummaryThread implements Runnable {

		private String info;

		private String sql;

		private CountDownLatch cdl;

		public SummaryThread(CountDownLatch cdl, String info, String sql) {
			this.info = info;
			this.sql = sql;
			this.cdl = cdl;
		}

		@Override
		public void run() {
			PreparedStatement statement;
			LOGGER.debug(info);
			long start = System.currentTimeMillis();
			try {
				statement = connection.prepareStatement(sql);
				statement.execute();
				long end = System.currentTimeMillis();
				long time = (end - start) / 1000;
				LOGGER.debug(info + "的语句执行完毕，耗时{}Sec.", time);
			} catch (SQLException e) {
				LOGGER.error("执行汇总语句{}时报错." + e, sql);
			} finally {
				if (cdl != null) {
					cdl.countDown();
				}
			}
		}
	}
}