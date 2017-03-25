package cn.uway.summary.lte;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cn.uway.framework.parser.database.DatabaseParserMultiTemplate;
import cn.uway.summary.lte.format.SqlFormatter;
import cn.uway.util.TimeUtil;

public class LteHDSimpleLabelParser extends LteHDSummaryParser {


	@Override
	protected void executeSummary() throws Exception {
		List<String> sqlList = transformSql();
		if(sqlList.size() == 0)
		{
			LOGGER.debug("汇总任务："+task.getId()+"不需要汇总监控范围外的小时,当前任务时间："
		+TimeUtil.getDateString_yyyyMMddHHmmss(task.getDataTime()));
			return;
		}
		int cdNum = sqlList.size() >= 20 ? 20 : sqlList.size();
		CountDownLatch cdl = new CountDownLatch(sqlList.size());
		ExecutorService es = Executors.newFixedThreadPool(cdNum);
		// if(!sqlList.isEmpty()){// 便于检查sql是否拼接正确
		// LOGGER.debug("first sql="+sqlList.get(0));
		// }
		for (int j = 0; j < sqlList.size(); j++) {
			String sql = sqlList.get(j);
			LOGGER.debug("执行汇总模板总数为:" + sqlList.size() + ";第" + (j + 1) + "个的语句:" + sql);
			String info = "执行汇总模板总数为:" + sqlList.size() + ";第" + (j + 1) + "个的语句.";
			SummaryThread st = new SummaryThread(cdl, info, sql);
			es.submit(st);
		}
		es.shutdown();
		cdl.await();
		// others必须要等其它场景先执行完后才能执行，因为它会依赖其它场景的数据；
		LOGGER.debug("所有语句执行完毕.");
	}


	protected List<String> transformSql() {
		List<String> sqlList = new ArrayList<String>();
		for (DatabaseParserMultiTemplate t : parserTemplate) {
			// 是否为其它类型，如freeway
			List<String> sqls = t.getSqlList();
			for (String sql : sqls) {
				if (!sql.contains("{labelId}")) {
					sqlList.add(SqlFormatter.formatWeekWeightSql(task, sql, t.getTypeId()));
				} else {
					sqlList.addAll(SqlFormatter.formatWeekWeightSqls(task, sql, t.getTypeId()));
				}
				
			}
		}
		return sqlList;
	}
}
