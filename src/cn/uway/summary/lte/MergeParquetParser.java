package cn.uway.summary.lte;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.uway.framework.parser.database.DatabaseParserMultiTemplate;
import cn.uway.summary.lte.format.SqlFormatter;
import cn.uway.util.parquet.ParqContext;

public class MergeParquetParser extends LteHDSummaryParser {
	
	private static String warehousePath = "/user/impala/lte_hd/";
	
	private int index = 0;

	@Override
	protected void executeSummary() throws Exception {
		List<String> sqlList = transformSql();
		
		String sql = null;
		try {
			Statement statement = connection.createStatement();
			for (int j = 0; j < sqlList.size(); j++) {
				sql = sqlList.get(j);
				LOGGER.debug("执行汇总模板总数为:" + sqlList.size() + ";第" + (j + 1) + "个的语句:" + sql);
				long start = System.currentTimeMillis();
				if(j == index)
				{
					removeFile(sql);
				}else{
					statement.execute(sql);
				}
				long end = System.currentTimeMillis();
				long time = (end - start) / 1000;
				LOGGER.debug("执行汇总模板总数为:" + sqlList.size() + ";第" + (j + 1) + "个语句执行完毕，耗时{}Sec.", time);
			}
		} catch (SQLException e) {
			LOGGER.error("执行汇总语句{}时报错." + e, sql);
		}	 
		
		LOGGER.debug("所有语句执行完毕.");
	}
	
	private void removeFile(String sql) throws Exception
	{
		Map<String,String> map = new HashMap<String,String>();
		try{		
			String[] confs = sql.split(",");
			String[] kv = null;
			for(String c: confs)
			{
				kv = c.split(":");
				map.put(kv[0].toUpperCase(), kv[1]);
			} 
			Path srcPath = new Path(warehousePath+map.get("SRCTABLE")+map.get("PARTITION"));
			Configuration conf = ParqContext.getNewCfg();
			FileSystem fs = FileSystem.newInstance(conf);
			FileStatus[] stats = fs.listStatus(srcPath);
			int count = 0;
			for(int s=0; s<stats.length; s++){
				if(fs.rename(stats[s].getPath(),new Path(stats[s].getPath().toString().replace(map.get("SRCTABLE"), map.get("DSTTABLE"))))){
					count++;
				}
			}
			LOGGER.debug("本次移动文件总数量:"+stats.length+",移动成功的文件数量:"+count);
		}
		catch(Exception e){
			LOGGER.error("将"+warehousePath+map.get("SRCTABLE")+map.get("PARTITION")
			+"下的文件移动到"+map.get("DSTTABLE")+"失败，原因："+e);
			throw e;
		}
	}

	protected List<String> transformSql() {
		List<String> sqlList = new ArrayList<String>();
		DatabaseParserMultiTemplate t = null;
		for (int i=0; i<parserTemplate.size(); i++) {
			t = parserTemplate.get(i);
			List<String> sqls = t.getSqlList();
			if(t.isConf()){
				index = i;
			}
			for (String sql : sqls) {
				sqlList.addAll(SqlFormatter.fill(task, sql, t.getTypeId()));
			}
		}
		return sqlList;
	}
}

