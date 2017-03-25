package cn.uway.summary.extradatacache;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cn.uway.framework.connection.DatabaseConnectionInfo;
import cn.uway.framework.connection.pool.database.DbPoolManager;
import cn.uway.summary.lte.context.SummaryConfigMgr;
import cn.uway.util.DbUtil;

/**
 * 信源信息缓存
 * 
 * @author sunt
 *
 */
public class SignalCache extends PeriodCache {
	protected SignalCache() {
		super("信源数据", HOUR);
	}
	protected SignalCache(String name, long period) {
		super(name, period);
	}

//	protected String SQL = "select label_type_id||'-'||label_id key,wmsys.wm_concat(ne_cell_id) ss from MOD_LHD_LABEL_SIGNAL_SOURCE group by label_type_id||'-'||label_id";
	protected String SQL = "select label_type_id||'-'||label_id key,wmsys.wm_concat(ne_cell_id) ss from (select label_type_id,label_id,ne_cell_id,row_number() over(partition by label_type_id,label_id order by ne_cell_id) rn from MOD_LHD_LABEL_SIGNAL_SOURCE) where rn<=1000 group by label_type_id||'-'||label_id";
	
	private static HashMap<String, String> Cache = new HashMap<String, String>();
	private static HashMap<String, List<String>> Cache2 = new HashMap<String, List<String>>();
//	private static Pattern signal_p = Pattern.compile("(\\d+(,\\d+){0,9999})");
	private static final Integer MAX_IN_CHILD = 9999;
	public static synchronized String get(String key) {
		return Cache.containsKey(key)?Cache.get(key):"";
	}
	public static synchronized List<String> get2(String key) {
		return Cache2.containsKey(key)?Cache2.get(key):null;
	}

	@Override
	protected synchronized void load() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = DbPoolManager.getConnection(connectionInfo);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(SQL);
			readCache(rs);
		} catch (Exception e) {
			LOG.error("获取数据库连接失败,msg:{},cause:{}", e.getMessage(), e.getCause());
		} finally {
			DbUtil.close(rs, stmt, conn);
		}
	}

	private void readCache(ResultSet rs) throws Exception {
		Cache.clear();
		Cache2.clear();
		while(rs.next()){
			String name=rs.getString(1);
			String val=rs.getString(2);
			String[] arr = val.split(",");
			if(arr.length>MAX_IN_CHILD){
				StringBuffer sb = new StringBuffer();
				int num =0;
				List<String> ss = new ArrayList<String>();
				for (String str : arr) {
					sb.append(str);
					num++;
					if(num == MAX_IN_CHILD){
						num=0;
						ss.add(sb.toString());
						sb.setLength(0);
					}else{
						sb.append(",");
					}
				}
				ss.add(sb.deleteCharAt(sb.length()-1).toString());
				Cache2.put(name, ss);
				LOG.debug("超过1w个,name:{},arr.size:{}", name, arr.length);
			}else{
				Cache.put(name, val);
			}
		}
	}

	// 配置表和任务表不在同一个库
	private DatabaseConnectionInfo connectionInfo = SummaryConfigMgr.getExtraDataServiceDB();
}
