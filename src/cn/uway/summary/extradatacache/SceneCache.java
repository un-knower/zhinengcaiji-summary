package cn.uway.summary.extradatacache;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

import cn.uway.framework.connection.DatabaseConnectionInfo;
import cn.uway.framework.connection.pool.database.DbPoolManager;
import cn.uway.summary.lte.context.SummaryConfigMgr;
import cn.uway.util.DbUtil;

/**
 * 场景信息缓存
 * 
 * @author sunt
 *
 */
public class SceneCache extends PeriodCache {
	protected SceneCache() {
		super("场景数据", HOUR);
	}
	protected SceneCache(String name, long period) {
		super(name, period);
	}

	protected String SQL = "select label_type_id||'-'||label_id||'-'||ne_cell_id key,scene_name from MOD_LHD_LABEL_SIGNAL_SOURCE";
	
	private static HashMap<String, String> Cache = new HashMap<String, String>();

	public static synchronized String get(String key) {
		return Cache.containsKey(key)?Cache.get(key):"";
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
		while(rs.next()){
			Cache.put(rs.getString(1), rs.getString(2));
		}
	}

	// 配置表和任务表不在同一个库
	private DatabaseConnectionInfo connectionInfo = SummaryConfigMgr.getExtraDataServiceDB();
}
