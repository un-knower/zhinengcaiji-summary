package cn.uway.summary.extradatacache;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cn.uway.framework.connection.DatabaseConnectionInfo;
import cn.uway.framework.connection.pool.database.DbPoolManager;
import cn.uway.summary.lte.context.LabelRule;
import cn.uway.summary.lte.context.SummaryConfigMgr;
import cn.uway.util.DbUtil;

/**
 * 标签规则信息缓存
 * 
 * @author sunt
 *
 */
public class LabelRuleCache extends PeriodCache {
	protected LabelRuleCache() {
		super("标签规则数据", HOUR);
	}
	protected LabelRuleCache(String name, long period) {
		super(name, period);
	}

	protected String SQL = "select label_type_id,start_hour,end_hour,hour_oprator,min_hour,wmsys.wm_concat(label_id) from mod_lhd_label_rule group by label_type_id,start_hour,end_hour,hour_oprator,min_hour";
	
	private static HashMap<Integer, List<LabelRule>> Cache = new HashMap<Integer, List<LabelRule>>();

	public static synchronized List<LabelRule> get(Integer key) {
		return Cache.get(key);
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
			int typeId = rs.getInt(1);
			List<LabelRule> ll = Cache.get(typeId);
			if(null == ll){
				ll = new ArrayList<LabelRule>();
			}
			LabelRule lr = new LabelRule();
			//label_type_id,start_hour,end_hour,hour_oprator,min_hour
			lr.setTypeId(typeId);
			lr.setStartHour(rs.getInt(2));
			lr.setEndHour(rs.getInt(3));
			lr.setHourOprator(rs.getInt(4));
			lr.setMinHour(rs.getInt(5));
			lr.setLabelIds(rs.getString(6));
			ll.add(lr);
			Cache.put(typeId, ll);
		}
	}

	// 配置表和任务表不在同一个库
	private DatabaseConnectionInfo connectionInfo = SummaryConfigMgr.getExtraDataServiceDB();
}
