package cn.uway.summary.extradatacache;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.accessor.DbAccessor;
import cn.uway.framework.connection.DatabaseConnectionInfo;
import cn.uway.summary.lte.context.SummaryConfigMgr;
import cn.uway.util.DbUtil;

/**
 * imsi  省份对应关系缓存 (从impala获取)
 * @author huzq
 *
 */
public class ImsiProvinceCache extends PeriodCache {
	protected static final Logger LOGGER = LoggerFactory.getLogger(ImsiProvinceCache.class);
	protected ImsiProvinceCache() {
		super("IMSI与省份场景数据", DAY);
	}

	public ImsiProvinceCache(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	
	protected String SQL = "select imsi,belong_area_id,belong_area_name,belong_province_name from cfg_imsi_province_relation";

	private static HashMap<String, HashMap<String,String>> Cache = new HashMap<String, HashMap<String,String>>();
	private static HashMap<String, HashMap<String,String>> city_id_name_cache = new HashMap<String, HashMap<String,String>>();

	
	@Override
	protected void load() {
		
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
//			conn = DbPoolManager.getConnection(connectionInfo);
			conn = DbAccessor.getConnection(connectionInfo);
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
		city_id_name_cache.clear();
		while(rs.next()){
			HashMap<String,String> map = new HashMap<String,String>();
			map.put("belong_area_id", rs.getString(2));
			map.put("belong_area_name", rs.getString(3));
			map.put("belong_province_name", rs.getString(4));
			Cache.put(rs.getString(1), map);
			city_id_name_cache.put(rs.getString(2),map);
		}
	}
	

	public static synchronized HashMap<String,String> get(String key) {
		return Cache.containsKey(key)?Cache.get(key):null;
	}
	
	public static synchronized HashMap<String,String> geCityIDNameMap(String key) {
		return city_id_name_cache.containsKey(key)?city_id_name_cache.get(key):null;
	}
	
	
	// 配置表和任务表不在同一个库
		private DatabaseConnectionInfo connectionInfo = SummaryConfigMgr.getImsiProvinceDataServiceDB();

}
