package cn.uway.summary.lte.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.connection.DatabaseConnectionInfo;
import cn.uway.framework.connection.pool.database.DbPoolManager;
import cn.uway.framework.external.AbstractCache;
import cn.uway.summary.lte.context.LabelRule;
import cn.uway.summary.lte.context.SummaryConfigMgr;
import cn.uway.summary.lte.context.WeekPercentage;
import cn.uway.util.DbUtil;

/**
 * lte话单汇总用户配置数据缓存
 * 
 * @author tylerlee @ 2016年3月19日
 */
public class LteHdSummaryCache extends AbstractCache implements Runnable {

	private final static Logger LOGGER = LoggerFactory.getLogger(LteHdSummaryCache.class);

	private static LteHdSummaryCache cache = new LteHdSummaryCache();

	// lte话单用户配置数据库连接
	private DatabaseConnectionInfo connectionInfo = SummaryConfigMgr.getExtraDataServiceDB();

	private String sqlForLabelRule = "select LABEL_TYPE_ID,LABEL_ID,LABEL_NAME,MIN_HOUR,MIN_DAYS,MAX_DAYS,DAY_OPRATOR,START_HOUR,END_HOUR,HOUR_OPRATOR from MOD_LHD_LABEL_RULE";

	private String sqlForWeekPercentage = "select LABEL_TYPE_ID,LABEL_TYPE_NAME,WEEK_SEQ,WEIGHT from MOD_LHD_WEEK_PERCENTAGE";

	private String sqlForSignalSourceGroup = "select LABEL_TYPE_ID,LABEL_ID from MOD_LHD_LABEL_SIGNAL_SOURCE group by LABEL_TYPE_ID,LABEL_ID";

	/*
	 * insert into MOD_LHD_LABEL_SIGNAL_SOURCE values(5,6, 174410 ,4,'场景23'); insert into MOD_LHD_LABEL_SIGNAL_SOURCE values(5,6, 114651 ,4,'场景23');
	 */
	/**
	 * 数据库原生连接对象
	 */
	private Connection connection;

	// 周权重map<统计类型id;MOD_LHD_User_f_Statistics.Statistics_TYPE,周权重列表>
	private Map<Integer, List<WeekPercentage>> weekWeightMap;

	// 标签规则map<统计类型id;MOD_LHD_User_f_Statistics.Statistics_TYPE,子类型标签列表>
	private Map<Integer, List<LabelRule>> labelRuleMap;

	// 信源表分类信息，主要是用于功能others的处理；如：漫游用户出行方式的FreeWay,如果labelRuleMap中存在的类型，但在signalSourceGroup中不存在，那么就认为是others
	// <大的场景类型LABEL_TYPE_ID,list 小的标签类型列表LABEL_ID>
	private Map<Integer, List<Integer>> signalSourceGroup;

	/**
	 * 定时器
	 */
	private Timer timer;

	/**
	 * 定时执行线程
	 */
	private Thread executeThread;

	private LteHdSummaryCache() {
		weekWeightMap = new HashMap<Integer, List<WeekPercentage>>();
		labelRuleMap = new HashMap<Integer, List<LabelRule>>();
		signalSourceGroup = new HashMap<Integer, List<Integer>>();
	}

	public static LteHdSummaryCache getInstance() {
		return cache;
	}

	/**
	 * 加载一次，然后开启定时线程
	 */
	public synchronized void startLoadData() {
		loadUserConfigData();
		executeThread = new Thread(this, "LTE话单汇总用户配置数据加载线程");
		executeThread.start();
	}

	@Override
	public void run() {
		LOGGER.debug("LTE话单汇总用户配置数据加载线程启动。");
		timer = new Timer("LTE_HD_SUMMARY Timmer");
		timer.schedule(new ReloadTimerTask(), period, period);
	}

	class ReloadTimerTask extends TimerTask {

		public void run() {
			loadUserConfigData();
		}
	}

	private void loadUserConfigData() {
		loadLabelRule();
		loadWeekWeight();
		// 主要是用于功能others的处理；如：漫游用户出行方式的FreeWay
		loadSignalSourceGroup();
	}

	private void loadLabelRule() {
		PreparedStatement stm = null;
		ResultSet rs = null;
		try {
			connection = getConnection(connectionInfo);
			stm = connection.prepareStatement(sqlForLabelRule);
			rs = stm.executeQuery();
			loadLabelRule(rs);
		} catch (Exception e) {
			LOGGER.error("加载LTE话单汇总标签规则时报错:" + e);
		} finally {
			DbUtil.close(rs, stm, connection);
		}
	}

	private void loadWeekWeight() {
		PreparedStatement stm = null;
		ResultSet rs = null;
		try {
			connection = getConnection(connectionInfo);
			stm = connection.prepareStatement(sqlForWeekPercentage);
			rs = stm.executeQuery();
			loadWeekWeight(rs);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("加载LTE话单汇总周权重时报错:" + e);
		} finally {
			DbUtil.close(rs, stm, connection);
		}
	}

	private void loadSignalSourceGroup() {
		PreparedStatement stm = null;
		ResultSet rs = null;
		try {
			connection = getConnection(connectionInfo);
			stm = connection.prepareStatement(sqlForSignalSourceGroup);
			rs = stm.executeQuery();
			loadSignalSourceGroup(rs);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("加载LTE话单汇总周权重时报错:" + e);
		} finally {
			DbUtil.close(rs, stm, connection);
		}
	}

	private void loadSignalSourceGroup(ResultSet rs) throws Exception {
		Map<Integer, List<Integer>> ssg = new HashMap<Integer, List<Integer>>();
		while (rs.next()) {
			int typeId = rs.getInt("LABEL_TYPE_ID");
			int labelId = rs.getInt("LABEL_ID");
			if (ssg.containsKey(typeId)) {
				ssg.get(typeId).add(labelId);
			} else {
				List<Integer> list = new ArrayList<Integer>();
				list.add(labelId);
				ssg.put(typeId, list);
			}
		}
		setSignalSourceGroup(ssg);
	}

	private void loadWeekWeight(ResultSet rs) throws Exception {
		Map<Integer, List<WeekPercentage>> weekWeights = new HashMap<Integer, List<WeekPercentage>>();
		while (rs.next()) {
			WeekPercentage wp = new WeekPercentage();
			int typeId = rs.getInt("LABEL_TYPE_ID");
			wp.setTypeId(typeId);
			wp.setTypeName(rs.getString("LABEL_TYPE_NAME"));
			wp.setWeekSeq(rs.getInt("WEEK_SEQ"));
			wp.setWeight(rs.getInt("WEIGHT"));
			if (weekWeights.containsKey(typeId)) {
				weekWeights.get(typeId).add(wp);
			} else {
				List<WeekPercentage> list = new ArrayList<WeekPercentage>();
				list.add(wp);
				weekWeights.put(typeId, list);
			}
		}
		setWeekWeightMap(weekWeights);
	}

	private void loadLabelRule(ResultSet rs) throws Exception {
		Map<Integer, List<LabelRule>> labelRules = new HashMap<Integer, List<LabelRule>>();
		while (rs.next()) {
			LabelRule lr = new LabelRule();
			int typeId = rs.getInt("LABEL_TYPE_ID");
			lr.setTypeId(typeId);
			lr.setLabelId(rs.getInt("LABEL_ID"));
			lr.setLabelName(rs.getString("LABEL_NAME"));
			lr.setMinHour(rs.getInt("MIN_HOUR"));
			lr.setMinDays(rs.getInt("MIN_DAYS"));
			lr.setMaxDays(rs.getInt("MAX_DAYS"));
			lr.setDayOprator(rs.getInt("DAY_OPRATOR"));
			lr.setStartHour(rs.getInt("START_HOUR"));
			lr.setEndHour(rs.getInt("END_HOUR"));
			lr.setHourOprator(rs.getInt("HOUR_OPRATOR"));
			if (labelRules.containsKey(typeId)) {
				labelRules.get(typeId).add(lr);
			} else {
				List<LabelRule> list = new ArrayList<LabelRule>();
				list.add(lr);
				labelRules.put(typeId, list);
			}
		}
		setLabelRuleMap(labelRules);
	}

	/**
	 * 使用JDBC创建数据库连接 如果连接创建失败 直接异常退出 由外部捕获
	 * 
	 * @param dbConnectionInfo
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection(DatabaseConnectionInfo dbConnectionInfo) throws SQLException {
		return DbPoolManager.getConnection(dbConnectionInfo);
	}

	public synchronized Map<Integer, List<WeekPercentage>> getWeekWeightMap() {
		return new HashMap<Integer, List<WeekPercentage>>(weekWeightMap);
	}

	public synchronized void setWeekWeightMap(Map<Integer, List<WeekPercentage>> weekWeightMap) {
		this.weekWeightMap.clear();
		this.weekWeightMap.putAll(weekWeightMap);
	}

	public synchronized Map<Integer, List<LabelRule>> getLabelRuleMap() {
		return new HashMap<Integer, List<LabelRule>>(labelRuleMap);
	}

	public synchronized void setLabelRuleMap(Map<Integer, List<LabelRule>> labelRuleMap) {
		this.labelRuleMap.clear();
		this.labelRuleMap.putAll(labelRuleMap);
	}

	public synchronized Map<Integer, List<Integer>> getSignalSourceGroup() {
		return new HashMap<Integer, List<Integer>>(signalSourceGroup);
	}

	public synchronized void setSignalSourceGroup(Map<Integer, List<Integer>> signalSourceGroup) {
		this.signalSourceGroup.clear();
		this.signalSourceGroup.putAll(signalSourceGroup);
	}

}
