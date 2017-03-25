package cn.uway.summary.lte.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.connection.DatabaseConnectionInfo;
import cn.uway.summary.lte.dao.LteHdImsiProvinceConfigDao;
import cn.uway.summary.lte.dao.LteHdSummaryConfigDAO;

/**
 * lte话单汇总配置管理器
 * @author tylerlee @ 2016年3月19日
 */
public class SummaryConfigMgr {

	private static Logger logger = LoggerFactory.getLogger(SummaryConfigMgr.class); // 日志

	/**
	 * 公共配置
	 */
	private static SummarySystemConfig systemConfig;
	
	private static SummarySystemConfig systemConfig2;

	private LteHdSummaryConfigDAO lteHdSummaryConfigDAO;
	
	private LteHdImsiProvinceConfigDao lteHdSummaryImsiProvinceConfigDAO;
	
	private String imsiProvinceSwitch = "none";

	public LteHdSummaryConfigDAO getLteHdSummaryConfigDAO() {
		return lteHdSummaryConfigDAO;
	}

	public void setLteHdSummaryConfigDAO(LteHdSummaryConfigDAO lteHdSummaryConfigDAO) {
		this.lteHdSummaryConfigDAO = lteHdSummaryConfigDAO;
	}
	
	

	public LteHdImsiProvinceConfigDao getLteHdSummaryImsiProvinceConfigDAO() {
		return lteHdSummaryImsiProvinceConfigDAO;
	}

	public void setLteHdSummaryImsiProvinceConfigDAO(
			LteHdImsiProvinceConfigDao lteHdSummaryImsiProvinceConfigDAO) {
		this.lteHdSummaryImsiProvinceConfigDAO = lteHdSummaryImsiProvinceConfigDAO;
	}

	/**
	 * 执行数据systemConfig初始化 如初始化失败 程序将不能启动
	 */
	public void loadDBConfig() {
		if (systemConfig == null ) {
			systemConfig = lteHdSummaryConfigDAO.getCommonConfg();
			logger.debug("读取lte话单汇总配置成功!");
		}
		
		if (systemConfig2 == null && !"none".equals(imsiProvinceSwitch)) {
			systemConfig2 = lteHdSummaryImsiProvinceConfigDAO.getCommonConfg();
			logger.debug("读取imsi 与省份关联 配置成功!");
		}
	}

	/**
	 * 用户配置数据库地址信息
	 * 
	 * @return
	 */
	public static DatabaseConnectionInfo getExtraDataServiceDB() {
		return systemConfig.getConnectionInfo();
	}
	
	/**
	 * 获取imsi 与省份关联所在的数据库地址信息
	 * @return
	 */
	public static DatabaseConnectionInfo getImsiProvinceDataServiceDB() {
		return systemConfig2.getConnectionInfo();
	}

	public String getImsiProvinceSwitch() {
		return imsiProvinceSwitch;
	}

	public void setImsiProvinceSwitch(String imsiProvinceSwitch) {
		this.imsiProvinceSwitch = imsiProvinceSwitch;
	}
	
	
}
