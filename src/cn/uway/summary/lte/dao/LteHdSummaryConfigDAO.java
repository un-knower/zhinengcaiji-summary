package cn.uway.summary.lte.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.connection.DatabaseConnectionInfo;
import cn.uway.framework.log.ImportantLogger;
import cn.uway.summary.lte.context.SummarySystemConfig;
import cn.uway.util.DbUtil;

/**
 * lte话单配置数据库汇总
 * @author tylerlee
 * @ 2016年3月19日
 */
public class LteHdSummaryConfigDAO {

	private static Logger logger = LoggerFactory.getLogger(LteHdSummaryConfigDAO.class); // 日志

	/**
	 * 读取lte话单汇总用户配置数据(数据库地址)语句
	 */
	private String sqlForLteHdSummaryUserConfigDB;

	
	public String getSqlForLteHdSummaryUserConfigDB() {
		return sqlForLteHdSummaryUserConfigDB;
	}

	
	public void setSqlForLteHdSummaryUserConfigDB(String sqlForLteHdSummaryUserConfigDB) {
		this.sqlForLteHdSummaryUserConfigDB = sqlForLteHdSummaryUserConfigDB;
	}

	/**
	 * 使用framewoke提供数据库连接数据源
	 */
	private BasicDataSource datasource;

	/**
	 * @return the datasource
	 */
	public BasicDataSource getDatasource() {
		return datasource;
	}

	/**
	 * @param datasource the datasource to set
	 */
	public void setDatasource(BasicDataSource datasource) {
		this.datasource = datasource;
	}

	/**
	 * 查询公共配置 如查询失败 程序终止
	 * 
	 * @return
	 */
	public SummarySystemConfig getCommonConfg() {
		logger.debug("开始执行LTE话单用户配置数据库配置加载!");
		Connection conn = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		SummarySystemConfig summarySystemConfig = null;
		try {
			conn = datasource.getConnection();
			statement = conn.prepareStatement(sqlForLteHdSummaryUserConfigDB);
			rs = statement.executeQuery();
			while (rs.next()) {
				summarySystemConfig = new SummarySystemConfig();
				// 读取外部FTP配置
				summarySystemConfig.setConnectionInfo(getExtraDB(rs));
				logger.debug("LTE话单用户配置数据库配置igp_cfg_connection,IGP_CFG_CONNECTION_DB查询成功!");
			}
			if (summarySystemConfig == null) {
				ImportantLogger.getLogger().warn("LTE话单用户配置数据库配置igp_cfg_connection,IGP_CFG_CONNECTION_DB未配置!");
				ImportantLogger.getLogger().warn("LTE话单汇总程序即将停止!");
				System.exit(0);
				logger.warn("LTE话单汇总程序已停止!请检查配置项后再重新启动!");
			}
		} catch (SQLException e) {
			ImportantLogger.getLogger().warn("LTE话单用户配置数据库配置igp_cfg_connection,IGP_CFG_CONNECTION_DB未配置!");
			ImportantLogger.getLogger().warn("LTE话单汇总程序即将停止!");
			System.exit(0);
			logger.warn("LTE话单汇总程序已停止!请检查配置项后再重新启动!");
		} finally {
			DbUtil.close(rs, statement, conn);
		}
		return summarySystemConfig;
	}

	/**
	 * 从结果集中获取FTP连接信息
	 * 
	 * @param rs
	 * @return ExtraDataServiceFTP
	 */
	private DatabaseConnectionInfo getExtraDB(ResultSet rs) {
		try {
			DatabaseConnectionInfo connInfo = new DatabaseConnectionInfo();
			connInfo.setUserName(rs.getString("USER_NAME"));
			connInfo.setPassword(rs.getString("USER_PWD"));
			connInfo.setDriver(rs.getString("DRIVER"));
			connInfo.setUrl(rs.getString("URL"));
			connInfo.setId(rs.getInt("ID"));
			connInfo.setIp(rs.getString("IP"));
			return connInfo;
		} catch (Exception e) {
			return null;
		}
	}
}
