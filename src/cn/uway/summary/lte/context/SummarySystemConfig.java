package cn.uway.summary.lte.context;

import cn.uway.framework.connection.DatabaseConnectionInfo;

/**
 * 针对lte话单汇总的系统全局配置 多个采集服务器共享
 * @author tylerlee
 * @ 2016年3月19日
 */
public class SummarySystemConfig {
	/**
	 * 外部统一服务 FTP地址
	 */
	private DatabaseConnectionInfo connectionInfo;

	
	public DatabaseConnectionInfo getConnectionInfo() {
		return connectionInfo;
	}

	
	public void setConnectionInfo(DatabaseConnectionInfo connectionInfo) {
		this.connectionInfo = connectionInfo;
	}
}
