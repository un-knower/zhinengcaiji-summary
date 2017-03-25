package cn.uway.summary.extradatacache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.external.ExternalLoader;
import cn.uway.summary.lte.cache.LteHdSummaryCache;

/**
 * 汇总模板外部数据加载器
 * 
 * @author tylerlee @ 2016年3月21日
 */
public class SummaryExternalDatasLoader implements ExternalLoader {

	private static final Logger LOGGER = LoggerFactory.getLogger(SummaryExternalDatasLoader.class);

	@Override
	public boolean loadExternalDatas() {
		LOGGER.debug("【开始初始化汇总外部数据……】");
		LteHdSummaryCache.getInstance().startLoadData();
		return false;
	}

}
