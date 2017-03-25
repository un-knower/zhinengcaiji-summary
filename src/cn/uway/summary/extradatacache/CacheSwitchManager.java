package cn.uway.summary.extradatacache;

import java.util.HashMap;
import java.util.Map;

import cn.uway.framework.external.SwitchManager;

public class CacheSwitchManager implements SwitchManager{
	private String[] cacheNames;
	private Map<String,PeriodCache> cacheRun = new HashMap<String, PeriodCache>();

	public void setCacheNames(String cacheNames) {
		if (cacheNames != null) {
			this.cacheNames = cacheNames.split("\\|");
		}
	}

	/**
	 * 根据system.periodCache配置的缓存名，逐个开启
	 */
	public Boolean open() {
		if (null == cacheNames) {
			return false;
		}
		for (String name : cacheNames) {
			Thread t = null;
			PeriodCache r = null;
			switch (name) {
			case "signal":
				r = new SignalCache();
				break;
			case "scene":
				r = new SceneCache();
				break;
			case "labelRule":
				r = new LabelRuleCache();
				break;
			case "imsiCity":
				r = new ImsiProvinceCache();
				break;
			default:
				continue;
			}
			cacheRun.put(name, r);
			t = new Thread(r);
			t.start();
		}
		return true;
	}

	@Override
	public Boolean isReady() {
		if (null == cacheNames) {
			return true;
		}
		for (String name : cacheNames) {
			PeriodCache r = cacheRun.get(name);
			if(null != r){
				if(!r.isReady()){
					return false;
				}
			}
		}
		return true;
	}

	public String getCacheValue(String cscheName,String key){
		PeriodCache pc = cacheRun.get(cscheName);
		if(pc instanceof ImsiProvinceCache){
			Map<String,String> map = ((ImsiProvinceCache) pc).get(key);
			return null != map ?map.get("belong_area_id").toString():null;
		}
		if(pc instanceof SignalCache){
			return ((SignalCache) pc).get(key);
		}
		if(pc instanceof SceneCache){
			return ((SceneCache) pc).get(key);
		}
//		if(pc instanceof LabelRuleCache){
//			return ((LabelRuleCache) pc).get(key);
//		}
		return null;
	}
	
	public Map getCacheMap(String cscheName,String key){
		PeriodCache pc = cacheRun.get(cscheName);
		if(pc instanceof ImsiProvinceCache){
			return ((ImsiProvinceCache) pc).get(key);
		}
		return null;
	}
	
	public Map getCacheCityIdCityNameMap(String cscheName,String key){
		PeriodCache pc = cacheRun.get(cscheName);
		if(pc instanceof ImsiProvinceCache){
			return ((ImsiProvinceCache) pc).geCityIDNameMap(key);
		}
		return null;
	}
}
