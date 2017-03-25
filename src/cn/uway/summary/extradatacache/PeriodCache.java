package cn.uway.summary.extradatacache;

import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 周期缓存加载
 * <p>
 * 1、定时加载缓存，子类可指定“线程名”、“加载周期”<br>
 * 2、默认加载周期是一天
 * 
 * @author sunt
 *
 */
public abstract class PeriodCache implements Runnable {
	protected static Logger LOG = LoggerFactory.getLogger(PeriodCache.class);

	protected static final Integer MINUTE = 60 * 1000;
	protected static final Integer HOUR = 60 * MINUTE;
	protected static final Integer DAY = 24 * HOUR;
	// 缓存是否加载完
	protected Boolean isReady = false;
	
	// 线程名
	private String name;
	// 加载周期
	private long period = DAY;

	public PeriodCache(String name) {
		this.name = name;
	}

	public PeriodCache(String name, long period) {
		this.name = name;
		this.period = period;
	}

	@Override
	public void run() {
		LOG.info("{}加载线程开始运行,加载周期：{}ms", name, period);
		Timer timer = new Timer(name + "定时加载器");
		timer.schedule(new TimerMonitor(), 0, period);
	}

	private class TimerMonitor extends TimerTask {
		@Override
		public void run() {
			long start = System.currentTimeMillis();
			load();
			long end = System.currentTimeMillis();
			LOG.info("{}加载完毕,加载耗时：{}s", name, (end-start)/1000);
			isReady = true;
		}
	}

	protected abstract void load();
	
	/**
	 * 使用前需要先判断是否缓存完毕
	 * @return
	 */
	protected Boolean isReady(){
		return isReady;
	}
}
