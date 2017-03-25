package cn.uway.summary.lte.format;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.task.Task;
import cn.uway.summary.extradatacache.LabelRuleCache;
import cn.uway.summary.extradatacache.SignalCache;
import cn.uway.summary.lte.cache.LteHdSummaryCache;
import cn.uway.summary.lte.context.LabelRule;
import cn.uway.summary.lte.context.WeekPercentage;

/**
 * sql格式化器
 * 
 * @author tylerlee @ 2016年3月21日
 */
public class SqlFormatter {

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlFormatter.class);

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	// 1=左右开区间，2=左右闭区间，3=左开右闭，4=左闭右开
	private static Map<Integer, String> operator = new HashMap<Integer, String>();
	static {
		operator.put(1, "< and >");
		operator.put(2, "<= and >=");
		operator.put(3, "< and >=");
		operator.put(4, "<= and >");
	}

	public static String formatLabelRuleSql(Task task, String sql, int typeId) {
		Date time = getTaskQueyTime(task);
		Map<Integer, List<LabelRule>> labelRuleMap = LteHdSummaryCache.getInstance().getLabelRuleMap();
		List<LabelRule> lrList = labelRuleMap.get(typeId);
		if (lrList == null) {
			sql = formatSingleLabelRuleSql(task, time, sql, typeId);
		} else {
			LabelRule lr = lrList.get(0);
			sql = formatSingleLabelRuleSql(task, time, lr, sql, typeId);
		}
		LOGGER.debug("格式化中间结果语句结束");
		return sql;
	}

	/**
	 * 根据数据库中每种标签类型ID的具体标签ID格式化输出sql
	 * 
	 * @param sql
	 * @return 标签规则sql数组
	 */
	public static List<String> formatLabelRuleSqls(Task task, String sql, int typeId) {
		Date time = getTaskQueyTime(task);
		Map<Integer, List<LabelRule>> labelRuleMap = LteHdSummaryCache.getInstance().getLabelRuleMap();
		List<LabelRule> lrList = labelRuleMap.get(typeId);
		List<String> sqlArray = new ArrayList<String>();
		for (int i = 0; i < lrList.size(); i++) {
			LabelRule lr = lrList.get(i);
			String formatedSql = formatSingleLabelRuleSql(task, time, lr, sql, typeId);
			sqlArray.add(formatedSql);
		}
		LOGGER.debug("格式化中间结果语句结束");

		return sqlArray;
	}

	/**
	 * 获取真实要查询的时间点(如：任务时间是20160414,如果是天周期汇总，那么真实汇总时间就是20160413)
	 * 
	 * @param task
	 * @return
	 */
	private static Date getTaskQueyTime(Task task) {
		// 时间操作请参考下面1,2,3,4
		Calendar c = Calendar.getInstance();
		// 1初始化时间
		if (task.getPeriod() <= 0) {
			/* 如果是非周期任务当前日期减一天， */
			c.setTime(task.getDataTime());
			c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) - 1);
		} else {
			// 采集周期默认是分钟
			c.setTimeInMillis(task.getDataTime().getTime() - task.getPeriod() * 60 * 1000);
		}
		// 2将小时，分，钞，豪钞设为0;表示查询哪天的数据{dataTime}
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		return c.getTime();
	}

	/**
	 * 处理一个单独的LabelRuleSql语句
	 * 
	 * @param time
	 * @param lr
	 * @param c
	 * @param sql
	 * @param typeId
	 * @return
	 */
	private static String formatSingleLabelRuleSql(Task task, Date time, LabelRule lr, String sql, int typeId) {
		// 3设置实际查询的开始时间{dataTimeMin}及分区
		String dataTime = sdf.format(time);// {dataTime}
		Calendar startCal = Calendar.getInstance();
		startCal.setTime(time);
		boolean isLabelRuleNull = lr == null;
		startCal.set(Calendar.HOUR_OF_DAY, isLabelRuleNull ? 0 : lr.getStartHour());
		String dataTimeMin = sdf.format(startCal.getTime());// {dataTimeMin}
		// 4设置实际查询的结束时间{dataTimeMax}及分区
		Calendar endCal = Calendar.getInstance();
		endCal.setTime(time);
		endCal.set(Calendar.HOUR_OF_DAY, isLabelRuleNull ? 24 : lr.getEndHour());
		String dataTimeMax = sdf.format(endCal.getTime());// {dataTimeMax}
		if (!isLabelRuleNull) {
			sql = sql.replace("{labelId}", String.valueOf(lr.getLabelId())).replace("{labelName}", lr.getLabelName());
			String signalSource = SignalCache.get(lr.getSignalKey());
			if (signalSource != null && !"".equals(signalSource)) {
				sql = sql.replace("{minHour}", String.valueOf(lr.getMinHour())).replace("{signalSource}", signalSource);
			} else {
				// TODO 此处表示MOD_LHD_LABEL_RULE表中对应的LABEL_TYPE_ID，LABEL_ID在MOD_LHD_LABEL_SIGNAL_SOURCE表中没有配置
			}
		}
		// 处理真实开始、结束时间和数据时间
		sql = sql.replace("{typeId}", String.valueOf(typeId)).replace("{dataTime}", dataTime).replace("{dataTimeMin}", dataTimeMin)
				.replace("{dataTimeMax}", dataTimeMax);
		// 处理各种分区
		String partitionStr = getPartitionCon(task, startCal, endCal);
		String partitionHw = partitionStr.replace("year", "hd.year").replace("month", "hd.month").replace("day", "hd.day").replace("hour", "hd.hour");
		String partitionSetup = partitionStr.replace("year", "s.year").replace("month", "s.month").replace("day", "s.day").replace("hour", "s.hour");
		String partitionRelease = partitionStr.replace("year", "r.year").replace("month", "r.month").replace("day", "r.day")
				.replace("hour", "r.hour");
		String partitionUserDetail = partitionStr.replace("year", "my.year").replace("month", "my.month").replace("day", "my.day")
				.replace("hour", "my.hour");
		String partitionMod = partitionStr.replace("year", "mod.year").replace("month", "mod.month").replace("day", "mod.day")
				.replace("hour", "mod.hour");
		sql = sql.replace("{partition_hw}", partitionHw).replace("{partition_setup}", partitionSetup)
				.replace("{partition_release}", partitionRelease).replace("{partition_userdetail}", partitionUserDetail)
				.replace("{partition_mod}", partitionMod);
		return sql;
	}

	/**
	 * 处理一个单独的LabelRuleSql语句
	 * 
	 * @param time
	 * @param c
	 * @param sql
	 * @param typeId
	 * @return
	 */
	private static String formatSingleLabelRuleSql(Task task, Date time, String sql, int typeId) {
		return formatSingleLabelRuleSql(task, time, null, sql, typeId);
	}

	/**
	 * 获取分区条件
	 * 
	 * @param startCal
	 *            开始日期
	 * @param endCal
	 *            结束日期
	 * @return 分区条件
	 */
	public static String getPartitionCon(Task task, Calendar startCal, Calendar endCal) {
		int yearStrat = startCal.get(Calendar.YEAR);
		int yearEnd = endCal.get(Calendar.YEAR);
		/* 分别构造year,month,day,hour分区 */
		StringBuilder partition = new StringBuilder("");
		if (yearStrat == yearEnd) {
			partition.append("year=").append(yearStrat);
		} else {
			partition.append("(year>=").append(yearStrat).append(" and year").append("<=").append(yearEnd).append(")");
		}
		// 如果跨年超过一个整年，那么就不用计算后面的条件，因为最终条件会有整年的情况，如果再加条件，会导致数据丢失；
		if (yearStrat + 2 <= yearEnd) {
			return partition.toString();
		}
		int monthStrat = startCal.get(Calendar.MONTH) + 1;
		int monthEnd = endCal.get(Calendar.MONTH) + 1;
		partition.append(" and ");
		if (monthStrat == monthEnd) {
			partition.append("month=").append(monthStrat);
		} else {
			partition.append("(month>=").append(monthStrat);
			// 如果开始月大于结束月，那么肯定是跨年了
			if (monthStrat > monthEnd) {
				partition.append(" or ");
			} else {
				partition.append(" and ");
			}
			partition.append("month<=").append(monthEnd).append(")");
		}
		// 如果跨月超过一个整月，那么就不用计算后面的条件，因为最终条件会有整月的情况，如果再加条件，会导致数据丢失；
		if (monthStrat + 2 <= monthEnd) {
			return partition.toString();
		}
		int dayStrat = startCal.get(Calendar.DAY_OF_MONTH);
		int dayEnd = endCal.get(Calendar.DAY_OF_MONTH);
		Calendar c = Calendar.getInstance();
		c.setTime(task.getDataTime());
		int endHour = c.get(Calendar.HOUR_OF_DAY);
		// int endDay = c.get(Calendar.DAY_OF_MONTH);
		// partition.append(" and day=").append(endDay);
		partition.append(" and ");
		if (dayStrat == dayEnd) {
			partition.append("day=").append(dayStrat);
		} else {
			partition.append("(day>=").append(dayStrat);
			// 如果开始日大于结束日，那么肯定是跨月了
			if (dayStrat > dayEnd) {
				partition.append(" or ");
			} else {
				partition.append(" and ");
			}
			partition.append("day<=").append(dayEnd).append(")");
		}

		// 如果跨月超过一个整天，那么就不用计算后面的条件，因为最终条件会有整天的情况，如果再加条件，会导致数据丢失；
		/*
		 * if (dayStrat + 2 <= dayEnd) { return partition.toString(); }
		 */
		// int hourEnd = endCal.get(Calendar.HOUR_OF_DAY);
		// 简单判断，只适合一个小时
		if (task.getPeriod() == 60) {
			partition.append(" and hour=").append(endHour);
		}
		return partition.toString();
	}

	/**
	 * 根据数据库中每种标签类型ID的具体标签ID格式化输出sql
	 * 
	 * @param sql
	 * @return 标签规则sql数组
	 */
	public static List<String> fill(Task task, String sql, int typeId) {
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat ym_sdf1 = new SimpleDateFormat("yyyyMM");
		
		// 任务时间
		Calendar c = Calendar.getInstance();
		c.setTime(task.getDataTime());
		
		// 数据时间
		String dataTime = sdf1.format(c.getTime());
		// 分区月
		String ym = ym_sdf1.format(c.getTime());
		
		// 常规分区
		StringBuffer sb = new StringBuffer();
		// 分表分区
		StringBuffer sbzte = new StringBuffer();
		StringBuffer sbDay = new StringBuffer();
		
		// 简单判断，只适合一个小时
		if(task.getPeriod() == 60){
			// 数据分区
			int endYear = c.get(Calendar.YEAR);
			int endMonth = c.get(Calendar.MONTH)+1;
			int endDay = c.get(Calendar.DAY_OF_MONTH);
			int endHour = c.get(Calendar.HOUR_OF_DAY);
			
			sb.append(" and year =").append(endYear)
			.append(" and month=").append(endMonth)
			.append(" and day=").append(endDay);
			sbDay.append(sb);
			sb.append(" and hour=").append(endHour);
			
			sbzte.append(" and s.year =").append(endYear).append(" and r.year =").append(endYear)
			.append(" and s.month=").append(endMonth).append(" and r.month=").append(endMonth)
			.append(" and s.day =").append(endDay).append(" and r.day =").append(endDay)
			.append(" and s.hour =").append(endHour).append(" and r.hour =").append(endHour);
			
			sql = sql.replace("{year}", String.valueOf(endYear))
			.replace("{month}", String.valueOf(endMonth))
			.replace("{day}", String.valueOf(endDay))
			.replace("{hour}", String.valueOf(endHour));
		}else if(task.getPeriod() == 1440){
			c.add(Calendar.DAY_OF_MONTH, -1);
			dataTime = sdf1.format(c.getTime());
			ym = ym_sdf1.format(c.getTime());
			
			// 数据分区
			int endYear = c.get(Calendar.YEAR);
			int endMonth = c.get(Calendar.MONTH)+1;
			int endDay = c.get(Calendar.DAY_OF_MONTH);
			
			sb.append(" and year =").append(endYear)
			.append(" and month=").append(endMonth)
			.append(" and day=").append(endDay);
			sbDay.append(sb);
			
			sbzte.append(" and s.year =").append(endYear).append(" and r.year =").append(endYear)
			.append(" and s.month=").append(endMonth).append(" and r.month=").append(endMonth)
			.append(" and s.day =").append(endDay).append(" and r.day =").append(endDay);
			
			sql = sql.replace("{year}", String.valueOf(endYear))
			.replace("{month}", String.valueOf(endMonth))
			.replace("{day}", String.valueOf(endDay));
		}
		
		// 周分区：有些数据是按周进行统计，首先会计算天是否符合规则，再统计周；如：漫游用户出行方式
		String partWeekStr = getLastWeekPart(task.getDataTime());
		List<String> sqlArray = new ArrayList<String>();
		String startTime = "";
		String endTime = "";
		List<LabelRule> lrList = LabelRuleCache.get(typeId);
		if(null == lrList){
			// 不要格式化此段代码，格式化后不便于维护
			sqlArray.add(sql.replace("{yearmonth}", ym)
					.replace("{typeId}", String.valueOf(typeId))
					.replace("{dataTime}", dataTime)
					.replace("{partStr}", sb.toString())
					.replace("{partDayStr}", sbDay.toString())
					.replace("{partWeekStr}", partWeekStr)
					.replace("{partStrZTE}", sbzte.toString())
					);
		}else{
			Date d = c.getTime();
			int currentHour = c.get(Calendar.HOUR_OF_DAY);
			for (LabelRule lr : lrList) {
				// 开始阀门
				c.setTime(d);
				c.set(Calendar.HOUR_OF_DAY, 0);
				c.set(Calendar.MINUTE, 0);
				c.set(Calendar.SECOND, 0);
				c.add(Calendar.HOUR_OF_DAY, lr.getStartHour());
				// rDay>0跨天
				int rDay = lr.getEndHour()/24;
				// 
				int rHour = lr.getEndHour()%24;
				// 跨天且采集时间小于配置的开始时间时，采集数据属于前一天，天数减rDay
				if(rDay > 0 && currentHour < lr.getStartHour())
				{
					c.add(Calendar.DAY_OF_MONTH, -rDay);
				}
				// 跳过监控范围外的小时
				if(task.getPeriod() == 60&&task.getDataTime().before(c.getTime())){
					continue;
				}
				startTime = sdf1.format(c.getTime());
				
				// 结束阀门
				c.setTime(d);
				c.set(Calendar.HOUR_OF_DAY, 0);
				c.set(Calendar.MINUTE, 0);
				c.set(Calendar.SECOND, 0);
				c.add(Calendar.HOUR_OF_DAY, rHour);
				// 跨天且采集时间>=配置的开始时间时，采集数据属于当前，天数加上rDay
				if(rDay > 0 && currentHour >= lr.getStartHour())
				{
					c.add(Calendar.DAY_OF_MONTH, rDay);
				}
				
				// 跳过监控范围外的小时
				if(task.getPeriod() == 60&&task.getDataTime().after(c.getTime())){
					continue;
				}
				// 如果监控9~21点，实际上是到21点59分59秒，所以加一个小时
				c.add(Calendar.HOUR_OF_DAY, 1);
				endTime = sdf1.format(c.getTime());
				
				// 不要格式化此段代码，格式化后不便于维护
				sqlArray.add(sql.replace("{yearmonth}", ym)
						.replace("{typeId}", String.valueOf(typeId))
						.replace("{labelIds}", lr.getLabelIds())
						.replace("{minHour}", String.valueOf(lr.getMinHour()))
						.replace("{dataTime}", dataTime)
						.replace("{startTime}", startTime)
						.replace("{endTime}", endTime)
						.replace("{partStr}", sb.toString())
						.replace("{partDayStr}", sbDay.toString())
						.replace("{partWeekStr}", partWeekStr)
						.replace("{partStrZTE}", sbzte.toString())
						);
			}
		}
		
		LOGGER.debug("格式化中间结果语句结束");
		return sqlArray;
	}


	/**
	 * 获取指定日期前一周7天分区
	 * 
	 * @return
	 */
	public static String getLastWeekPart(Date now) {
		StringBuilder sb = new StringBuilder();
		// start calendar
		Calendar startCal = Calendar.getInstance();
		startCal.setTime(now);
		// 设置到上周
		int week = startCal.get(Calendar.WEEK_OF_YEAR);
		startCal.set(Calendar.WEEK_OF_YEAR, (week - 1));
		startCal.set(Calendar.DAY_OF_YEAR, startCal.get(Calendar.DAY_OF_YEAR) - startCal.get(Calendar.DAY_OF_WEEK) + 2);
		int startYear = startCal.get(Calendar.YEAR);
		int startMonth = startCal.get(Calendar.MONTH) + 1;
		int startDay = startCal.get(Calendar.DAY_OF_MONTH);
		// end calendar
		Calendar endCal = Calendar.getInstance();
		endCal.setTime(startCal.getTime());
		endCal.set(Calendar.DAY_OF_YEAR, endCal.get(Calendar.DAY_OF_YEAR) + 6);
		System.out.println(sdf.format(startCal.getTime()));
		System.out.println(sdf.format(endCal.getTime()));
		int endYear = endCal.get(Calendar.YEAR);
		int endMonth = endCal.get(Calendar.MONTH) + 1;
		int endDay = endCal.get(Calendar.DAY_OF_MONTH);
		if (startYear == endYear) {
			sb.append("year=").append(startYear);
			if (startMonth == endMonth) {
				sb.append(" and month=").append(startMonth);
				sb.append(" and day>=").append(startDay).append(" and day<=").append(endDay);
			} else {
				sb.append(" and (month=").append(startMonth);
				// start month end day
				int smendDay = startCal.getActualMaximum(Calendar.DATE);
				if (smendDay == startDay) {
					sb.append(" and day=").append(startDay);
				} else {
					sb.append(" and day>=").append(startDay).append(" and day<=").append(smendDay);
				}
				sb.append(") or (month=").append(endMonth);
				if (1 == endDay) {
					sb.append(" and day=").append(endDay);

				} else {
					sb.append(" and day>=").append(1).append(" and day<=").append(endDay);
				}
				sb.append(")");
			}
		}else{
			sb.append("(year=").append(startYear);
			sb.append(" and month=").append(startMonth);
			// start month end day
			int smendDay = startCal.getActualMaximum(Calendar.DATE);
			if (smendDay == startDay) {
				sb.append(" and day=").append(startDay);
			} else {
				sb.append(" and day>=").append(startDay).append(" and day<=").append(smendDay);
			}
			sb.append(") or (year=").append(endYear);
			sb.append(" and month=").append(endMonth);
			if (1 == endDay) {
				sb.append(" and day=").append(endDay);
			} else {
				sb.append(" and day>=").append(1).append(" and day<=").append(endDay);
			}
			sb.append(")");
		}
		// 2016-12-26 09:45:04
		// 2017-01-01 09:45:04
		return sb.toString();
	}

	/**
	 * 其它方式,如free way
	 * 
	 * @param sql
	 * @return 标签规则sql数组
	 */
	public static List<String> fillOthers(Task task, String sql, int typeId) {
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat ym_sdf1 = new SimpleDateFormat("yyyyMM");

		// 任务时间
		Calendar c = Calendar.getInstance();
		c.setTime(task.getDataTime());
		// 数据时间
		String dataTime = sdf1.format(c.getTime());
		// 分区月
		String ym = ym_sdf1.format(c.getTime());
		// 数据分区
		int endYear = c.get(Calendar.YEAR);
		int endMonth = c.get(Calendar.MONTH) + 1;
		// 常规分区
		StringBuffer sb = new StringBuffer();
		sb.append(" and year =").append(endYear).append(" and month=").append(endMonth);

		// 分表分区
		StringBuffer sbzte = new StringBuffer();
		sbzte.append(" and s.year =").append(endYear).append(" and r.year =").append(endYear).append(" and s.month=").append(endMonth)
				.append(" and r.month=").append(endMonth);

		// 天分区：有些数据是按进行统计，首先会计算天是否符合规则，如：漫游用户出行方式
		StringBuffer sbDay = new StringBuffer();
		StringBuffer sbzteDay = new StringBuffer();

		// 简单判断，只适合一个小时
		if (task.getPeriod() == 60) {
			int endDay = c.get(Calendar.DAY_OF_MONTH);
			int endHour = c.get(Calendar.HOUR_OF_DAY);

			sb.append(" and day=").append(endDay);

			sbzte.append(" and s.day =").append(endDay).append(" and r.day =").append(endDay);

			sbDay.append(sb);
			sbzteDay.append(sbzte);

			sb.append(" and hour=").append(endHour);

			sbzte.append(" and s.hour =").append(endHour).append(" and r.hour =").append(endHour);
			
			sql = sql.replace("{year}", String.valueOf(endYear))
			.replace("{month}", String.valueOf(endMonth))
			.replace("{day}", String.valueOf(endDay))
			.replace("{hour}", String.valueOf(endHour));
		} else if (task.getPeriod() == 1440) {
			c.add(Calendar.DAY_OF_MONTH, -1);
			dataTime = sdf1.format(c.getTime());
			int endDay = c.get(Calendar.DAY_OF_MONTH);

			sb.append(" and day=").append(endDay);

			sbzte.append(" and s.day =").append(endDay).append(" and r.day =").append(endDay);

			sbDay.append(sb);
			sbzteDay.append(sbzte);
			
			sql = sql.replace("{year}", String.valueOf(endYear))
			.replace("{month}", String.valueOf(endMonth))
			.replace("{day}", String.valueOf(endDay));
		}

		List<String> sqlArray = new ArrayList<String>();
		String startTime = "";
		String endTime = "";
		List<LabelRule> lrList = LabelRuleCache.get(typeId);
		if (null == lrList) {
			sqlArray.add(sql.replace("{yearmonth}", ym).replace("{typeId}", String.valueOf(typeId)).replace("{dataTime}", dataTime)
					.replace("{partStr}", sb.toString()).replace("{partDayStr}", sbDay.toString()).replace("{partStrZTE}", sbzte.toString())
					.replace("{partDayStrZTE}", sbzteDay.toString()));
		} else {
			Date d = c.getTime();
			List<Integer> ssgLabelIdList = LteHdSummaryCache.getInstance().getSignalSourceGroup().get(typeId);
			LabelRule lr = null;
			List<LabelRule> labelRuleList = LteHdSummaryCache.getInstance().getLabelRuleMap().get(typeId);
			for (LabelRule tlr : labelRuleList) {
				// 规则表中和规则在信源表不存在，那么就视为others
				if (!ssgLabelIdList.contains(tlr.getLabelId())) {
					lr = tlr;
					break;
				}
			}
			// 开始阀门
			c.setTime(d);
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.add(Calendar.HOUR_OF_DAY, lr.getStartHour());
			startTime = sdf1.format(c.getTime());

			// 结束阀门
			c.setTime(d);
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.add(Calendar.HOUR_OF_DAY, lr.getEndHour());
			endTime = sdf1.format(c.getTime());

			sqlArray.add(sql.replace("{yearmonth}", ym).replace("{typeId}", String.valueOf(typeId))
					.replace("{labelName}", String.valueOf(lr.getLabelName())).replace("{labelId}", String.valueOf(lr.getLabelId()))
					.replace("{minHour}", String.valueOf(lr.getMinHour())).replace("{dataTime}", dataTime).replace("{startTime}", startTime)
					.replace("{endTime}", endTime).replace("{partStr}", sb.toString()).replace("{partDayStr}", sbDay.toString())
					.replace("{partStrZTE}", sbzte.toString()).replace("{partDayStrZTE}", sbzteDay.toString()));
		}

		LOGGER.debug("格式化中间结果语句结束");
		return sqlArray;
	}

	/**
	 * 根据数据库中每种标签类型ID的具体标签ID格式化输出sql
	 * 
	 * @param sql
	 * @return 周权重汇总sql数组
	 */
	public static List<String> formatWeekWeightSqls(Task task, String sql, int typeId) {
		Map<Integer, List<WeekPercentage>> weekWeightMap = LteHdSummaryCache.getInstance().getWeekWeightMap();
		List<WeekPercentage> weekWeightList = weekWeightMap.get(typeId);
		StringBuilder sb = new StringBuilder();
		List<String> sqlArray = new ArrayList<String>();
		sql = covertSql(task,sql);
		if (weekWeightList == null) {
			sqlArray.add(sql.replace("{typeId}", String.valueOf(typeId)).replace("{dataTime}", task.getDateString(task.getDataTime())));
		} else {
			// 将时间定位到周一
			Calendar cal = Calendar.getInstance();
			cal.setTime(task.getDataTime());
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			// 星期天
			if(cal.get(Calendar.DAY_OF_WEEK) == 1)
			{
				cal.add(Calendar.DAY_OF_MONTH,-6);
			}
			// 星期一到六
			else
			{
				cal.add(Calendar.DAY_OF_MONTH,-(cal.get(Calendar.DAY_OF_WEEK)-2));
			}
			
			for (WeekPercentage wp : weekWeightList) {
				cal.add(Calendar.DAY_OF_MONTH, -7);
				sb.append(" when '").append(task.getDateString(cal.getTime()));
				sb.append("' then ").append(wp.getWeight());
			}
			Map<Integer, List<LabelRule>> labelRuleMap = LteHdSummaryCache.getInstance().getLabelRuleMap();
			// List<Integer> ssgLabelIdList = LteHdSummaryCache.getInstance().getSignalSourceGroup().get(typeId);
			List<LabelRule> lrList = labelRuleMap.get(typeId);
			for (int i = 0; i < lrList.size(); i++) {
				LabelRule lr = lrList.get(i);
				// 排除freeway
				if(typeId == 3 && lr.getLabelId() == 4)
				{
					continue;
				}
				sqlArray.add(sql.replace("{typeId}", String.valueOf(typeId)).replace("{labelId}", String.valueOf(lr.getLabelId()))
						.replace("{minDays}", String.valueOf(lr.getMinDays())).replace("{maxDays}", String.valueOf(lr.getMaxDays()))
						.replace("{percentageCondition}", sb.toString()).replace("{dataTime}", task.getDateString(task.getDataTime())));
			}
		}
		LOGGER.debug("格式化最终结果语句结束");
		return sqlArray;
	}

	public static String formatWeekWeightSql(Task task, String sql, int typeId) {
		Map<Integer, List<WeekPercentage>> weekWeightMap = LteHdSummaryCache.getInstance().getWeekWeightMap();
		List<WeekPercentage> weekWeightList = weekWeightMap.get(typeId);
		StringBuilder sb = new StringBuilder();
		sql = covertSql(task,sql);
		if (weekWeightList == null) {
			sql = sql.replace("{typeId}", String.valueOf(typeId)).replace("{dataTime}", task.getDateString(task.getDataTime()));
		} else {
			// 将时间定位到周一
			Calendar cal = Calendar.getInstance();
			cal.setTime(task.getDataTime());
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			// 星期天
			if(cal.get(Calendar.DAY_OF_WEEK) == 1)
			{
				cal.add(Calendar.DAY_OF_MONTH,-6);
			}
			// 星期一到六
			else
			{
				cal.add(Calendar.DAY_OF_MONTH,-(cal.get(Calendar.DAY_OF_WEEK)-2));
			}			
			for (WeekPercentage wp : weekWeightList) {
				cal.add(Calendar.DAY_OF_MONTH, -7);
				sb.append(" when '").append(task.getDateString(cal.getTime()));
				sb.append("' then ").append(wp.getWeight());
			}
			Map<Integer, List<LabelRule>> labelRuleMap = LteHdSummaryCache.getInstance().getLabelRuleMap();
			LabelRule lr = labelRuleMap.get(typeId).get(0);
			sql = sql.replace("{typeId}", String.valueOf(typeId)).replace("{labelId}", String.valueOf(lr.getLabelId()))
					.replace("{minDays}", String.valueOf(lr.getMinDays())).replace("{maxDays}", String.valueOf(lr.getMaxDays()))
					.replace("{percentageCondition}", sb.toString()).replace("{dataTime}", task.getDateString(task.getDataTime()));
		}
		LOGGER.debug("格式化最终结果语句结束");
		return sql;
	}
	
	private static String covertSql(Task task,String sql)
	{
		// 任务时间
		Calendar c = Calendar.getInstance();
		c.setTime(task.getDataTime());
		
		// 常规分区
		StringBuffer sb = new StringBuffer();
		// 分表分区
		StringBuffer sbzte = new StringBuffer();
		StringBuffer sbDay = new StringBuffer();
		
		// 简单判断，只适合一个小时
		if(task.getPeriod() == 60){
			// 数据分区
			int endYear = c.get(Calendar.YEAR);
			int endMonth = c.get(Calendar.MONTH)+1;
			int endDay = c.get(Calendar.DAY_OF_MONTH);
			int endHour = c.get(Calendar.HOUR_OF_DAY);
			
			sb.append(" and year =").append(endYear)
			.append(" and month=").append(endMonth)
			.append(" and day=").append(endDay);
			sbDay.append(sb);
			sb.append(" and hour=").append(endHour);
			
			sbzte.append(" and s.year =").append(endYear).append(" and r.year =").append(endYear)
			.append(" and s.month=").append(endMonth).append(" and r.month=").append(endMonth)
			.append(" and s.day =").append(endDay).append(" and r.day =").append(endDay)
			.append(" and s.hour =").append(endHour).append(" and r.hour =").append(endHour);
			
			sql = sql.replace("{year}", String.valueOf(endYear))
			.replace("{month}", String.valueOf(endMonth))
			.replace("{day}", String.valueOf(endDay))
			.replace("{hour}", String.valueOf(endHour));
		}else if(task.getPeriod() == 1440){
			c.add(Calendar.DAY_OF_MONTH, -1);
			
			// 数据分区
			int endYear = c.get(Calendar.YEAR);
			int endMonth = c.get(Calendar.MONTH)+1;
			int endDay = c.get(Calendar.DAY_OF_MONTH);
			
			sb.append(" and year =").append(endYear)
			.append(" and month=").append(endMonth)
			.append(" and day=").append(endDay);
			sbDay.append(sb);
			
			sbzte.append(" and s.year =").append(endYear).append(" and r.year =").append(endYear)
			.append(" and s.month=").append(endMonth).append(" and r.month=").append(endMonth)
			.append(" and s.day =").append(endDay).append(" and r.day =").append(endDay);
			sql = sql.replace("{year}", String.valueOf(endYear))
			.replace("{month}", String.valueOf(endMonth))
			.replace("{day}", String.valueOf(endDay));
		}
		return sql.replace("{partStr}", sb.toString())
		.replace("{partDayStr}", sbDay.toString())
		.replace("{partStrZTE}", sbzte.toString());
	}

	public static void main(String[] args) {
		// a(null, null, 0);
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		c.setFirstDayOfWeek(Calendar.MONDAY);
		//System.out.println(c.getFirstDayOfWeek());
		c.set(Calendar.YEAR, 2016);
		c.set(Calendar.MONTH, 6);
		c.set(Calendar.DAY_OF_MONTH, 4);
		c.set(Calendar.HOUR_OF_DAY, 9);
		System.out.println(sdf.format(c.getTime()));
		System.out.println(getLastWeekPart(c.getTime()));
		/*int week = c.get(Calendar.WEEK_OF_YEAR);
		System.out.println(week);
		c.set(Calendar.WEEK_OF_YEAR, (week - 1));
		System.out.println((c.getFirstDayOfWeek() == Calendar.SUNDAY));
		System.out.println(sdf.format(c.getTime()));
		System.out.println(c.get(Calendar.DAY_OF_WEEK));
		c.set(Calendar.DAY_OF_YEAR, c.get(Calendar.DAY_OF_YEAR) - c.get(Calendar.DAY_OF_WEEK) + 2);
		System.out.println(sdf.format(c.getTime()));
		c.set(Calendar.DAY_OF_YEAR, c.get(Calendar.DAY_OF_YEAR) + 6);
		System.out.println(sdf.format(c.getTime()));
		System.out.println("-----------");

		System.out.println(c.getWeekYear());
		c.set(Calendar.MONTH, 11);
		c.set(Calendar.DAY_OF_MONTH, 30);
		c.set(Calendar.HOUR_OF_DAY, 9);
		// c.set(Calendar.HOUR_OF_DAY, 0);
		Calendar c1 = Calendar.getInstance();
		c1.setTime(new Date());
		c1.set(Calendar.MONTH, 11);
		c1.set(Calendar.DAY_OF_MONTH, 31);
		c1.set(Calendar.HOUR_OF_DAY, 6);
		System.out.println(sdf.format(c.getTime()));
		System.out.println(sdf.format(c1.getTime()));*/
		// System.out.println(getPartitionCon(c, c1));
		// System.out.println(c.get(Calendar.DAY_OF_MONTH));
		// c.set(Calendar.HOUR_OF_DAY, 0);
		// c.set(Calendar.MINUTE, 0);
		// c.set(Calendar.SECOND, 0);
		// System.out.println(sdf.format(c.getTime()));
		// Date time = c.getTime();
		// c.set(Calendar.HOUR_OF_DAY, -25);
		// System.out.println(sdf.format(c.getTime()));
		// c.setTime(time);
		// c.set(Calendar.HOUR_OF_DAY, 25);
		// System.out.println(sdf.format(c.getTime()));
	}

}
