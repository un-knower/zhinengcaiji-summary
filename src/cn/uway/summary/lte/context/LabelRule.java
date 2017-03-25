package cn.uway.summary.lte.context;

/**
 * 标签规则设置
 * 
 * @author tylerlee @ 2016年3月19日
 */
public class LabelRule {

	/* 类型信息 */
	private int typeId;// LABEL_TYPE 数值 标签类型 职业、出行方式、兴趣爱好

	private int labelId;// LABEL_ID 数值 标签ID 1//MOD_LHD_User_f_Statistics.Statistics_ID

	private String labelName;// LABEL_NAME 文本 标签名称 银行//MOD_LHD_User_f_Statistics.Statistics_NAME

	/* 符合周信息 */
	private int minDays;// MIN_DAYS 数值 周粒度有效天数下限 3

	private int maxDays;// MAX_DAYS 数值 周粒度有效天数上限 5

	private int dayOprator;// HOUR_OPRATOR 数值 周粒度有效天数运算符 1=左右开区间，2=左右闭区间，3=左开右闭，4=左闭右开

	/* 符合天信息 */
	private int minHour;// MIN_HOUR 数值 天粒度有效话单下限 2

	private int startHour;// START_HOUR 数值 统计时间段 开始小时 9

	private int endHour;// END_HOUR 数值 统计时间段 结束小时 17

	private int hourOprator;// HOUR_OPRATOR 数值 统计时间段运算符 1=左右开区间，2=左右闭区间，3=左开右闭，4=左闭右开

	/* 信源信息 */
	private String signalSource;// SIGNAL_SOURCE 列表 信源 　
	
	// 此字段能直接关联出包含的网元id列表
	// 再加上具体的网元id后，可以关联出所属场景
	private String signalKey;
	public String getSignalKey(){
		if(null == signalKey){
			signalKey=typeId+"-"+labelId;
		}
		return signalKey;
	}
	
	// 分区后的lableid集合
	private String labelIds;
	public String getLabelIds() {
		return labelIds;
	}
	public void setLabelIds(String labelIds) {
		this.labelIds = labelIds;
	}

	public int getTypeId() {
		return typeId;
	}

	public void setTypeId(int typeId) {
		this.typeId = typeId;
	}

	public int getLabelId() {
		return labelId;
	}

	public void setLabelId(int labelId) {
		this.labelId = labelId;
	}

	public String getLabelName() {
		return labelName;
	}

	public void setLabelName(String labelName) {
		this.labelName = labelName;
	}

	public int getMinDays() {
		return minDays;
	}

	public void setMinDays(int minDays) {
		this.minDays = minDays;
	}

	public int getMaxDays() {
		return maxDays;
	}

	public void setMaxDays(int maxDays) {
		this.maxDays = maxDays;
	}

	public int getDayOprator() {
		return dayOprator;
	}

	public void setDayOprator(int dayOprator) {
		this.dayOprator = dayOprator;
	}

	public int getMinHour() {
		return minHour;
	}

	public void setMinHour(int minHour) {
		this.minHour = minHour;
	}

	public int getStartHour() {
		return startHour;
	}

	public void setStartHour(int startHour) {
		this.startHour = startHour;
	}

	public int getEndHour() {
		return endHour;
	}

	public void setEndHour(int endHour) {
		this.endHour = endHour;
	}

	public int getHourOprator() {
		return hourOprator;
	}

	public void setHourOprator(int hourOprator) {
		this.hourOprator = hourOprator;
	}
}
