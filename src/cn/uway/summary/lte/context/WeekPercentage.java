package cn.uway.summary.lte.context;

/**
 * 标签数据周权重
 * 
 * @author tylerlee @ 2016年3月19日
 */
public class WeekPercentage {

	private int typeId;// 统计类型id;MOD_LHD_User_f_Statistics.Statistics_TYPE

	private String typeName;// 统计类型名称:职业属性、兴趣爱好、出行方式…之一

	private int weekSeq;// 最近n周

	private int weight;// 权重

	public int getTypeId() {
		return typeId;
	}

	public void setTypeId(int typeId) {
		this.typeId = typeId;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public int getWeekSeq() {
		return weekSeq;
	}

	public void setWeekSeq(int weekSeq) {
		this.weekSeq = weekSeq;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}
}
