package cn.uway.summary.cdma.neway;

import java.util.ArrayList;
import java.util.List;

/**
 * 网元表
 * 
 * @author tianjing @ 2014年12月25日
 */
public class CellCfgInfo {

	public Long neSysId;

	public Short carrId;

	public Short pn;

	public Short cellId;

	public String cellName;

	public Long neCellId;

	public Short cityId;

	public String cityName;

	public Integer countyId;

	public String countyName;

	public String vendor;

	public Short btsId;

	public Long neBtsId;

	public Short bscId;

	public Long neBscId;

	public Short adjtype;

	// public Short adjstate;

	public Short sid;

	// public Short nbrseq;

	/**
	 * 邻区信息集合
	 */
	public List<AdjacentCellCfg> adjacentCells = new ArrayList<CellCfgInfo.AdjacentCellCfg>();

	public class AdjacentCellCfg {

		public Long neSysId;

		public Short carrId;

		public Short pn;

		public Short adjstate;

		public Short nbrseq;

		public Short cellId;

		public String cellName;

		public Long neCellId;

		public Short btsId;

		public Long neBtsId;

		public Short bscId;

		public Long neBscId;

		public Integer countyId;

		public String countyName;

		public Short cityId;

		public String cityName;

		public Short sid;
	}
}
