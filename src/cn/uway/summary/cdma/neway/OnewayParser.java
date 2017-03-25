package cn.uway.summary.cdma.neway;

import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.accessor.AccessOutObject;
import cn.uway.framework.accessor.JdbcAccessOutObject;
import cn.uway.framework.context.Vendor;
import cn.uway.framework.orientation.GeographicalOperation;
import cn.uway.framework.parser.DatabaseParser;
import cn.uway.framework.parser.ParseOutRecord;
import cn.uway.framework.parser.database.DatabaseParseTempletParser;
import cn.uway.summary.cdma.neway.CellCfgInfo.AdjacentCellCfg;
import cn.uway.util.DbUtil;

/**
 * one-way、two-way、疑似one-way数据汇总
 * 
 * @author tianjing @ 2014年12月25日
 */
public class OnewayParser extends DatabaseParser {

	/**
	 * 日志
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(OnewayParser.class);

	public static final int ONE_WAY_DATA_TYPY = 60;

	public static final int RESEMBLE_WAY_DATA_TYPY = 61;

	public static final int TWO_WAY_DATA_TYPY = 62;

	public final DecimalFormat df = new DecimalFormat("0.00");

	public String busType = null;

	public String vendor = null;

	public int[] summaryCnt = new int[3];

	/**
	 * 结果数据临时缓存
	 */
	private LinkedList<ParseOutRecord> cacheElement = new LinkedList<ParseOutRecord>();

	Map<Long, Double[]> neSysIdToNeMap = new HashMap<Long, Double[]>();

	Map<Long, CellCfgInfo> neSysIdToCellMap = new HashMap<Long, CellCfgInfo>();

	Iterator<CellCfgInfo> cellCfgIterator = null;

	@Override
	public void parse(AccessOutObject accessOutObject) throws Exception {
		this.startTime = new Date();
		this.task = accessOutObject.getTask();
		this.currentDataTime = this.task.getDataTime();
		this.dateTime = new SimpleDateFormat("yyyy/MM/dd").format(task.getDataTime());
		this.parserTemplate = DatabaseParseTempletParser.parse(this.templates);
		this.busType = this.parserTemplate.getBusType();
		JdbcAccessOutObject outObject = (JdbcAccessOutObject) accessOutObject;
		this.connection = outObject.getConnection();
		// 获得邻区大表信息
		getAdjacentCellCfgInfo();
		if (neSysIdToCellMap.size() > 0) {
			// 获得网元表信息
			getNEInfo();
		}
	}

	public void getNEInfo() {
		this.vendor = this.task.getExtraInfo().getVendor().toUpperCase();
		String sql = null;
		if (Vendor.VENDOR_ALC.equals(vendor))
			sql = "SELECT LONGITUDE, LATITUDE, NE_CELL_ID AS NE_SYS_ID FROM CFG_MAP_DEV_TO_NE WHERE VENDOR = ? AND BUSTYPE = ? AND LONGITUDE IS NOT NULL AND LATITUDE IS NOT NULL";
		else
			sql = "SELECT LONGITUDE, LATITUDE, NE_CARR_ID AS NE_SYS_ID FROM CFG_MAP_DEV_TO_NE WHERE VENDOR = ? AND BUSTYPE = ? AND LONGITUDE IS NOT NULL AND LATITUDE IS NOT NULL";
		LOGGER.debug("网元加载执行SQL：{}, VENDOR ='{}' AND BUSTYPE ='{}'", new Object[]{sql, vendor, this.busType});
		try {
			this.statement = this.connection.prepareStatement(sql);
			this.statement.setString(1, vendor);
			this.statement.setString(2, this.busType);
			// 查询网元经纬度信息
			this.resultSet = this.statement.executeQuery();
			while (this.resultSet.next()) {
				Double[] lngLat = new Double[2];
				lngLat[0] = this.resultSet.getDouble("LONGITUDE");
				lngLat[1] = this.resultSet.getDouble("LATITUDE");
				neSysIdToNeMap.put(this.resultSet.getLong("NE_SYS_ID"), lngLat);
			}
			LOGGER.debug("本次共加载到{}条网元信息", neSysIdToNeMap.size());
		} catch (SQLException e) {
			LOGGER.warn("网元经纬度信息加载失败。", e);
		} finally {
			DbUtil.close(this.resultSet, this.statement, null);
		}
	}

	private void getAdjacentCellCfgInfo() throws SQLException {
		LOGGER.debug("任务执行SQL：{}to_date('{}','yyyy/mm/dd')", new Object[]{parserTemplate.getSql(), this.dateTime});
		this.statement = connection.prepareStatement(parserTemplate.getSql());
		this.statement.setDate(1, new java.sql.Date(this.currentDataTime.getTime()));
		this.resultSet = statement.executeQuery();
		while (resultSet.next()) {
			this.totalNum++;
			Long neSysid = resultSet.getLong("NE_SYS_ID");
			CellCfgInfo cellCfg = this.neSysIdToCellMap.get(neSysid);
			if (cellCfg != null) {
				AdjacentCellCfg neiCfg = cellCfg.new AdjacentCellCfg();
				neiCfg.neSysId = resultSet.getLong("NEI_NE_SYS_ID");
				neiCfg.adjstate = resultSet.getShort("ADJSTATE");
				neiCfg.nbrseq = resultSet.getShort("NBRSEQ");
				neiCfg.carrId = resultSet.getShort("NEI_CARR_ID");
				neiCfg.pn = resultSet.getShort("NEI_NE_PN");
				neiCfg.cellId = resultSet.getShort("NEI_CELL_ID");
				neiCfg.cellName = resultSet.getString("NEI_CELL_NAME");
				neiCfg.neCellId = resultSet.getLong("NEI_NE_CELL_ID");
				neiCfg.btsId = resultSet.getShort("NEI_BTS_ID");
				neiCfg.neBtsId = resultSet.getLong("NEI_NE_BTS_ID");
				neiCfg.bscId = resultSet.getShort("NEI_BSC_ID");
				neiCfg.neBscId = resultSet.getLong("NEI_NE_BSC_ID");
				neiCfg.countyId = resultSet.getInt("NEI_COUNTY_ID");
				neiCfg.countyName = resultSet.getString("NEI_COUNTY_NAME");
				neiCfg.cityId = resultSet.getShort("NEI_CITY_ID");
				neiCfg.cityName = resultSet.getString("NEI_CITY_NAME");
				neiCfg.sid = resultSet.getShort("NEI_SID");
				cellCfg.adjacentCells.add(neiCfg);
				continue;
			}
			cellCfg = new CellCfgInfo();
			cellCfg.neSysId = neSysid;
			cellCfg.pn = resultSet.getShort("PN");
			cellCfg.carrId = resultSet.getShort("CARR_ID");
			cellCfg.cellId = resultSet.getShort("CELL_ID");
			cellCfg.cellName = resultSet.getString("CELL_NAME");
			cellCfg.neCellId = resultSet.getLong("NE_CELL_ID");
			cellCfg.bscId = resultSet.getShort("BSC_ID");
			cellCfg.neBscId = resultSet.getLong("NE_BSC_ID");
			cellCfg.btsId = resultSet.getShort("BTS_ID");
			cellCfg.neBtsId = resultSet.getLong("NE_BTS_ID");
			cellCfg.cityId = resultSet.getShort("CITY_ID");
			cellCfg.cityName = resultSet.getString("CITY_NAME");
			cellCfg.countyId = resultSet.getInt("COUNTY_ID");
			cellCfg.countyName = resultSet.getString("COUNTY_NAME");
			cellCfg.vendor = resultSet.getString("VENDOR");
			cellCfg.sid = resultSet.getShort("SID");
			cellCfg.adjtype = resultSet.getShort("ADJTYPE");
			AdjacentCellCfg neiCfg = cellCfg.new AdjacentCellCfg();
			neiCfg.neSysId = resultSet.getLong("NEI_NE_SYS_ID");
			neiCfg.adjstate = resultSet.getShort("ADJSTATE");
			neiCfg.nbrseq = resultSet.getShort("NBRSEQ");
			neiCfg.carrId = resultSet.getShort("NEI_CARR_ID");
			neiCfg.pn = resultSet.getShort("NEI_NE_PN");
			neiCfg.cellId = resultSet.getShort("NEI_CELL_ID");
			neiCfg.cellName = resultSet.getString("NEI_CELL_NAME");
			neiCfg.neCellId = resultSet.getLong("NEI_NE_CELL_ID");
			neiCfg.btsId = resultSet.getShort("NEI_BTS_ID");
			neiCfg.neBtsId = resultSet.getLong("NEI_NE_BTS_ID");
			neiCfg.bscId = resultSet.getShort("NEI_BSC_ID");
			neiCfg.neBscId = resultSet.getLong("NEI_NE_BSC_ID");
			neiCfg.countyId = resultSet.getInt("NEI_COUNTY_ID");
			neiCfg.countyName = resultSet.getString("NEI_COUNTY_NAME");
			neiCfg.cityId = resultSet.getShort("NEI_CITY_ID");
			neiCfg.cityName = resultSet.getString("NEI_CITY_NAME");
			neiCfg.sid = resultSet.getShort("NEI_SID");
			cellCfg.adjacentCells.add(neiCfg);
			this.neSysIdToCellMap.put(neSysid, cellCfg);
		}
		LOGGER.debug("本次共加载到{}条邻区信息, 待汇总的小区数：{}。", this.totalNum, this.neSysIdToCellMap.size());
		cellCfgIterator = this.neSysIdToCellMap.values().iterator();
	}

	@Override
	public boolean hasNextRecord() throws Exception {
		if (cacheElement.size() > 0)
			return true;
		if (cellCfgIterator == null)
			return false;
		return cellCfgIterator.hasNext();
	}

	@Override
	public ParseOutRecord nextRecord() throws Exception {
		if (cacheElement.size() > 0)
			return cacheElement.removeFirst();
		// 进行汇总
		CellCfgInfo currtCell = cellCfgIterator.next();
		AdjacentCellCfg currtOneAdjacentCell = null;
		AdjacentCellCfg currtTwoAdjacentCell = null;
		for (int i = 0; i < currtCell.adjacentCells.size(); i++) {
			currtOneAdjacentCell = currtCell.adjacentCells.get(i);
			if (currtCell.neSysId.longValue() == currtOneAdjacentCell.neSysId.longValue())
				continue;
			// 疑似ONEWAY
			processResembleOneWay(currtCell, currtOneAdjacentCell);
			for (int j = 0; j < currtCell.adjacentCells.size(); j++) {
				if (j == i)
					continue;
				currtTwoAdjacentCell = currtCell.adjacentCells.get(j);
				// ONEWAY
				if (j > i) {
					processOneWay(currtCell, currtOneAdjacentCell, currtTwoAdjacentCell);
				}
				// TWOWAY
				processTwoWay(currtCell, currtOneAdjacentCell, currtTwoAdjacentCell);
			}
		}
		return null;
	}

	public void processTwoWay(CellCfgInfo currtCell, AdjacentCellCfg currtOneAdjacentCell, AdjacentCellCfg currtTwoAdjacentCell) {
		if (currtTwoAdjacentCell.neSysId.longValue() == currtCell.neSysId.longValue()
				|| currtTwoAdjacentCell.neSysId.longValue() == currtOneAdjacentCell.neSysId.longValue())
			return;
		CellCfgInfo twoCellCfg = this.neSysIdToCellMap.get(currtTwoAdjacentCell.neSysId);
		if (twoCellCfg == null)
			return;
		AdjacentCellCfg adjacentCellInTwoCell = null;
		for (int i = 0; i < twoCellCfg.adjacentCells.size(); i++) {
			adjacentCellInTwoCell = twoCellCfg.adjacentCells.get(i);
			if (adjacentCellInTwoCell.neSysId.longValue() == currtCell.neSysId.longValue()
					|| adjacentCellInTwoCell.neSysId.longValue() == currtOneAdjacentCell.neSysId.longValue()
					|| adjacentCellInTwoCell.neSysId.longValue() == currtTwoAdjacentCell.neSysId.longValue())
				continue;
			if (!Vendor.VENDOR_ALC.equals(vendor)) {
				if (currtCell.carrId.shortValue() != currtOneAdjacentCell.carrId.shortValue()
						|| currtCell.carrId.shortValue() != currtTwoAdjacentCell.carrId.shortValue()
						|| currtCell.carrId.shortValue() != adjacentCellInTwoCell.carrId.shortValue())
					continue;
			}
			if (adjacentCellInTwoCell.pn.shortValue() != currtOneAdjacentCell.pn.shortValue())
				continue;
			// 构建TWOWAY数据
			buildTwoWayData(currtCell, currtOneAdjacentCell, currtTwoAdjacentCell, adjacentCellInTwoCell);
		}
	}

	public void buildTwoWayData(CellCfgInfo currtCell, AdjacentCellCfg currtOneAdjacentCell, AdjacentCellCfg currtTwoAdjacentCell,
			AdjacentCellCfg adjacentCellInTwoCell) {
		ParseOutRecord outRecord = new ParseOutRecord();
		outRecord.setType(TWO_WAY_DATA_TYPY);
		Map<String, String> outTwoWayData = this.createExportPropertyMap(TWO_WAY_DATA_TYPY);
		String vendorName = Vendor.getVendorName(currtCell.vendor);
		outTwoWayData.put("START_TIME", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(this.currentDataTime));
		outTwoWayData.put("NE_SYS_ID", objToString(currtCell.neSysId));
		outTwoWayData.put("CELL_ID", objToString(currtCell.cellId));
		outTwoWayData.put("CELL_NAME", objToString(currtCell.cellName));
		outTwoWayData.put("NE_CELL_ID", objToString(currtCell.neCellId));
		outTwoWayData.put("CITY_ID", objToString(objToString(currtCell.cityId)));
		outTwoWayData.put("COUNTY_ID", objToString(objToString(currtCell.countyId)));
		outTwoWayData.put("COUNTY_NAME", objToString(objToString(currtCell.countyName)));
		outTwoWayData.put("VENDOR", objToString(objToString(currtCell.vendor)));
		outTwoWayData.put("VENDOR_NAME", objToString(objToString(vendorName)));
		outTwoWayData.put("PN", objToString(currtCell.pn));
		outTwoWayData.put("CARRIER", objToString(currtCell.carrId));
		outTwoWayData.put("ADJTYPE", objToString(currtCell.adjtype));
		outTwoWayData.put("CITY_NAME", objToString(currtCell.cityName));
		outTwoWayData.put("SID", objToString(currtCell.sid));
		outTwoWayData.put("DATATYPE", "0");
		outTwoWayData.put("BUSTYPE", this.busType);
		outTwoWayData.put("BTS_ID", objToString(currtCell.btsId));
		outTwoWayData.put("NE_BSC_ID", objToString(currtCell.neBscId));
		outTwoWayData.put("NE_BTS_ID", objToString(currtCell.neBtsId));
		Double[] currCellLngLat = this.neSysIdToNeMap.get(currtCell.neSysId);
		// 第一小区
		outTwoWayData.put("NEI_NE_SYS_ID", objToString(objToString(currtOneAdjacentCell.neSysId)));
		outTwoWayData.put("NEI_CELL_ID", objToString(objToString(currtOneAdjacentCell.cellId)));
		outTwoWayData.put("NEI_CITY_ID", objToString(objToString(currtOneAdjacentCell.cityId)));
		outTwoWayData.put("NEI_COUNTY_ID", objToString(objToString(currtOneAdjacentCell.countyId)));
		outTwoWayData.put("NEI_VENDOR", objToString(objToString(currtCell.vendor)));
		outTwoWayData.put("NEI_VENDOR_NAME", objToString(objToString(vendorName)));
		outTwoWayData.put("NEI_COUNTY_NAME", objToString(objToString(currtOneAdjacentCell.countyName)));
		outTwoWayData.put("NEI_CELL_NAME", objToString(currtOneAdjacentCell.cellName));
		outTwoWayData.put("NEI_PN", objToString(currtOneAdjacentCell.pn));
		outTwoWayData.put("NEI_CARRIER", objToString(currtOneAdjacentCell.carrId));
		outTwoWayData.put("NEI_CITY_NAME", objToString(currtOneAdjacentCell.cityName));
		outTwoWayData.put("NEI_SID", objToString(currtOneAdjacentCell.sid));
		outTwoWayData.put("NEI_NE_CELL_ID", objToString(currtOneAdjacentCell.neCellId));
		outTwoWayData.put("NEI_BTS_ID", objToString(currtOneAdjacentCell.btsId));
		outTwoWayData.put("NEI_NE_BSC_ID", objToString(currtOneAdjacentCell.neBscId));
		outTwoWayData.put("NEI_NE_BTS_ID", objToString(currtOneAdjacentCell.neBtsId));
		outTwoWayData.put("NBRSEQ_PROPOSED_AB", objToString(currtOneAdjacentCell.nbrseq));
		Double[] oneCellLngLat = this.neSysIdToNeMap.get(currtOneAdjacentCell.neSysId);
		if (null != oneCellLngLat && currCellLngLat != null)
			outTwoWayData
					.put("DISTANCE_AB", doubleToString(GeographicalOperation.distanceOperation(currCellLngLat[0], currCellLngLat[1],
							oneCellLngLat[0], oneCellLngLat[1])));
		// 第二小区
		outTwoWayData.put("NEI_NE_SYS_ID2", objToString(objToString(currtTwoAdjacentCell.neSysId)));
		outTwoWayData.put("NEI_CELL_ID2", objToString(objToString(currtTwoAdjacentCell.cellId)));
		outTwoWayData.put("NEI_CITY_ID2", objToString(objToString(currtTwoAdjacentCell.cityId)));
		outTwoWayData.put("NEI_COUNTY_ID2", objToString(objToString(currtTwoAdjacentCell.countyId)));
		outTwoWayData.put("NEI_COUNTY_NAME2", objToString(objToString(currtTwoAdjacentCell.countyName)));
		outTwoWayData.put("NEI_VENDOR2", objToString(objToString(currtCell.vendor)));
		outTwoWayData.put("NEI_VENDOR_NAME2", objToString(objToString(vendorName)));
		outTwoWayData.put("NEI_CELL_NAME2", objToString(currtTwoAdjacentCell.cellName));
		outTwoWayData.put("NEI_PN2", objToString(currtTwoAdjacentCell.pn));
		outTwoWayData.put("NEI_CARRIER2", objToString(currtTwoAdjacentCell.carrId));
		outTwoWayData.put("NEI_CITY_NAME2", objToString(currtTwoAdjacentCell.cityName));
		outTwoWayData.put("NEI_SID2", objToString(currtTwoAdjacentCell.sid));
		outTwoWayData.put("NEI_NE_CELL_ID2", objToString(currtTwoAdjacentCell.neCellId));
		outTwoWayData.put("NEI_NE_BSC_ID2", objToString(currtTwoAdjacentCell.neBscId));
		outTwoWayData.put("NEI_BTS_ID2", objToString(currtTwoAdjacentCell.btsId));
		outTwoWayData.put("NEI_NE_BTS_ID2", objToString(currtTwoAdjacentCell.neBtsId));
		outTwoWayData.put("NBRSEQ_PROPOSED_AC", objToString(currtTwoAdjacentCell.nbrseq));
		Double[] twoCellLngLat = this.neSysIdToNeMap.get(currtTwoAdjacentCell.neSysId);
		if (null != twoCellLngLat && currCellLngLat != null)
			outTwoWayData
					.put("DISTANCE_AC", doubleToString(GeographicalOperation.distanceOperation(currCellLngLat[0], currCellLngLat[1],
							twoCellLngLat[0], twoCellLngLat[1])));
		// 第二小区邻区
		outTwoWayData.put("NEI_NEI_NE_SYS_ID", objToString(objToString(adjacentCellInTwoCell.neSysId)));
		outTwoWayData.put("NEI_NEI_CELL_ID", objToString(objToString(adjacentCellInTwoCell.cellId)));
		outTwoWayData.put("NEI_NEI_CITY_ID", objToString(objToString(adjacentCellInTwoCell.cityId)));
		outTwoWayData.put("NEI_NEI_COUNTY_ID", objToString(objToString(adjacentCellInTwoCell.countyId)));
		outTwoWayData.put("NEI_NEI_COUNTY_NAME", objToString(objToString(adjacentCellInTwoCell.countyName)));
		outTwoWayData.put("NEI_NEI_VENDOR", objToString(objToString(currtCell.vendor)));
		outTwoWayData.put("NEI_NEI_VENDOR_NAME", objToString(vendorName));
		outTwoWayData.put("NEI_NEI_CELL_NAME", objToString(adjacentCellInTwoCell.cellName));
		outTwoWayData.put("NEI_NEI_PN", objToString(adjacentCellInTwoCell.pn));
		outTwoWayData.put("NEI_NEI_CARRIER", objToString(adjacentCellInTwoCell.carrId));
		outTwoWayData.put("NEI_NEI_CITY_NAME", objToString(adjacentCellInTwoCell.cityName));
		outTwoWayData.put("NEI_NEI_SID", objToString(adjacentCellInTwoCell.sid));
		outTwoWayData.put("NEI_NEI_NE_CELL_ID", objToString(adjacentCellInTwoCell.neCellId));
		outTwoWayData.put("NEI_NEI_BTS_ID", objToString(adjacentCellInTwoCell.btsId));
		outTwoWayData.put("NEI_NEI_BSC_ID", objToString(adjacentCellInTwoCell.bscId));
		outTwoWayData.put("NBRSEQ_PROPOSED_CD", objToString(adjacentCellInTwoCell.nbrseq));
		Double[] threeCellLngLat = this.neSysIdToNeMap.get(adjacentCellInTwoCell.neSysId);
		if (null != threeCellLngLat && oneCellLngLat != null)
			outTwoWayData.put("DISTANCE_BD", doubleToString(GeographicalOperation.distanceOperation(oneCellLngLat[0], oneCellLngLat[1],
					threeCellLngLat[0], threeCellLngLat[1])));
		if (null != threeCellLngLat && twoCellLngLat != null)
			outTwoWayData.put("DISTANCE_CD", doubleToString(GeographicalOperation.distanceOperation(twoCellLngLat[0], twoCellLngLat[1],
					threeCellLngLat[0], threeCellLngLat[1])));
		outRecord.setRecord(outTwoWayData);
		cacheElement.add(outRecord);
		summaryCnt[2]++;
	}

	public void processResembleOneWay(CellCfgInfo currtCell, AdjacentCellCfg currtOneAdjacentCell) {
		CellCfgInfo oneCellCfg = null;
		AdjacentCellCfg adjacentCellInfo = null;
		oneCellCfg = this.neSysIdToCellMap.get(currtOneAdjacentCell.neSysId);
		if (oneCellCfg == null)
			return;
		for (int i = 0; i < oneCellCfg.adjacentCells.size(); i++) {
			adjacentCellInfo = oneCellCfg.adjacentCells.get(i);
			if (adjacentCellInfo.neSysId.longValue() == currtCell.neSysId.longValue()
					|| adjacentCellInfo.neSysId.longValue() == currtOneAdjacentCell.neSysId.longValue())
				continue;
			if (adjacentCellInfo.pn.shortValue() != currtCell.pn.shortValue())
				continue;
			// 构建疑似ONEWAY数据
			buildResembleOneWayData(currtCell, currtOneAdjacentCell, adjacentCellInfo);
		}
	}

	private void buildResembleOneWayData(CellCfgInfo currtCell, AdjacentCellCfg currtOneAdjacentCell, AdjacentCellCfg adjacentCellInOneCell) {
		ParseOutRecord outRecord = new ParseOutRecord();
		outRecord.setType(RESEMBLE_WAY_DATA_TYPY);
		Map<String, String> outResembleOneWayData = this.createExportPropertyMap(RESEMBLE_WAY_DATA_TYPY);
		String vendorName = Vendor.getVendorName(currtCell.vendor);
		outResembleOneWayData.put("START_TIME", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(this.currentDataTime));
		outResembleOneWayData.put("NE_SYS_ID", objToString(currtCell.neSysId));
		outResembleOneWayData.put("CELL_ID", objToString(currtCell.cellId));
		outResembleOneWayData.put("CITY_ID", objToString(currtCell.cityId));
		outResembleOneWayData.put("CITY_NAME", objToString(currtCell.cityName));
		outResembleOneWayData.put("COUNTY_ID", objToString(currtCell.countyId));
		outResembleOneWayData.put("COUNTY_NAME", objToString(currtCell.countyName));
		outResembleOneWayData.put("BTS_ID", objToString(currtCell.btsId));
		outResembleOneWayData.put("BSC_ID", objToString(currtCell.bscId));
		outResembleOneWayData.put("VENDOR", objToString(currtCell.vendor));
		outResembleOneWayData.put("VENDOR_NAME", objToString(vendorName));
		outResembleOneWayData.put("CELL_NAME", objToString(currtCell.cellName));
		outResembleOneWayData.put("PN", objToString(currtCell.pn));
		outResembleOneWayData.put("CARRIER", objToString(currtCell.carrId));
		outResembleOneWayData.put("ADJTYPE", objToString(currtCell.adjtype));
		outResembleOneWayData.put("SID", objToString(currtCell.sid));
		outResembleOneWayData.put("DATATYPE", "0");
		outResembleOneWayData.put("BUSTYPE", this.busType);
		outResembleOneWayData.put("NE_CELL_ID", objToString(currtCell.neCellId));
		outResembleOneWayData.put("NE_BTS_ID", objToString(currtCell.neBtsId));
		outResembleOneWayData.put("NE_BSC_ID", objToString(currtCell.neBscId));
		Double[] currCellLngLat = this.neSysIdToNeMap.get(currtCell.neSysId);
		// 第一小区
		outResembleOneWayData.put("NEI_NE_SYS_ID_B", objToString(currtOneAdjacentCell.neSysId));
		outResembleOneWayData.put("NEI_CELL_ID_B", objToString(currtOneAdjacentCell.cellId));
		outResembleOneWayData.put("NEI_CITY_ID_B", objToString(currtOneAdjacentCell.cityId));
		outResembleOneWayData.put("NEI_CITY_NAME_B", objToString(currtOneAdjacentCell.cityName));
		outResembleOneWayData.put("NEI_COUNTY_ID_B", objToString(currtOneAdjacentCell.countyId));
		outResembleOneWayData.put("NEI_COUNTY_NAME_B", objToString(currtOneAdjacentCell.countyName));
		outResembleOneWayData.put("NEI_BTS_ID_B", objToString(currtOneAdjacentCell.btsId));
		outResembleOneWayData.put("NEI_BSC_ID_B", objToString(currtOneAdjacentCell.bscId));
		outResembleOneWayData.put("NEI_VENDOR_B", objToString(currtCell.vendor));
		outResembleOneWayData.put("NEI_VENDOR_NAME_B", objToString(vendorName));
		outResembleOneWayData.put("NEI_CELL_NAME_B", objToString(currtOneAdjacentCell.cellName));
		outResembleOneWayData.put("NEI_PN_B", objToString(currtOneAdjacentCell.pn));
		outResembleOneWayData.put("ADJSTATE_B", objToString(currtOneAdjacentCell.adjstate));
		outResembleOneWayData.put("NEI_CARRIER_B", objToString(currtOneAdjacentCell.carrId));
		outResembleOneWayData.put("NEI_SID_B", objToString(currtOneAdjacentCell.sid));
		outResembleOneWayData.put("NEI_NE_CELL_ID_B", objToString(currtOneAdjacentCell.neCellId));
		outResembleOneWayData.put("NE_BTS_ID_B", objToString(currtOneAdjacentCell.neBtsId));
		outResembleOneWayData.put("NE_BSC_ID_B", objToString(currtOneAdjacentCell.neBscId));
		Double[] oneCellLngLat = this.neSysIdToNeMap.get(currtOneAdjacentCell.neSysId);
		if (null != oneCellLngLat && currCellLngLat != null)
			outResembleOneWayData
					.put("DISTANCE_B", doubleToString(GeographicalOperation.distanceOperation(currCellLngLat[0], currCellLngLat[1], oneCellLngLat[0],
							oneCellLngLat[1])));
		// 第二小区
		outResembleOneWayData.put("NEI_NE_SYS_ID_C", objToString(adjacentCellInOneCell.neSysId));
		outResembleOneWayData.put("NEI_CELL_ID_C", objToString(adjacentCellInOneCell.cellId));
		outResembleOneWayData.put("NEI_CITY_ID_C", objToString(adjacentCellInOneCell.cityId));
		outResembleOneWayData.put("NEI_CITY_NAME_C", objToString(adjacentCellInOneCell.cityName));
		outResembleOneWayData.put("NEI_COUNTY_ID_C", objToString(adjacentCellInOneCell.countyId));
		outResembleOneWayData.put("NEI_COUNTY_NAME_C", objToString(adjacentCellInOneCell.countyName));
		outResembleOneWayData.put("NEI_BTS_ID_C", objToString(adjacentCellInOneCell.btsId));
		outResembleOneWayData.put("NEI_BSC_ID_C", objToString(adjacentCellInOneCell.bscId));
		outResembleOneWayData.put("NEI_VENDOR_C", objToString(currtCell.vendor));
		outResembleOneWayData.put("NEI_VENDOR_NAME_C", objToString(vendorName));
		outResembleOneWayData.put("NEI_CELL_NAME_C", objToString(adjacentCellInOneCell.cellName));
		outResembleOneWayData.put("NEI_PN_C", objToString(adjacentCellInOneCell.pn));
		outResembleOneWayData.put("ADJSTATE_C", objToString(adjacentCellInOneCell.adjstate));
		outResembleOneWayData.put("NEI_CARRIER_C", objToString(adjacentCellInOneCell.carrId));
		outResembleOneWayData.put("NEI_SID_C", objToString(adjacentCellInOneCell.sid));
		outResembleOneWayData.put("NEI_NE_CELL_ID_C", objToString(adjacentCellInOneCell.neCellId));
		outResembleOneWayData.put("NE_BTS_ID_C", objToString(adjacentCellInOneCell.neBtsId));
		outResembleOneWayData.put("NE_BSC_ID_C", objToString(adjacentCellInOneCell.neBscId));
		Double[] twoCellLngLat = this.neSysIdToNeMap.get(adjacentCellInOneCell.neSysId);
		if (null != twoCellLngLat && oneCellLngLat != null)
			outResembleOneWayData.put("DISTANCE_C",
					doubleToString(GeographicalOperation.distanceOperation(oneCellLngLat[0], oneCellLngLat[1], twoCellLngLat[0], twoCellLngLat[1])));
		outRecord.setRecord(outResembleOneWayData);
		cacheElement.add(outRecord);
		summaryCnt[1]++;
	}

	public void processOneWay(CellCfgInfo currtCell, AdjacentCellCfg currtOneAdjacentCell, AdjacentCellCfg currtTwoAdjacentCell) {
		if (currtCell.neSysId.longValue() == currtTwoAdjacentCell.neSysId.longValue()
				|| currtOneAdjacentCell.neSysId.longValue() == currtTwoAdjacentCell.neSysId.longValue())
			return;
		if (!Vendor.VENDOR_ALC.equals(vendor)) {
			if (currtOneAdjacentCell.carrId.shortValue() != currtTwoAdjacentCell.carrId.shortValue())
				return;
		}
		if (currtOneAdjacentCell.pn.shortValue() != currtTwoAdjacentCell.pn.shortValue())
			return;
		// 构建ONEWAY数据
		buildOneWayData(currtCell, currtOneAdjacentCell, currtTwoAdjacentCell);
	}

	public void buildOneWayData(CellCfgInfo currtCell, AdjacentCellCfg currtOneAdjacentCell, AdjacentCellCfg currtTwoAdjacentCell) {
		ParseOutRecord outRecord = new ParseOutRecord();
		outRecord.setType(ONE_WAY_DATA_TYPY);
		Map<String, String> outOneWayData = this.createExportPropertyMap(ONE_WAY_DATA_TYPY);
		String vendorName = Vendor.getVendorName(currtCell.vendor);
		outOneWayData.put("START_TIME", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(this.currentDataTime));
		outOneWayData.put("NE_SYS_ID", objToString(currtCell.neSysId));
		outOneWayData.put("CELL_ID", objToString(currtCell.cellId));
		outOneWayData.put("CITY_ID", objToString(currtCell.cityId));
		outOneWayData.put("COUNTY_ID", objToString(currtCell.countyId));
		outOneWayData.put("COUNTY_NAME", objToString(currtCell.countyName));
		outOneWayData.put("VENDOR", objToString(currtCell.vendor));
		outOneWayData.put("VENDOR_NAME", objToString(vendorName));
		outOneWayData.put("CELL_NAME", objToString(currtCell.cellName));
		outOneWayData.put("PN", objToString(currtCell.pn));
		outOneWayData.put("CARRIER", objToString(currtCell.carrId));
		outOneWayData.put("ADJTYPE", objToString(currtCell.adjtype));
		outOneWayData.put("CITY_NAME", objToString(currtCell.cityName));
		outOneWayData.put("SID", objToString(currtCell.sid));
		outOneWayData.put("BTS_ID", objToString(currtCell.btsId));
		outOneWayData.put("NE_CELL_ID", objToString(currtCell.neCellId));
		outOneWayData.put("NE_BTS_ID", objToString(currtCell.btsId));
		outOneWayData.put("NE_BSC_ID", objToString(currtCell.neBscId));
		// 第一小区
		outOneWayData.put("NEI_NE_SYS_ID", objToString(currtOneAdjacentCell.neSysId));
		outOneWayData.put("NEI_CELL_ID", objToString(currtOneAdjacentCell.cellId));
		outOneWayData.put("NEI_CITY_ID", objToString(currtOneAdjacentCell.cityId));
		outOneWayData.put("NEI_COUNTY_ID", objToString(currtOneAdjacentCell.countyId));
		outOneWayData.put("NEI_COUNTY_NAME", objToString(currtOneAdjacentCell.countyName));
		outOneWayData.put("NEI_VENDOR", objToString(currtCell.vendor));
		outOneWayData.put("NEI_VENDOR_NAME", objToString(vendorName));
		outOneWayData.put("NEI_CELL_NAME", objToString(currtOneAdjacentCell.cellName));
		outOneWayData.put("NEI_PN", objToString(currtOneAdjacentCell.pn));
		outOneWayData.put("NEI_CARRIER", objToString(currtOneAdjacentCell.carrId));
		outOneWayData.put("NEI_CITY_NAME", objToString(currtOneAdjacentCell.cityName));
		outOneWayData.put("NEI_SID", objToString(currtOneAdjacentCell.sid));
		outOneWayData.put("NEI_NE_CELL_ID", objToString(currtOneAdjacentCell.neCellId));
		outOneWayData.put("NEI_BTS_ID", objToString(currtOneAdjacentCell.btsId));
		outOneWayData.put("NEI_NE_BTS_ID", objToString(currtOneAdjacentCell.neBtsId));
		outOneWayData.put("NEI_NE_BSC_ID", objToString(currtOneAdjacentCell.neBscId));
		Double[] oneCellLngLat = this.neSysIdToNeMap.get(currtOneAdjacentCell.neSysId);
		// 第二小区
		outOneWayData.put("NEI_NE_SYS_ID2", objToString(currtTwoAdjacentCell.neSysId));
		outOneWayData.put("NEI_CELL_ID2", objToString(currtTwoAdjacentCell.cellId));
		outOneWayData.put("NEI_CITY_ID2", objToString(currtTwoAdjacentCell.cityId));
		outOneWayData.put("NEI_COUNTY_ID2", objToString(currtTwoAdjacentCell.countyId));
		outOneWayData.put("NEI_COUNTY_NAME2", objToString(currtTwoAdjacentCell.countyName));
		outOneWayData.put("NEI_VENDOR2", objToString(currtCell.vendor));
		outOneWayData.put("NEI_VENDOR_NAME2", objToString(vendorName));
		outOneWayData.put("NEI_CELL_NAME2", objToString(currtTwoAdjacentCell.cellName));
		outOneWayData.put("NEI_PN2", objToString(currtTwoAdjacentCell.pn));
		outOneWayData.put("NEI_CARRIER2", objToString(currtTwoAdjacentCell.carrId));
		outOneWayData.put("NEI_CITY_NAME2", objToString(currtTwoAdjacentCell.cityName));
		outOneWayData.put("NEI_SID2", objToString(currtTwoAdjacentCell.sid));
		outOneWayData.put("NEI_NE_CELL_ID2", objToString(currtTwoAdjacentCell.neCellId));
		outOneWayData.put("NEI_BTS_ID2", objToString(currtTwoAdjacentCell.btsId));
		outOneWayData.put("NEI_NE_BTS_ID2", objToString(currtTwoAdjacentCell.neBtsId));
		outOneWayData.put("NEI_NE_BSC_ID2", objToString(currtTwoAdjacentCell.neBscId));
		Double[] twoCellLngLat = this.neSysIdToNeMap.get(currtTwoAdjacentCell.neSysId);
		if (null != oneCellLngLat && twoCellLngLat != null)
			outOneWayData.put("DISTANCE_BC",
					doubleToString(GeographicalOperation.distanceOperation(oneCellLngLat[0], oneCellLngLat[1], twoCellLngLat[0], twoCellLngLat[1])));
		outRecord.setRecord(outOneWayData);
		cacheElement.add(outRecord);
		summaryCnt[0]++;
	}

	@Override
	public List<ParseOutRecord> getAllRecords() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		super.close();
		LOGGER.debug("汇总结束，总共汇总输出记录数：para_oneway={}、para_oneway_v2={}、para_twoway={}", new Object[]{summaryCnt[0], summaryCnt[1], summaryCnt[2]});
	}

	private final String objToString(Object obj) {
		if (null == obj)
			return "";
		return obj.toString();
	}

	private String doubleToString(double distanceOperation) {
		// 单位转换成米，返回精确到小数点后两位。
		return df.format(distanceOperation * 1000);
	}

}
