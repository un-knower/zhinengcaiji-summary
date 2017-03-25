package cn.uway.summary.DataJoin;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.util.ArrayMap;
import cn.uway.util.DbUtil;
import cn.uway.util.StringUtil;

public class DataJoinner {

	protected static final Logger LOGGER = LoggerFactory
			.getLogger(DataJoinner.class);

	private final static SimpleDateFormat timeFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:SS");

	private static final int CLOBBUFFERSIZE = 2048;

	// note: we want to maintain compatibility with Java 5 VM's
	// These types don't exist in Java 5
	private static final int NVARCHAR = -9;

	private static final int NCHAR = -15;

	private static final int LONGNVARCHAR = -16;

	private static final int NCLOB = 2011;

	protected ArrayMap<String, String> records = new ArrayMap<String, String>();

	public DataJoinSet leftJoin(DataJoinSet set1, DataJoinSet set2,
			String primaryKeys, boolean bAutoClearJoinSet) {
		return this.joinRecord(set1, set2, true, false, primaryKeys, bAutoClearJoinSet);
	}

	public DataJoinSet rightJion(DataJoinSet set1, DataJoinSet set2,
			String primaryKeys, boolean bAutoClearJoinSet) {
		return this.joinRecord(set1, set2, false, true, primaryKeys, bAutoClearJoinSet);
	}

	public DataJoinSet fullJoin(DataJoinSet set1, DataJoinSet set2,
			String primaryKeys, boolean bAutoClearJoinSet) {
		return this.joinRecord(set1, set2, true, true, primaryKeys, bAutoClearJoinSet);
	}

	public DataJoinSet innerJoin(DataJoinSet set1, DataJoinSet set2,
			String primaryKeys, boolean bAutoClearJoinSet) {
		return this.joinRecord(set1, set2, false, false, primaryKeys, bAutoClearJoinSet);
	}

	public static void main(String[] args) throws Exception {
		Class.forName("oracle.jdbc.OracleDriver");
		Connection conn = DriverManager.getConnection(
				"jdbc:oracle:thin:@192.168.15.223:1521:ora11", "wcdma",
				"uwaysoft2010");

		DataJoinner jonner = new DataJoinner();
		DataJoinSet set1 = jonner
				.LoadDataFromSql(
						conn,
						"select t1.start_time, t1.ne_cell_id, t1.city_id, t1.nbrcs2gto3ghoinsucc, t1.nbr2gto3ghoinreq from perf_cell_w_h t1 where t1.start_time >= to_date('2011-11-02', 'yyyy-mm-dd') and  t1.start_time <= to_date('2011-11-04', 'yyyy-mm-dd')",
						"NE_CELL_ID, CITY_ID");
		DataJoinSet set2 = jonner
				.LoadDataFromSql(
						conn,
						"select ne_cell_id, city_id, cell_name, city_name from ne_cell_w_gd",
						"NE_CELL_ID, CITY_ID");
		DataJoinSet set3 = jonner.innerJoin(set1, set2,
				"start_time, ne_cell_id", true);

		System.out.println(set3.records.size());

	}

	public DataJoinSet LoadDataFromSql(Connection conn, String sql,
			String primaryKeys) {
		PreparedStatement stm = null;
		ResultSet rs = null;
		ResultSetMetaData meta = null;
		DataJoinSet dataSet = null;
		try {
			stm = conn.prepareStatement(sql);
			stm.setFetchSize(1024);
			rs = stm.executeQuery();
			rs.setFetchSize(1000);

			meta = rs.getMetaData();
			int columnCount = meta.getColumnCount();
			dataSet = new DataJoinSet();
			DataBlockManager blockManager = DataBlockManager.getInstance();
			DataBlock block = null;

			ArrayList<String> headers = new ArrayList<String>(columnCount);
			String[] joinKeys = primaryKeys.split(",");
			Integer[] keyIndex = new Integer[joinKeys.length];
			dataSet.setPrimaryKeys(joinKeys);

			for (int i = 0; i < columnCount; ++i) {
				headers.add(meta.getColumnName(i + 1).toUpperCase());
			}
			dataSet.setColumnHeader(headers);

			// 找出key对应的数据位置
			for (int i = 0; i < joinKeys.length; ++i) {
				joinKeys[i] = joinKeys[i].trim();
				for (int j = 0; j < headers.size(); ++j) {
					if (joinKeys[i].equalsIgnoreCase(headers.get(j))) {
						keyIndex[i] = j;
						break;
					}
				}
			}

			// 读取记录
			StringBuilder sb = new StringBuilder(2048);
			String value[] = new String[columnCount];
			while (rs.next()) {
				for (int i = 0; i < columnCount; ++i) {
					int type = meta.getColumnType(i + 1);
					value[i] = getColumnValue(rs, type, i + 1);
				}

				// 组装记录的key
				sb.setLength(0);
				for (int i = 0; i < joinKeys.length; ++i) {
					if (keyIndex[i] == null)
						continue;

					if (i > 0)
						sb.append("--");
					sb.append(value[keyIndex[i]]);
				}
				String recordKey = sb.toString();

				// 组装记录内容
				sb.setLength(0);
				for (int i = 0; i < columnCount; ++i) {
					if (i > 0)
						sb.append(",");

					if (value[i] != null) {
						sb.append(value[i]);
					}
				}

				byte[] buffRecord = sb.toString().getBytes();
				if (block == null || !block.canSave(buffRecord.length)) {
					if (block != null) {
						blockManager.addBlockData(block);
					}

					block = blockManager.allowBlockData();
				}

				int start = block.put(buffRecord);
				DataJoinRecord record = new DataJoinRecord(recordKey, block,
						start, buffRecord.length);
				// record.primaryKey = recordKey;
				// record.values = sb.toString();
				dataSet.records.add(record);
			}

			if (block != null) {
				blockManager.addBlockData(block);
				block = null;
			}

			dataSet.sort();
			return dataSet;
		} catch (Exception e) {
			LOGGER.error("加载数据源发生错误，sql={}", sql, e);
			if (dataSet != null)
				dataSet.clear();
			
		} finally {
			DbUtil.close(rs, stm, null);
		}

		return null;
	}

	protected DataJoinSet joinRecord(DataJoinSet set1, DataJoinSet set2,
			final boolean bLeftJoinCase, final boolean bRightJoinCase,
			String primaryKeys, boolean bAutoClearJoinSet) {
		if (set1 == null || set1.records.size() < 1) {
			if (bRightJoinCase) {
				if (bAutoClearJoinSet)
					set1.clear();
				return set2;
			}
			
			if (bAutoClearJoinSet)
				set2.clear();
			return set1;
		}

		if (set2 == null || set2.records.size() < 1) {
			if (bLeftJoinCase) {
				if (bAutoClearJoinSet)
					set2.clear();
				return set1;
			}

			if (bAutoClearJoinSet)
				set1.clear();
			return set2;
		}

		// 合并header
		ArrayList<String> leftRecordHeaders = set1.getColumnHeader();
		ArrayList<String> rightRecordHeaders = set2.getColumnHeader();
		ArrayList<String> mergeRecordHeaders = new ArrayList<String>(
				leftRecordHeaders.size() + rightRecordHeaders.size());

		Map<String, Integer> mapMergeRecordHeadersPos = new HashMap<String, Integer>();
		for (String header : leftRecordHeaders) {
			mapMergeRecordHeadersPos.put(header, mergeRecordHeaders.size());
			mergeRecordHeaders.add(header);
		}

		for (String header : rightRecordHeaders) {
			if (mapMergeRecordHeadersPos.containsKey(header))
				continue;

			mapMergeRecordHeadersPos.put(header, mergeRecordHeaders.size());
			mergeRecordHeaders.add(header);
		}

		// 创建合并的数据集
		DataJoinSet mergeDataSet = new DataJoinSet();
		mergeDataSet.setColumnHeader(mergeRecordHeaders);

		// 拆分索引keys
		String[] joinKeys = primaryKeys.split(",");
		Integer[] keyIndex = new Integer[joinKeys.length];
		mergeDataSet.setPrimaryKeys(joinKeys);

		// 找出key对应的数据位置
		for (int i = 0; i < joinKeys.length; ++i) {
			joinKeys[i] = joinKeys[i].trim();
			for (int j = 0; j < mergeRecordHeaders.size(); ++j) {
				if (joinKeys[i].equalsIgnoreCase(mergeRecordHeaders.get(j))) {
					keyIndex[i] = j;
					break;
				}
			}
		}

		// 合并记录(依次弹出两个队列中，位于顶部小的那条记录，直至完成)
		StringBuilder sb = new StringBuilder(2048);
		DataJoinRecord leftRecord = null;
		DataJoinRecord rightRecord = null;

		// 合并的数据集位置
		String[] mergeRecordValue = new String[mergeRecordHeaders.size()];
		boolean bBuilder = false;
		DataBlockManager blockManager = DataBlockManager.getInstance();
		DataBlock block = null;

		// 左右记录是否被使用过
		boolean bLeftRecordUsed = false;
		boolean bRightRecordUsed = false;
		while (set1.records.size() > 0 || set2.records.size() > 0) {
			if (leftRecord == null && set1.records.size() > 0) {
				leftRecord = set1.records.remove(0);
				bLeftRecordUsed = false;
			}
			if (rightRecord == null && set2.records.size() > 0) {
				rightRecord = set2.records.remove(0);
				bRightRecordUsed = false;
			}

			// 比较两个队列的首条记录的key值
			int nCompareRet = 1;
			if (leftRecord == null) {
				// 将比较结果设置为左列大于右列，让其不断弹出右列的记录
				nCompareRet = 1;
				if (bLeftJoinCase) {
					if (bAutoClearJoinSet) {
						if (rightRecord != null) {
							rightRecord.destory();
							rightRecord = null;
						}
						
						set2.clear();
					}
					
					break;
				}
			} else if (rightRecord == null) {
				// 将比较结果设置为左列小于右列，让其不断弹出左列的记录
				nCompareRet = -1;
				if (bRightJoinCase) {
					if (bAutoClearJoinSet) {
						if (leftRecord != null) {
							leftRecord.destory();
							leftRecord = null;
						}
						
						set1.clear();
					}
					
					break;
				}
			} else {
				nCompareRet = leftRecord.primaryKey
						.compareTo(rightRecord.primaryKey);
			}

			bBuilder = false;
			if (nCompareRet == 0) {
				bLeftRecordUsed = true;
				bRightRecordUsed = true;
				bBuilder = true;

				// 将左记录按列顺序复制到mergeRecordValue中
				fillRecord(mergeRecordValue, null, leftRecord, null, true);
				// 将右记录按实际字段顺序复制到mergeRecordValue中
				fillRecord(mergeRecordValue, mapMergeRecordHeadersPos,
						rightRecord, rightRecordHeaders, false);

				// in many to one or one to one case :(多对一情况下，右列首条不能在未关联完前弹出)
				if (set1.records.size() < 1
						|| !set1.records.get(0).primaryKey
								.equals(leftRecord.primaryKey)) {
					if (bAutoClearJoinSet)
						rightRecord.destory();
					rightRecord = null;
				}

				// in on to many or one to one case: (一对多情况下，左列首条不能在未关联完前弹出)
				if (set2.records.size() < 1
						|| !set2.records.get(0).primaryKey
								.equals(leftRecord.primaryKey)) {
					if (bAutoClearJoinSet)
						leftRecord.destory();
					leftRecord = null;
				}

				// in many to many case
				// TODO: many to many 方法不支持，此处将只生成一条记录
			} else if (nCompareRet < 0) {
				// 左比右小，弹出左列首条记录
				if (!bLeftRecordUsed && bLeftJoinCase) {
					bLeftRecordUsed = true;
					bBuilder = true;
					fillRecord(mergeRecordValue, null, leftRecord, null, true);
				}

				if (bAutoClearJoinSet)
					leftRecord.destory();
				leftRecord = null;
			} else if (nCompareRet > 0) {
				// 左比右列大，弹出右列的首条记录
				if (!bRightRecordUsed && bRightJoinCase) {
					bRightRecordUsed = true;
					bBuilder = true;
					fillRecord(mergeRecordValue, mapMergeRecordHeadersPos,
							rightRecord, rightRecordHeaders, false);
				}

				if (bAutoClearJoinSet)
					rightRecord.destory();
				rightRecord = null;
			}

			if (bBuilder) {
				// 组装记录的key
				sb.setLength(0);
				for (int i = 0; i < joinKeys.length; ++i) {
					if (keyIndex[i] == null)
						continue;

					if (i > 0)
						sb.append("--");
					sb.append(mergeRecordValue[keyIndex[i]]);
				}
				String recordKey = sb.toString();

				// 组装记录内容
				sb.setLength(0);
				for (int i = 0; i < mergeRecordHeaders.size(); ++i) {
					if (i > 0)
						sb.append(",");

					if (mergeRecordValue[i] != null) {
						sb.append(mergeRecordValue[i]);
					}
				}

				byte[] buffRecord = sb.toString().getBytes();
				if (block == null || !block.canSave(buffRecord.length)) {
					if (block != null) {
						blockManager.addBlockData(block);
					}

					block = blockManager.allowBlockData();
				}

				int start = block.put(buffRecord);
				DataJoinRecord mergeRecord = new DataJoinRecord(recordKey,
						block, start, buffRecord.length);
				mergeDataSet.records.add(mergeRecord);
			}
		}

		if (block != null) {
			blockManager.addBlockData(block);
			block = null;
		}

		return mergeDataSet;
	}

	/**
	 * 合并记录集，将记录集2的内容合并到记录集1中
	 * 
	 * @param set1
	 *            记录集1
	 * @param set2
	 *            记录集2
	 * @param primaryKeys
	 *            新主键
	 * @param bAutoClearJoinSet
	 *			  是否自动清除set2的记录集            
	 * @return
	 */
	protected DataJoinSet UnionRecord(DataJoinSet set1, DataJoinSet set2,
			String primaryKeys, boolean bAutoClearJoinSet) {
		if (set1 == null || set1.records.size() < 1) {
			return set2;
		}

		if (set2 == null || set2.records.size() < 1) {
			return set1;
		}

		// 合并header
		ArrayList<String> leftRecordHeaders = set1.getColumnHeader();
		ArrayList<String> rightRecordHeaders = set2.getColumnHeader();
		ArrayList<String> unionRecordHeaders = new ArrayList<String>(
				leftRecordHeaders.size() + rightRecordHeaders.size());

		Map<String, Integer> mapMergeRecordHeadersPos = new HashMap<String, Integer>();
		for (String header : leftRecordHeaders) {
			mapMergeRecordHeadersPos.put(header, unionRecordHeaders.size());
			unionRecordHeaders.add(header);
		}

		for (String header : rightRecordHeaders) {
			if (mapMergeRecordHeadersPos.containsKey(header))
				continue;

			mapMergeRecordHeadersPos.put(header, unionRecordHeaders.size());
			unionRecordHeaders.add(header);
		}

		// 拆分索引keys
		String[] joinKeys = primaryKeys.split(",");
		Integer[] keyIndex = new Integer[joinKeys.length];
		set1.setPrimaryKeys(joinKeys);

		// 找出key对应的数据位置
		for (int i = 0; i < joinKeys.length; ++i) {
			joinKeys[i] = joinKeys[i].trim();
			for (int j = 0; j < unionRecordHeaders.size(); ++j) {
				if (joinKeys[i].equalsIgnoreCase(unionRecordHeaders.get(j))) {
					keyIndex[i] = j;
					break;
				}
			}
		}

		String[] unionRecordValue = new String[unionRecordHeaders.size()];
		StringBuilder sb = new StringBuilder(2048);
		DataBlockManager blockManager = DataBlockManager.getInstance();
		DataBlock block = null;

		// 重新生成set1的primaryKey(如果header新增了字段)
		if (leftRecordHeaders.size() < unionRecordHeaders.size()) {
			// 设置新的columnHeaders(将set2的column Headers合并到set1的column headers中)
			set1.setColumnHeader(unionRecordHeaders);

			for (DataJoinRecord record : set1.records) {
				fillRecord(unionRecordValue, null, record, null, true);

				// 组装记录的key
				sb.setLength(0);
				for (int i = 0; i < joinKeys.length; ++i) {
					if (keyIndex[i] == null)
						continue;

					if (i > 0)
						sb.append("--");
					sb.append(unionRecordValue[keyIndex[i]]);
				}
				record.primaryKey = sb.toString();
			}
		}

		// 将set2的内容龕并到set1中;
		while (set2.records.size() > 0) {
			DataJoinRecord record = set2.records.remove(0);
			fillRecord(unionRecordValue, mapMergeRecordHeadersPos, record,
					rightRecordHeaders, true);

			{
				// 组装记录的key
				sb.setLength(0);
				for (int i = 0; i < joinKeys.length; ++i) {
					if (keyIndex[i] == null)
						continue;

					if (i > 0)
						sb.append("--");
					sb.append(unionRecordValue[keyIndex[i]]);
				}
				String recordKey = sb.toString();

				// 组装记录内容
				sb.setLength(0);
				for (int i = 0; i < unionRecordHeaders.size(); ++i) {
					if (i > 0)
						sb.append(",");

					if (unionRecordValue[i] != null) {
						sb.append(unionRecordValue[i]);
					}
				}

				byte[] buffRecord = sb.toString().getBytes();
				if (block == null || !block.canSave(buffRecord.length)) {
					if (block != null) {
						blockManager.addBlockData(block);
					}

					block = blockManager.allowBlockData();
				}

				int start = block.put(buffRecord);
				DataJoinRecord mergeRecord = new DataJoinRecord(recordKey,
						block, start, buffRecord.length);
				set1.records.add(mergeRecord);
			}

			// 删掉union 2记录集的记录
			if (bAutoClearJoinSet)
				record.destory();
			record = null;
		}

		if (block != null) {
			blockManager.addBlockData(block);
			block = null;
		}

		return set1;
	}

	/**
	 * 将DataJoinRecord的内容，合并到mergeRecordValue数据组
	 * 
	 * @param mergeRecordValue
	 *            被合并的数组
	 * @param mapMergeRecordHeadersPos
	 *            合并的数组字段对应的位置信息
	 * @param record
	 *            原记录
	 * @param recordHeaders
	 *            原记录的列头
	 * @param bInitMerageRecordValue
	 *            是否初始mergeRecordValue数据各元索的值
	 * @return
	 */
	protected boolean fillRecord(String[] mergeRecordValue,
			Map<String, Integer> mapMergeRecordHeadersPos,
			DataJoinRecord record, ArrayList<String> recordHeaders,
			boolean bInitMerageRecordValue) {
		Integer targetRecordFieldIndex = -1;
		int prePos = 0;
		int pos = 0;
		int srcRecordFieldIndex = 0;

		if (bInitMerageRecordValue) {
			for (int i = 0; i < mergeRecordValue.length; ++i) {
				mergeRecordValue[i] = null;
			}
		}

		String valueLine = record.getRecordLineValue();
		pos = valueLine.indexOf(",");
		while (pos <= valueLine.length()) {
			// 　找出record的字段在要填充的记录中的字段位置(如果未设置字段位置信息，则从第一个开始顺序填充）
			if (mapMergeRecordHeadersPos == null || recordHeaders == null) {
				++targetRecordFieldIndex;
			} else {
				targetRecordFieldIndex = mapMergeRecordHeadersPos
						.get(recordHeaders.get(srcRecordFieldIndex));
				++srcRecordFieldIndex;
			}

			if (targetRecordFieldIndex == null)
				continue;

			if (pos > prePos) {
				mergeRecordValue[targetRecordFieldIndex] = valueLine.substring(
						prePos, pos);
			}

			if (pos >= valueLine.length())
				break;

			prePos = pos + 1;
			pos = valueLine.indexOf(',', prePos);
			if (pos < 0) {
				pos = valueLine.length();
			}
		}

		return true;
	}

	public static String getColumnValue(ResultSet rs, int colType, int colIndex)
			throws SQLException, IOException {
		String value = "";
		switch (colType) {
			case Types.BIT :
			case Types.JAVA_OBJECT :
				value = handleObject(rs.getObject(colIndex));
				break;
			case Types.BOOLEAN :
				boolean b = rs.getBoolean(colIndex);
				value = Boolean.valueOf(b).toString();
				break;
			case NCLOB : // todo : use rs.getNClob
			case Types.CLOB :
				Clob c = rs.getClob(colIndex);
				if (c != null)
					value = read(c);
				break;
			case Types.BIGINT :
				value = handleLong(rs, colIndex);
				break;
			case Types.DECIMAL :
			case Types.DOUBLE :
			case Types.FLOAT :
			case Types.REAL :
			case Types.NUMERIC :
				value = handleBigDecimal(rs.getBigDecimal(colIndex));
				break;
			case Types.INTEGER :
			case Types.TINYINT :
			case Types.SMALLINT :
				value = handleInteger(rs, colIndex);
				break;
			case Types.DATE :
				value = handleTimestamp(rs.getTimestamp(colIndex));
				break;
			case Types.TIME :
				value = handleTime(rs.getTime(colIndex));
				break;
			case Types.TIMESTAMP :
				value = handleTimestamp(rs.getTimestamp(colIndex));
				break;
			case NVARCHAR : 		// todo : use rs.getNString
			case NCHAR : 			// todo : use rs.getNString
			case LONGNVARCHAR : 	// todo : use rs.getNString
			case Types.LONGVARCHAR :
			case Types.VARCHAR :
			case Types.CHAR :
				value = rs.getString(colIndex);
				if (StringUtil.isNotEmpty(value)) {
					if (value.contains(",")) {
						// value = "\"" + value + "\"";
						value = value.replace(",", ";");
					}

				}
				break;
			default :
				break;
		}
		
		return value;
	}

	private static String handleObject(Object obj) {
		return obj == null ? "" : String.valueOf(obj);
	}

	private static  String handleBigDecimal(BigDecimal decimal) {
		return decimal == null ? "" : decimal.toString();
	}

	private static String handleLong(ResultSet rs, int columnIndex)
			throws SQLException {
		long lv = rs.getLong(columnIndex);
		return rs.wasNull() ? "" : Long.toString(lv);
	}

	private static String handleInteger(ResultSet rs, int columnIndex)
			throws SQLException {
		int i = rs.getInt(columnIndex);
		return rs.wasNull() ? "" : Integer.toString(i);
	}

	private static String handleTime(Time time) {
		return time == null ? null : time.toString();
	}

	private static String handleTimestamp(Timestamp timestamp) {
		return timestamp == null ? null : timeFormat.format(timestamp);
	}

	private static String read(Clob c) throws SQLException, IOException {
		StringBuilder sb = new StringBuilder((int) c.length());
		Reader r = c.getCharacterStream();
		char[] cbuf = new char[CLOBBUFFERSIZE];
		int n;
		while ((n = r.read(cbuf, 0, cbuf.length)) != -1)
			sb.append(cbuf, 0, n);
		return sb.toString();
	}
}
