package cn.uway.summary.DataJoin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cn.uway.summary.DataJoin.DataJoinRecord.DataJoinRecordComparator;



public class DataJoinSet {
	/**
	 * 数据集合的header
	 */
	protected ArrayList<String> columnHeader;
	protected String [] primaryKeys;
	protected Map<String, Integer> columnHeaderIndex;
	
	public List<DataJoinRecord> records = new LinkedList<DataJoinRecord>();
	
	public void sort() {
		DataJoinRecordComparator comparator = new DataJoinRecordComparator();
		Collections.sort(records, comparator);
	}
	
	public ArrayList<String> getColumnHeader() {
		return columnHeader;
	}
	
	public void setColumnHeader(ArrayList<String> header) {
		this.columnHeader = header;
		buildColumnHeaderIndex();
	}
	
	public String[] getPrimaryKeys() {
		return primaryKeys;
	}
	
	public void setPrimaryKeys(String[] keys) {
		this.primaryKeys = keys;
	}
	
	public void clear() {
		if (records == null)
			return;
		
		for (DataJoinRecord record : records) {
			record.destory();
		}
		records.clear();
	}
	
	protected void buildColumnHeaderIndex() {
		if (columnHeaderIndex==null) {
			columnHeaderIndex = new HashMap<String, Integer>(this.columnHeader.size());
		}
		
		int i = 0;
		for (String column: columnHeader) {
			columnHeaderIndex.put(column, i++);
		}		
	}
	
/*	public String getRecordValue(DataJoinRecord record, String column){
		if (columnHeaderIndex == null) {
			buildColumnHeaderIndex();
		}
		
		Integer index = columnHeaderIndex.get(column);
		if (index == null)
			return null;
		
		String value = record.getRecordLineValue();
		
		
		
		return null;
	}*/
}
