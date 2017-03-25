package cn.uway.summary.DataJoin;

import java.util.Comparator;


public class DataJoinRecord {
	public static class DataJoinRecordComparator implements Comparator<DataJoinRecord>{
		@Override
		public int compare(DataJoinRecord o1, DataJoinRecord o2){
			return o1.primaryKey.compareTo(o2.primaryKey);
		}
	}
	
	public String primaryKey;
	public String recordValue;
	public DataBlock  block;
	public int offset;
	public int len;
	
	public DataJoinRecord(String primaryKey, DataBlock block, int offset, int len) {
		this.primaryKey = primaryKey;
		this.block = block;
		this.offset = offset;
		this.len = len;
	}
	
	public String getRecordLineValue() {
		if (recordValue != null)
			return recordValue;
		
		if (!block.isInMemroy()) {
			block.serialLoad();
		}
		recordValue = block.getString(offset, len); 

		return recordValue;
	}
	
	/**
	 * 每条记录在使用完，需要调用一下destory方法，让它从内存块中释放，
	 * 否则会造成内存块一直被占用．新的记录，就只会保在硬盘上．
	 */
	public void destory() {
		if (block == null)
			return;
		
		block.popElement();
		if (block.getElementCount() < 1) {
			this.block.destory();
			this.block = null;
		}
	}
}
