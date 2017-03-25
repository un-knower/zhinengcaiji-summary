package cn.uway.summary.DataJoin;

import java.util.HashMap;
import java.util.Map;

import cn.uway.framework.context.AppContext;

public class DataBlockManager {
	private static DataBlockManager dataBlockManagerHannder;
	/**
	 * 最小的缓存尺寸
	 */
	private static final int MIN_MEMORY_SIZE = 50;
	
	/**
	 * 存放在内存的blcok最大个数
	 */
	protected int maxMemoryBlockCount = 0;
	
	/**
	 *  最大的block_index
	 */
	private long blockCount;
	
	/**
	 * 正在使用的blocks列表
	 */
	protected Map<Long, DataBlock> blocks;
	
	private DataBlockManager() {
		this.blockCount = 0;
		this.blocks = new HashMap<Long, DataBlock>();
		int maxMemorySize = MIN_MEMORY_SIZE;
		
		String summaryCacheMaxSizeMB = AppContext.getBean("summaryCacheMaxSizeMB", java.lang.String.class);
		if (summaryCacheMaxSizeMB != null) {
			int nSummaryCacheMaxSizeMB = 0;
			try {
				nSummaryCacheMaxSizeMB = Integer.parseInt(summaryCacheMaxSizeMB);
			} catch (Exception e) {}
			
			if (nSummaryCacheMaxSizeMB > MIN_MEMORY_SIZE) {
				maxMemorySize = nSummaryCacheMaxSizeMB;
			}
		}
		this.maxMemoryBlockCount =  maxMemorySize / (DataBlock.BLOCK_DATA_SIZE/1024/1024);
	}
	
	public static synchronized DataBlockManager getInstance() {
		if (dataBlockManagerHannder == null) {
			dataBlockManagerHannder = new DataBlockManager();
		}
		
		return dataBlockManagerHannder;
	}
	
	public synchronized DataBlock allowBlockData() {
		DataBlock blockData = new DataBlock(this, blockCount);
		++blockCount;
		
		return blockData;
	}
	
	public synchronized void addBlockData(DataBlock blockData) {
		if (blocks.size()>= maxMemoryBlockCount) {
			blockData.serialSave();
			return;
		}
		
		this.blocks.put(blockCount, blockData);
	}
	
	public synchronized void removeBlockData(DataBlock blockData) {
		this.blocks.remove(blockData.blockIndex);
	}

}
