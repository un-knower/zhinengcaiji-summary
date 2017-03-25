package cn.uway.summary.DataJoin;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;


public class DataBlock {
	protected DataBlockManager blockManager;
	protected boolean inMemroy;
	
	protected long blockIndex;
	protected byte[] buff;
	protected int buffOffset;
	protected int elementCount;
	
	
	/**
	 * 每个Block的数据尺寸
	 */
	protected static final int BLOCK_DATA_SIZE = 10*1024*1024;
	
	protected DataBlock(DataBlockManager blockManager, long blockIndex) {
		this.blockManager = blockManager;
		this.blockIndex = blockIndex;
		buff = new byte[BLOCK_DATA_SIZE];
		this.inMemroy = true;
	}
	
	public boolean canSave(int byteLen) {
		if (byteLen + buffOffset > BLOCK_DATA_SIZE)
			return false;
		
		return true;
	}
	
	public int put(byte[] b) {
		if (b == null)
			return -1;
		
		if (!canSave(b.length))
			return -1;
		
		int startPos = buffOffset;
		System.arraycopy(b, 0, buff, startPos, b.length);
		buffOffset += b.length;
		++elementCount;
		
		return startPos;
	}
	
	public int putString(String element) {
		return put(element.getBytes());
	}
	
	public byte[] get(int off, int len) {
		byte[] b = new byte[len];
		System.arraycopy(buff, off, b, 0, len);
		
		return b;
	}
	
	public String getString(int off, int len) {
		String value = new String(buff, off, len);

		return value;
	}
	
	public void popElement() {
		--elementCount;
	}
	
	public int getElementCount() {
		return elementCount;
	}
	
	protected String getSerialFileName() {
		String cacheDir = "./cacheFile/dataJoinner";
		File dir = new File(cacheDir);
		if (!dir.exists() || !dir.isDirectory()) {
			dir.mkdirs();
		}
		
		return cacheDir + "/" + blockIndex + ".bin";
	}
	
	public boolean serialLoad() {
		if (this.inMemroy)
			return true;
		
		String serialFile = getSerialFileName();
		try {
			FileInputStream fs = new FileInputStream(serialFile);
			buff = new byte[this.buffOffset];
			fs.read(buff, 0, buffOffset);
			fs.close();
			fs = null;
			this.inMemroy = true;
			
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	public boolean serialSave() {
		if (!this.inMemroy)
			return true;
		
		String serialFile = getSerialFileName();
		try {
			FileOutputStream fs = new FileOutputStream(serialFile);
			fs.write(buff, 0, buffOffset);
			fs.flush();
			fs.close();
			fs = null;
			this.buff = null;
			this.inMemroy = false;
			
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	public void destory() {
		blockManager.removeBlockData(this);
		String serialFile = getSerialFileName();
		File file = new File(serialFile);
		if (file.exists() && file.isFile()) {
			file.delete();
		}
	}
	
	public boolean isInMemroy() {
		return inMemroy;
	}

}
