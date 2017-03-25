package cn.uway.summary.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SummaryUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(SummaryUtil.class);
	
	public static HashMap<String,String> readTxtFileToMap(String filePath){
		HashMap<String,String> map = new HashMap<String,String>();

        try {

                String encoding="GBK";

                File file=new File(filePath);

                if(file.isFile() && file.exists()){ //判断文件是否存在

                    InputStreamReader read = new InputStreamReader(

                    new FileInputStream(file),encoding);//考虑到编码格式

                    BufferedReader bufferedReader = new BufferedReader(read);

                    String lineTxt = null;
                    int index = 1;
                    while((lineTxt = bufferedReader.readLine()) != null){

                    	map.put(lineTxt, String.valueOf(index));
                    	index++;

                    }

                    read.close();

        }else{
        	LOGGER.error("读取缓存数据找不到指定的文件:"+filePath);
        }

        } catch (Exception e) {
        	LOGGER.debug("读取文件内容出错,file="+filePath,e);
        }
        return map;
    }
}
