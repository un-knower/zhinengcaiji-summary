package cn.uway.summary.lte;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.parser.ParseOutRecord;
import cn.uway.framework.parser.file.CSVParser;
import cn.uway.framework.parser.file.templet.Field;
import cn.uway.util.FileUtil;
import cn.uway.util.StringUtil;
import cn.uway.util.TimeUtil;

/**
 * 号码|#|归属地|#|C_IMSI|#|G_IMSI|#|L_IMSI
18068920520|#|沭阳|#|460030135248818|#|204043931997868|#|
 * @author Admin
 *
 */
public class ImsiDataParser  extends CSVParser {
	private static Logger LOGGER = LoggerFactory.getLogger(ImsiDataParser.class);

	static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	static String currSplitSign="\\|#\\|";
	
	String city_en="";
	public ImsiDataParser(){
		
	}
	
	/**
	 * 字段定位
	 */
	public void setFieldLocalMap(String head){
		String tmpHead = switchLineWithSplitSign(currSplitSign, head, splitSign);
		String [] fieldNames = tmpHead.split(currSplitSign,5);;
		fieldLocalMap = new HashMap<String,Integer>();
		for(int n = 0; n < fieldNames.length; n++){
			String fieldName = fieldNames[n].toUpperCase().replace("\"", "");
			fieldLocalMap.put(fieldName, n);
		}
	}
	@Override
	public ParseOutRecord nextRecord() throws Exception{
		readLineNum++;
		ParseOutRecord record = new ParseOutRecord();
		String tmpLine = switchLineWithSplitSign(currSplitSign, currentLine, splitSign);
		String [] valList =tmpLine.split(currSplitSign,5);
		String phone="";
//		if(valList!=null && valList.length>0){
//			phone=valList[0];
//			if(StringUtil.isEmpty(phone) ||"null".equalsIgnoreCase(phone)){
//				return null;
//			}
//		}
		List<Field> fieldList = templet.getFieldList();
		//[18068920520, 沭阳, 460030135248818, 204043931997868, ]
		// Map<String, String> map = new HashMap<String, String>();
		Map<String,String> map = this.createExportPropertyMap(templet.getDataType());
		for(Field field : fieldList){
			if(field == null){
				continue;
			}
			// 定位，即找出模板中的字段在原始文件中的位置
			Integer indexInLine = fieldLocalMap.get(field.getName());
			// 找不到，设置为空
			if(indexInLine == null){
				if(map.get(field.getIndex()) != null)
					continue;
				//map.put(field.getIndex(), "");
				continue;
			}
			if(indexInLine >= valList.length)
				break;
			String value = valList[indexInLine];
			value = value.replace("\"", "");
			// 字段值处理
			if(!fieldValHandle(field, value, map)){
				invalideNum++;
				return null;
			}
			if("true".equals(field.getIsPassMS())){
				int i = value.indexOf(".");
				value = (i == -1 ? value : value.substring(0, i));
			}
			if(!"".equalsIgnoreCase(value))
			  map.put(field.getIndex(), null != value ? value.trim() : value);
		}

		// 公共回填字段
		map.put("MMEID", String.valueOf(task.getExtraInfo().getOmcId()));
		map.put("COLLECTTIME", TimeUtil.getDateString(new Date()));
		map.put("CITY_EN", city_en);
		
		
		handleTime(map);
		record.setType(templet.getDataType());
		record.setRecord(map);
		return record;
	}
	
	/**
	 * 获取当前文件对应的Templet
	 */
	public void getMyTemplet() {
		Iterator<String> it = templetMap.keySet().iterator();
		while (it.hasNext()) {
			String file = it.next();
			// 匹配通配符*
			String wildCard = "*";

			if (wildCardMatch(file, rawName, wildCard,currentDataTime)) {
				templet = templetMap.get(file);
				// on 20141022 如果不判断为空，后面的会替换前面的。 照成文件和 文件表达式不匹配
				if (templet != null)
					return;
				// end add
			}
		}
	}

	/**
	 * @param regex
	 *            正则表达式
	 * @param input
	 *            输入字符串
	 * @return
	 */
	public static boolean findValue(String regex, String input) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(input);
		if (matcher.find()) {
			String result = matcher.group();
			if (!(null == result || "".equals(result.trim()))) {
				return true;
			}
			//USENUMBER_IMSI_20160523_SQ.txt
			//USENUMBER_IMSI_%%Y%%M%%D_(.)*.txt
		}
		return false;
	}

	/**
	 * 通配符匹配
	 * 
	 * @param src
	 *            解析模板中的文件名或classname
	 * @param dest
	 *            厂家原始文件，如果是压缩包，则会带路径
	 * @param wildCard
	 *            通配符
	 * @return
	 */
	public static boolean wildCardMatch(String src, String dest, String wildCard,Date currentDataTime) {
		boolean flag = false;
		String tmp = src.replace("*", "(.)*");
		tmp=StringUtil.convertCollectPath(tmp, currentDataTime);
		if (dest.contains("/")) {
			int lastIndex = dest.lastIndexOf("/");
			String tmpFileNameOrClassName = dest.substring(lastIndex + 1);

			boolean result = findValue(tmp, tmpFileNameOrClassName);
			if (result)
				return true;

		} else {
			boolean result = findValue(tmp, dest);
			if (result)
				return true;
		}
		return flag;
	}

	/**
	 * 解析文件名
	 * 
	 * @throws Exception
	 */
	public void parseFileName() {
		try {
			String fileName = FileUtil.getFileName(this.rawName);
			String patternTime = StringUtil.getPattern(fileName, "\\d{8}");
			if (StringUtil.isNotEmpty(patternTime))
				this.currentDataTime = getDateTime(patternTime, "yyyyMMdd");
			
			 city_en =fileName.substring(fileName.lastIndexOf("_")+1);
		} catch (Exception e) {
			LOGGER.error("解析文件名异常", e);
		}
	}
	
	

	// 将时间转换成format格式的Date
	public final Date getDateTime(String date, String format) {
		if (date == null) {
			return null;
		}
		if (format == null) {
			format = "yyyy-MM-dd";
		}
		try {
			DateFormat df = new SimpleDateFormat(format);
			return df.parse(date);
		} catch (Exception e) {
			return null;
		}
	}
	public static void main(String[] args) {
		String input ="USENUMBER_IMSI_20160523_SQ.txt";
		String regex="USENUMBER_IMSI_20160523_(.)*.txt";
		
		String city_en =input.substring(input.lastIndexOf("_")+1).replace(".txt", "");
		System.out.println(city_en);
		
		//
		boolean result =findValue(regex, input);
		System.out.println(result);
		
		String  aa ="18068920520|#|沭阳|#|460030135248818|#|204043931997868|#|";
		 String currSplitSign="\\|#\\|";
		String sss[] =StringUtil.split(aa, currSplitSign);
		 sss =aa.split(currSplitSign,5);
		System.out.println(sss);
	}
}
