package cn.uway.summary.DataJoin;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataJoinParserTemplate {
	private static final Logger logger = LoggerFactory.getLogger(DataJoinParserTemplate.class); // 日志
	
	public static class JoinerTR {
		public static enum RELATE_JOIN {
			e_unknow,
			e_left,
			e_right,
			e_inner,
			e_full,
		};
		
		protected int id;
		protected RELATE_JOIN relateJoin;
		protected String primaryKeys;
		protected List<JoinerTD> tdList;
		
		public JoinerTR() {
			relateJoin = RELATE_JOIN.e_unknow;
		}
	}
	
	public static class JoinerTD {
		protected String sql;
		
		public JoinerTD() {
			
		}
	}
	
	protected List<JoinerTR> trLists;
	protected String orderbyKeys;
	public DataJoinParserTemplate() {
		trLists = new LinkedList<JoinerTR>();
	}
	
	public static DataJoinParserTemplate parse(String xmlTemplateFile) {
		FileInputStream fis = null;
		BufferedReader bf = null;
		try {
			DataJoinParserTemplate template = new DataJoinParserTemplate();
			
			fis = new FileInputStream(xmlTemplateFile);
			bf = new  BufferedReader(new InputStreamReader(fis));
			StringBuilder sb = new StringBuilder();
			while (true) {
				String line = bf.readLine();
				if (line == null)
					break;
	
				sb.append(line);
			}
			
			String xmlContent = sb.toString();
			ByteArrayInputStream bis = new ByteArrayInputStream(xmlContent.getBytes());
			InputStreamReader in = new InputStreamReader(bis, "utf-8");
			
			Document el = new SAXReader().read(in);
			Element root = el.getRootElement();
			template.orderbyKeys = root.attributeValue("order_by");
			//String name = root.getName();
			@SuppressWarnings("unchecked")
			List<Element> eltrs = root.elements("tr");
			for (Element eltr : eltrs) {
				JoinerTR tr = new JoinerTR();
				
				tr.id = Integer.parseInt(eltr.attributeValue("id"));
				tr.primaryKeys = eltr.attributeValue("primary_key");
				/*String primary_key = eltr.attributeValue("primary_key");
				String keys[] = primary_key.split(",");
				if (keys != null) {
					tr.primaryKeys = new ArrayList<String>(keys.length);
					for (String key : keys) {
						if (key == null)
							continue;
						
						key = key.trim();
						if (key.length() > 0)
							tr.primaryKeys.add(key);
					}
				}*/
				
				if ("true".equalsIgnoreCase(eltr.attributeValue("left_join")))
					tr.relateJoin = JoinerTR.RELATE_JOIN.e_left;
				else if ("true".equalsIgnoreCase(eltr.attributeValue("right_join")))
					tr.relateJoin = JoinerTR.RELATE_JOIN.e_right;
				else if ("true".equalsIgnoreCase(eltr.attributeValue("inner_join")))
					tr.relateJoin = JoinerTR.RELATE_JOIN.e_inner;
				else if ("true".equalsIgnoreCase(eltr.attributeValue("full_join")))
					tr.relateJoin = JoinerTR.RELATE_JOIN.e_full;
				else
					tr.relateJoin = JoinerTR.RELATE_JOIN.e_unknow;
				
				@SuppressWarnings("unchecked")
				List<Element> eltds = eltr.elements("td");
				tr.tdList = new ArrayList<JoinerTD>(eltds.size());
				for (Element eltd : eltds) {
					// 兼容nbi模板
					if (tr.relateJoin == JoinerTR.RELATE_JOIN.e_unknow) {
						if ("true".equalsIgnoreCase(eltd.attributeValue("left_join")))
							tr.relateJoin = JoinerTR.RELATE_JOIN.e_left;
						else if ("true".equalsIgnoreCase(eltd.attributeValue("right_join")))
							tr.relateJoin = JoinerTR.RELATE_JOIN.e_right;
						else if ("true".equalsIgnoreCase(eltd.attributeValue("inner_join")))
							tr.relateJoin = JoinerTR.RELATE_JOIN.e_inner;
						else if ("true".equalsIgnoreCase(eltd.attributeValue("full_join")))
							tr.relateJoin = JoinerTR.RELATE_JOIN.e_full;
					}
					
					
					JoinerTD td = new JoinerTD();
					String sql = eltd.getText();
					if (sql != null) {
						td.sql = sql.trim();
						int errCode = validSql(td.sql);
						if (0 != errCode) {
							String errMsg = getValidSqlError(errCode);
							logger.error(errMsg + " 模板文件名:{}",  xmlTemplateFile);
							return null;
						}
						
						tr.tdList.add(td);
					}
				}
				
				if (tr.relateJoin == JoinerTR.RELATE_JOIN.e_unknow) {
					logger.error("解析模板配置错误. 无法确定td之间的连关系, 模板文件名:{} tr_id:{}", xmlTemplateFile, tr.id);
					return null;
				}
				
				template.trLists.add(tr);
			}
			
			if (template.trLists.size()<1) {
				logger.error("tr的数量为0", xmlTemplateFile);
				return null;
			}
			
			return template;
		} catch (Exception e) {
			logger.error("解析模板解析错误. 模板文件名:{}", xmlTemplateFile, e);
		} finally {
			try {
				if (bf != null) {
					bf.close();
					bf = null;
				}
				
				if (fis != null) {
					fis.close();
					fis = null;
				}
			} catch (IOException e) {}
		}
		
		return null;
	}
	
	/**
	 * 简易地检测下sql的合法性
	 * @param sql
	 */
	public static int validSql(String sql) {
		if (sql != null) {
			sql = sql.trim().toLowerCase();
		}
			
		if (sql == null || sql.length()<1) {
			return -1;
		}
		
		if (sql.indexOf("select") < 0 || sql.indexOf("from") < 0)
			return -2;
		
		return 0;
	}
	
	public static String getValidSqlError(int errCode) {
		switch (errCode) {
			case -1:
				return "sql 语句为空";
			case -2:
				return "缺少\"select\", \"from\", 等关键字";
			default:
				break;
		}
		
		return "";
	}
}
