
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MysqlToTibero {
	public static void main(String[] args) throws IOException, URISyntaxException {
		//mysqlTableToTibero();
         File dir = new File("C:\\Mapper");
         
         File[] files = dir.listFiles();
		for(File f : files) {
			mysqlQueryToTibero(f);
		}
	}
	
	public static void mysqlTableToTibero() throws IOException{
		File sql = new File("C:\\Users\\dydrb\\Documents\\mysqlDatadump.sql");
		File psm = new File("C:\\Users\\dydrb\\Documents\\tiberoPsm.sql");

		FileReader reader = new FileReader(sql);
		BufferedReader bufReader = new BufferedReader(reader);

		FileWriter writer = new FileWriter(new File("C:\\Users\\dydrb\\Documents\\tiberoSql.sql"));
		FileWriter psmWriter = new FileWriter(psm);
		BufferedWriter bufWriter = new BufferedWriter(writer);
		BufferedWriter bufPsmWriter = new BufferedWriter(psmWriter);
		
		String line = "";
		String comment = "";
		String commentReg = "COMMENT '.*'";
		String tbName = "WIEZON.";
		String tbComment = "";
		String index = "";
		String auto_increment = "";
		String trigger = "";
		String tbSpace = "TABLESPACE USR\n" + 
				"PCTFREE 10\n" + 
				"INITRANS 2\n" + 
				"STORAGE (\n" + 
				"	MAXEXTENTS UNLIMITED\n" + 
				")\n" + 
				"LOGGING\n" + 
				"NOPARALLEL;\n";

		while ((line = bufReader.readLine()) != null) {
			// System.out.println(line);
			if (line.indexOf("CREATE TABLE ") >= 0) {
				Pattern pattern = Pattern.compile("CREATE TABLE .*");
				Matcher matcher = pattern.matcher(line);
				while (matcher.find()) {
					tbName += line.substring(matcher.start() + 14, matcher.end() - 3);
				}
				line = line.replace("CREATE TABLE ", "CREATE TABLE WIEZON.");
			}
			if (line.indexOf("varchar") >= 0) {
				line = line.replace("varchar", "VARCHAR2");
			}
			if (line.indexOf("bigint") >= 0) {
				line = line.replace("bigint", "NUMBER");
			}
			if (line.indexOf("smallint") >= 0) {
				line = line.replace("smallint", "NUMBER");
			}
			if (line.indexOf("int") >= 0) {
				line = line.replace("int", "NUMBER");
			}
			if (line.indexOf("decimal") >= 0) {
				line = line.replace("decimal", "NUMBER");
			}
			if (line.indexOf("double") >= 0) {
				line = line.replace("double", "NUMBER");
			}
			if (line.indexOf("float") >= 0) {
				line = line.replace("float", "NUMBER");
			}
			if (line.indexOf("mediumtext") >= 0) {
				line = line.replace("mediumtext", "CLOB");
			}
			if(line.indexOf("text") >= 0 ) {
				line = line.replace("text", "CLOB");
			}
			if (line.indexOf("COMMENT ") >= 0) {
				Pattern pattern = Pattern.compile(commentReg);
				Matcher matcher = pattern.matcher(line);
				String colName = "";
				Pattern pattern2 = Pattern.compile("`.*`");
				Matcher matcher2 = pattern2.matcher(line);
				if(matcher2.find()) {
					colName = line.substring(matcher2.start()+1,matcher2.end()-1);
				}
				while (matcher.find()) {
					comment +="COMMENT ON COLUMN "+tbName+"."+colName+" IS "+line.substring(matcher.start()+8, matcher.end()) + ";\n";
				}
				
				line = line.replaceAll(commentReg, "");
			}
			if (line.indexOf("COMMENT=") >= 0) {
				Pattern pattern = Pattern.compile("COMMENT='.*'");
				Matcher matcher = pattern.matcher(line);
				while (matcher.find()) {
					tbComment += "COMMENT ON TABLE "+tbName+" IS "+line.substring(matcher.start() + 8, matcher.end())+";\n";
				}
			}
			if (line.indexOf("COLLATE") >= 0) {
				line = line.replaceAll("COLLATE '.*'", "");
			}
			if (line.indexOf("NOT NULL") >= 0) {
				line = line.replace("NOT NULL ", "");
				line = line.substring(0, line.length() - 1) + "NOT NULL,";
			}
			if (line.indexOf("ON UPDATE CURRENT_TIMESTAMP") >= 0) {
				line = line.replace("ON UPDATE CURRENT_TIMESTAMP", "");
			}
			if (line.indexOf("PRIMARY KEY") >= 0) {
				// create table 문 마지막 도달 ',' 제거
				if (line.lastIndexOf(",") == line.length() - 1) {
					line = line.substring(0, line.length() - 1);
				}
			} else if (line.indexOf("KEY") >= 0) {
				Pattern pattern = Pattern.compile("KEY .*\\(");
				Matcher matcher = pattern.matcher(line);
				String indexName = "";
				while (matcher.find()) {
					line = line.replace("`", "");
					indexName = line.substring(matcher.start() + 4, matcher.end() - 3);
				}

				Pattern pattern2 = Pattern.compile("\\(.*\\)");
				matcher = pattern2.matcher(line);
				String colName = "";
				while (matcher.find()) {
					line = line.replace("`", "");
					colName = line.substring(matcher.start(), matcher.end());
				}

				if (!indexName.equals("") && !colName.equals("")) {
					index += "CREATE INDEX " + indexName + " ON " + tbName + " " + colName + "\n" + "TABLESPACE USR\n"
							+ "PCTFREE 10\n" + "INITRANS 2\n" + "STORAGE (\n" + "	MAXEXTENTS UNLIMITED\n" + ")\n"
							+ "LOGGING;\n";
					continue;
				}
			}
			if (line.indexOf(") ENGINE=InnoDB") >= 0) {
				trigger = "CREATE OR REPLACE TRIGGER " + tbName + "_TIMESTAMP\n" + "BEFORE INSERT OR UPDATE ON " + tbName
						+ "\n" + "FOR EACH ROW\n" + "BEGIN\n" + "    :new.UPD_DNT := SYSDATE;\n" + "END;\n";
				line = ");";
				bufWriter.write(line);
				bufWriter.newLine();
				bufWriter.write(tbComment);
				bufWriter.write(comment);
				bufPsmWriter.write(trigger);

				bufWriter.write(index);
				bufWriter.write(auto_increment);
				
				
				 line = "";
				 comment = "";
				 commentReg = "COMMENT '.*'";
				 tbName = "WIEZON.";
				 tbComment = "";
				 index = "";
				 auto_increment = "";
				 tbSpace = "TABLESPACE USR\n" + 
						"PCTFREE 10\n" + 
						"INITRANS 2\n" + 
						"STORAGE (\n" + 
						"	MAXEXTENTS UNLIMITED\n" + 
						")\n" + 
						"LOGGING\n" + 
						"NOPARALLEL;\n";
				
			}
			if(line.indexOf("AUTO_INCREMENT") >= 0) {
				line = line.replace("AUTO_INCREMENT", "");
				auto_increment += "CREATE SEQUENCE "+tbName+"_SEQ START WITH 1 INCREMENT BY 1 CACHE 20;\n";
			}
			line = line.replace("`", "");
			//System.out.println(line);
			bufWriter.write(line);
			bufWriter.newLine();
		}
		

		//System.out.println(tbComment);
		//System.out.println(comment);
//		bufWriter.write(tbSpace);
//		bufWriter.write(tbComment);
//		bufWriter.write(comment);
//		bufPsmWriter.write(trigger);
//		bufWriter.write(index);
//		bufWriter.write(auto_increment);

		bufReader.close();
		bufPsmWriter.close();
		bufWriter.close();
		
		System.out.println("success");
	}
	
	public static void mysqlQueryToTibero(File f) throws IOException {
		File xml = f;
		File target = new File("C:\\Mapper_out\\"+f.getName());
		
		FileReader reader = new FileReader(xml);
		BufferedReader bufReader = new BufferedReader(reader);

		FileWriter writer = new FileWriter(target);
		BufferedWriter bufWriter = new BufferedWriter(writer);
		
		String line = "";
		String preFix = "SELECT * \nFROM (";
		String posFix = ") WHERE 1=1 \n"
				+ "<if test=\"stRow != null and stRow >= 0 and iRows != null and iRows > 0\">AND ROW BETWEEN #{stRow} AND #{iRows}</if>";
		String out = "";
		boolean isLimit = false;
		while((line = bufReader.readLine()) != null) {
			
			if(line.indexOf("<?xml") >= 0 || line.indexOf("<!DOCTYPE") >= 0 || line.indexOf("<mapper") >= 0 || line.indexOf("<update") >= 0 || line.indexOf("<delete") >= 0 ||  line.indexOf("<insert") >= 0  || line.indexOf("<select") >= 0) {
				//System.out.println(line);
				bufWriter.write(line);
				bufWriter.newLine();
				continue;
			}
			if( line.indexOf("</update") >= 0 ||  line.indexOf("</delete") >= 0 || line.indexOf("</insert") >= 0 ||  line.indexOf("</select") >= 0) {
				//System.out.println(line);
				bufWriter.write(out);
				out = "";
				bufWriter.write(line);
				bufWriter.newLine();
				continue;
			}
			line = concat(line);
			line = ifNull(line);
			line = date(line);
			line = sysdate(line);
			line = strTodate(line);
			line = rowNumTb(line);
			line = asRow(line);
			line = rowNum(line);
			line = curDate(line);
			line = now(line);
			line = date_format(line);
			line = time_format(line);
			line = format(line);
			line = limit(line);
			line = dateDiff(line);
			line = date_add(line);
			line = date_sub(line);
			
			//isLimit = limit(line);
			
			
			//line = isLimit? "" : line; 
			out += line+"\n";
			if(isLimit) {
				out = preFix + out + posFix;
			}
			//Pattern pattern = Pattern.compile("(?i)concat\\((.*,*)*\\)");
			
			//bufWriter.write(line);
			//bufWriter.newLine();
		}
		
		//bufWriter.write(out);
		bufReader.close();
		bufWriter.close();
		
		System.out.println("SUCCESS");
		
	}
	
	public static String doubleToSingleQuote(String line) {
		line = line.replace("\"", "\'");
		return line;
	}
	
//	public static boolean limit(String line) {
//		Pattern pattern = Pattern.compile("(?i)limit");
//		Matcher matcher = pattern.matcher(line);
//		
//		if(matcher.find()) {
//			return true;
//		}
//		
//		return false;
//	}
	
	public static String format(String line) {
		Pattern pattern = Pattern.compile("(?i)format");
		Matcher matcher = pattern.matcher(line);
		String newLine = line;
		while(matcher.find()) {
			//System.out.println(bracelet(line,matcher.start()));
			String format = bracelet(line,matcher.start());
			//System.out.println(concat);
			//String replaceStr = commaReplace(add);
			String replaceStr = formatReplace(format);
			newLine = newLine.replace(format, replaceStr);
//			newLine = newLine.replace(sub, replaceStr);
//			System.out.println(concat);
//			System.out.println(replaceStr);
			
		}
		//System.out.println(newLine);
//		while(matcher.find()) {
//			
//			System.out.println(matcher.group());
//		}
		
		return newLine;
	}
	
	public static String limit(String line) {
		Pattern pattern = Pattern.compile("(?i)>\\s*limit");
		Pattern stRowPattern = Pattern.compile("(?i)\\s*#\\s*\\{\\s*stRow\\s*\\}");
		Pattern iRowPattern = Pattern.compile("(?i)\\s*#\\s*\\{\\s*iRows\\s*\\}");
		Matcher matcher = pattern.matcher(line);
		
		while(matcher.find()) {
			//System.out.println(matcher.group());
					
			Matcher stRowMatcher = stRowPattern.matcher(line);
			Matcher iRowMatcher = iRowPattern.matcher(line);
			
			line = line.replace(matcher.group(), "> AND ROWNUM BETWEEN");
			line = line.replaceAll("\\s*,\\s*", " AND ");
			String stRow = "";
			while(stRowMatcher.find()) {
				stRow = stRowMatcher.group();
				line = line.replace(stRow,stRow + " + 1");
			}
			
			while(iRowMatcher.find()) {
				line = line.replace(iRowMatcher.group(),iRowMatcher.group() +" + "+ stRow);
			}
			
		}
		
		return line;
	}
	
	public static String rowNum(String line) {
		Pattern pattern = Pattern.compile("(?i)@rownum\\s*:=\\s*(?i)@rownum\\s*\\+\\s*1");
		Matcher matcher = pattern.matcher(line);
		
		while(matcher.find()) {
			//System.out.println(matcher.group());
			line = line.replace(matcher.group(), "ROWNUM");
			
		}
		
		return line;
	}
	
	public static String asRow(String line) {
		Pattern pattern = Pattern.compile("(?i)as\\s*row\\s*,");
		Matcher matcher = pattern.matcher(line);
		
		while(matcher.find()) {
			//System.out.println(matcher.group());
			line = line.replace(matcher.group(), "AS \"ROW\" ,");
			
		}
		
		return line;
	}
	
	public static String rowNumTb(String line) {
		Pattern pattern = Pattern.compile("(?i),*\\s\\(\\s*select\\s*@rownum\\s*:=.*\\)\\s*.");
		Matcher matcher = pattern.matcher(line); 
		
		while(matcher.find()) {
			line = line.replace(matcher.group(), "");
		}
		
		return line;
	}
	
	
	public static String curDate(String line) {
		Pattern pattern = Pattern.compile("(?i)curdate\\(\\)");
		Matcher matcher = pattern.matcher(line);
		
		while(matcher.find()) {
			line = line.replace(matcher.group(), "SYSDATE");
		}
		
		return line;
	}
	public static String now(String line) {
		Pattern pattern = Pattern.compile("(?i)now\\(\\)");
		Matcher matcher = pattern.matcher(line);
		
		while(matcher.find()) {
			line = line.replace(matcher.group(), "SYSDATE");
		}
		
		return line;
	}
	
	public static String date_sub(String line) {
		Pattern pattern = Pattern.compile("(?i)date_sub");
		Matcher matcher = pattern.matcher(line);
		String newLine = line;
		while(matcher.find()) {
			//System.out.println(bracelet(line,matcher.start()));
			String sub = bracelet(line,matcher.start());
			//System.out.println(concat);
			//String replaceStr = commaReplace(add);
			String replaceStr = addReplace(sub);
			
			newLine = newLine.replace(sub, replaceStr);
			//System.out.println(concat);
			//System.out.println(replaceStr);
			
		}
		//System.out.println(newLine);
//		while(matcher.find()) {
//			
//			System.out.println(matcher.group());
//		}
		
		return newLine;
	}
	
	public static String date_add(String line) {
		Pattern pattern = Pattern.compile("(?i)date_add");
		Matcher matcher = pattern.matcher(line);
		String newLine = line;
		while(matcher.find()) {
			//System.out.println(bracelet(line,matcher.start()));
			String add = bracelet(line,matcher.start());
			//System.out.println(concat);
			//String replaceStr = commaReplace(add);
			String replaceStr = addReplace(add);
			
			newLine = newLine.replace(add, replaceStr);
			//System.out.println(concat);
			//System.out.println(replaceStr);
			
		}
		//System.out.println(newLine);
//		while(matcher.find()) {
//			
//			System.out.println(matcher.group());
//		}
		
		return newLine;
	}
	
//	public static String dayOfWeek(String line) {
//		Pattern pattern = Pattern.compile("(?i)dayOfWeek");
//		Matcher matcher = pattern.matcher(line);
//		String newLine = line;
//		while(matcher.find()) {
//			//System.out.println(bracelet(line,matcher.start()));
//			String add = bracelet(line,matcher.start());
//			//System.out.println(concat);
//			//String replaceStr = commaReplace(add);
//			String replaceStr = addReplace(add);
//			
//			newLine = newLine.replace(add, replaceStr);
//			//System.out.println(concat);
//			//System.out.println(replaceStr);
//			
//		}
//		//System.out.println(newLine);
////		while(matcher.find()) {
////			
////			System.out.println(matcher.group());
////		}
//		
//		return newLine;
//	}
	
	public static String dateDiff(String line) {
		Pattern pattern = Pattern.compile("(?i)datediff");
		Matcher matcher = pattern.matcher(line);
		String newLine = line;
		while(matcher.find()) {
			//System.out.println(bracelet(line,matcher.start()));
			String dateDiff = bracelet(line,matcher.start());
			//System.out.println(concat);
			//String replaceStr = commaReplace(dateDiff);
			String replaceStr = trunc(dateDiff);
			
			newLine = newLine.replace(dateDiff, replaceStr);
			//System.out.println(concat);
			//System.out.println(replaceStr);
			
		}
		//System.out.println(newLine);
//		while(matcher.find()) {
//			
//			System.out.println(matcher.group());
//		}
		
		return newLine;
	}
	
	public static String sysdate(String line) {
		Pattern pattern = Pattern.compile("(?i)sysdate\\(\\)");
		Matcher matcher = pattern.matcher(line);
		
		while(matcher.find()) {
			line = line.replace(matcher.group(), "SYSDATE");
		}
		
		return line;
	}
	
	public static String time_format(String line) {
		Pattern pattern = Pattern.compile("(?i)time_format");
		Matcher matcher = pattern.matcher(line);
		
		while(matcher.find()) {
			line = line.replace(matcher.group(), "TO_DATE");
		}
		
		return line;
	}
	
	public static String date_format(String line) {
		Pattern pattern = Pattern.compile("(?i)date_format");
		Matcher matcher = pattern.matcher(line);
		
		while(matcher.find()) {
			line = line.replace(matcher.group(), "TO_DATE");
		}
		
		return line;
	}
	
	public static String strTodate(String line) {
		Pattern pattern = Pattern.compile("(?i)str_to_date");
		Matcher matcher = pattern.matcher(line);
		
		while(matcher.find()) {
			line = line.replace(matcher.group(), "TO_DATE");
		}
		
		return line;
	}
	
	public static String concat(String line) {
		Pattern pattern = Pattern.compile("(?i)concat");
		Matcher matcher = pattern.matcher(line);
		String newLine = line;
		while(matcher.find()) {
			//System.out.println(bracelet(line,matcher.start()));
			String concat = bracelet(line,matcher.start());
			//System.out.println(concat);
			String replaceStr = commaReplace(concat);
			
			newLine = newLine.replace(concat, replaceStr);
			//System.out.println(concat);
			//System.out.println(replaceStr);
			newLine = doubleToSingleQuote(newLine);
		}
		//System.out.println(newLine);
//		while(matcher.find()) {
//			
//			System.out.println(matcher.group());
//		}
		
		
		
		return newLine;
	}
	
	public static String ifNull(String line) {
		Pattern pattern = Pattern.compile("(?i)IFNULL");
		Matcher matcher = pattern.matcher(line);
		
		while(matcher.find()) {
			line = line.replace(matcher.group(), "NVL");
		}
		
		return line;
	}
	
	public static String date(String line) {
		if(line.indexOf("%Y") >= 0) {
			line = line.replace("%Y", "YYYY");
		}
		if(line.indexOf("%m") >= 0) {
			line = line.replace("%m", "MM");
		}
		if(line.indexOf("%d") >= 0) {
			line = line.replace("%d", "DD");
		}
		if(line.indexOf("%H") >= 0) {
			line = line.replace("%H", "HH24");
		}
		if(line.indexOf("%i") >= 0) {
			line = line.replace("%i", "MI");
		}
		if(line.indexOf("%s") >= 0) {
			line = line.replace("%s", "SS");
		}
		return line;
	}
	
	public static String bracelet(String line, int start) {
		line = line.substring(start);
		int cnt = 0;
		boolean flg = false;
		for(int i = 0; i < line.length(); i++) {
			if(line.charAt(i) == '(') {
				flg = true;
				cnt++;
			}else if(line.charAt(i) == ')') {
				cnt--;
			}
			if(cnt == 0) {
				if(!flg) continue;
				line = line.substring(0,i+1);
				//System.out.println(line);
				break;
			}
		}
		
		return line;
	}
	
	public static String commaReplace(String concat) {
		for(int i = 0; i < concat.length(); i++) {
			if(concat.charAt(i) == '(') {
				concat = concat.substring(i+1,concat.length()-1);
				break;
			}
		}
		int j = 0;
		int bcnt = 0;
		while(j<concat.length()) {
			if(concat.charAt(j)=='(') {
				bcnt++;
			}
			else if(concat.charAt(j) == ')') {
				bcnt--;
			}
			if(concat.charAt(j) == ',') {
				if(bcnt == 0) {
					concat = concat.substring(0,j) + "||" + concat.substring(j+1);
				}
			}
			j++;
			
		}
		
		//System.out.println(concat);
		
		return concat;
	}
	
	public static String formatReplace(String format) {
		for(int i = 0; i < format.length(); i++) {
			if(format.charAt(i) == '(') {
				format = format.substring(i+1,format.length()-1);
				break;
			}
		}
		int j = 0;
		int bcnt = 0;
		String num = "999,999,999,999";
		while(j<format.length()) {
			if(format.charAt(j)=='(') {
				bcnt++;
			}
			else if(format.charAt(j) == ')') {
				bcnt--;
			}
			if(format.charAt(j) == ',') {
				int i = 0;
				if(bcnt == 0) {
					i = Integer.parseInt(format.substring(j+1).trim());
					//System.out.println(i);
					if(i != 0){
						num += ".";
						for(int k = 0; k < i; k++) {
							num +="9";
						}
					}
					format = format.substring(0,j);
				}
			}
			j++;
			
		}
		
		//System.out.println(format+", '"+num+"'");
		
		return "TO_CHAR("+format+", '"+num+"')";
	}
	
	public static String addReplace(String add) {
		for(int i = 0; i < add.length(); i++) {
			if(add.charAt(i) == '(') {
				add = add.substring(i+1,add.length()-1);
				break;
			}
		}
		int j = 0;
		int bcnt = 0;
		while(j<add.length()) {
			if(add.charAt(j)=='(') {
				bcnt++;
			}
			else if(add.charAt(j) == ')') {
				bcnt--;
			}
			if(add.charAt(j) == ',') {
				if(bcnt == 0) {
					add = add.substring(0,j) + " + " + add.substring(j+1).replaceAll("(?i)\\s*interval\\s*","").replaceAll("(?i)\\s*day\\s*","");
				}
			}
			j++;
			
		}
		
		
		return add;
	}
	
	public static String subReplace(String sub) {
		for(int i = 0; i < sub.length(); i++) {
			if(sub.charAt(i) == '(') {
				sub = sub.substring(i+1,sub.length()-1);
				break;
			}
		}
		int j = 0;
		int bcnt = 0;
		while(j<sub.length()) {
			if(sub.charAt(j)=='(') {
				bcnt++;
			}
			else if(sub.charAt(j) == ')') {
				bcnt--;
			}
			if(sub.charAt(j) == ',') {
				if(bcnt == 0) {
					sub = sub.substring(0,j) + " - " + sub.substring(j+1).replaceAll("(?i)\\s*interval\\s*","").replaceAll("(?i)\\s*day\\s*","");
				}
			}
			j++;
			
		}
		
		
		return sub;
	}
	
	public static String trunc(String datediff) {
		for(int i = 0; i < datediff.length(); i++) {
			if(datediff.charAt(i) == '(') {
				datediff = datediff.substring(i+1,datediff.length()-1);
				break;
			}
		}
		int j = 0;
		int bcnt = 0;
		while(j<datediff.length()) {
			if(datediff.charAt(j)=='(') {
				bcnt++;
			}
			else if(datediff.charAt(j) == ')') {
				bcnt--;
			}
			if(datediff.charAt(j) == ',') {
				if(bcnt == 0) {
					datediff = "TRUNC(TO_DATE("+datediff.substring(0,j) + ") - " + datediff.substring(j+1)+")";
				}
			}
			j++;
			
		}
		
		
		return datediff;
	}
	

}
