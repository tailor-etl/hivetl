package org.apache.hadoop.hive.ql.parse.defined;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * @Title: HivePrivilegeHook.java
 * @Package org.apache.hadoop.hive.ql.parse.defined
 * @Description: Hive �����select\alter\dropȨ��
 * @author xianbing.liu
 * @date 2013-1-18 ����6:23:13
 * @version V1.0
 */
public class HivePrivilegeHook extends AbstractSemanticAnalyzerHook {

	private static final Log l4j = LogFactory.getLog(HivePrivilegeHook.class);

	private static final String PERMISSION_SELECT = "Select";
	private static final String PERMISSION_DROP = "Drop";
	private static final String PERMISSION_ALTER = "Alter";
	private static final String PERMISSION_CREATE = "Create";

	private static final String REGEXP = "TOK_TABNAME\\([a-zA-Z0-9|_]*\\)\\([a-zA-Z0-9|_]*\\)";

	private static final Pattern PATTERN = Pattern.compile(REGEXP);

	/**
	 * ִ�н���֮ǰ�����ز���
	 */
	@Override
	public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context,
			ASTNode ast) throws SemanticException {
		if (LoadPrivelegeFile.getIfValid()) {
			l4j.warn("text:" + ast.getToken().getText() + " type:"
					+ ast.getToken().getType() + " index:"
					+ ast.getToken().getTokenIndex());
			Set<String> dbSets = new HashSet<String>();
			String userName = "";
			if (SessionState.get() != null
					&& SessionState.get().getAuthenticator() != null) {
				userName = SessionState.get().getAuthenticator().getUserName();
			}
			String dbname = "";
			String sqlPlan = "";
			try {
				dbname = context.getHive().getCurrentDatabase();
				dbSets.add(dbname);
			} catch (HiveException e) {
				l4j.warn("can't get dbname error message:" + e.getMessage());
			}
			ASTNode tablePart = (ASTNode) ast.getChild(0);
			if (tablePart != null) {
				sqlPlan = tablePart.dump();
				if (ast.getToken().getType() != HiveParser.TOK_QUERY
						&& ast.getToken().getType() != HiveParser.TOK_COLTYPELIST
						&& ast.getToken().getType() != HiveParser.TOK_UNLOCKTABLE) {
					sqlPlan = dbname + "TOK_TABNAME" + sqlPlan;
				}
				// l4j.warn("typeOfint:"+ast.getToken().getType());
				// l4j.warn(tablePart.dump() + "ast.getToken().getType()"+
				// ast.getToken().getText());
				// l4j.warn(sqlPlan);
				Matcher matcher = PATTERN.matcher(sqlPlan);
				while (matcher.find()) {
					String name = matcher.group();
					name = name.substring(name.indexOf("(") + 1,
							name.indexOf(")"));
					dbSets.add(name);
				}
				// l4j.warn(dbSets);
				for (String db : dbSets)
					check(ast, userName, db, sqlPlan);
			}
		}
		return ast;
	}

	/**
	 * 
	 * @Title: check
	 * @Description: ����û��Ա�Ĳ���Ȩ��
	 * @param @param ast
	 * @param @param userName �û�����
	 * @param @param dbName hive����
	 * @param @param sqlPlan sqlִ�мƻ�
	 * @param @throws SemanticException �쳣��
	 * @return void ��������
	 * @throws
	 */
	private void check(ASTNode ast, String userName, String dbName,
			String sqlPlan) throws SemanticException {
		if (ast.getToken().getType() == HiveParser.TOK_COLTYPELIST) {
			if (!sqlPlan.toLowerCase().contains(
					(LoadPrivelegeFile.TOK_TABNAME + LoadPrivelegeFile
							.getTmpTableRule()).toLowerCase())) {
				checkUserPrivilege(dbName, PERMISSION_CREATE, userName);
			}
			return;
		}
		if (LoadPrivelegeFile.getDatabasetablemaps().get(dbName) != null) {
			List<String> list = getCheckTable(sqlPlan, dbName);
			for (String key : list) {
				l4j.warn(ast.getToken().getType() + "come into key..." + key);
				switch (ast.getToken().getType()) {
				case HiveParser.TOK_QUERY:
					checkUserPrivilege(key, PERMISSION_SELECT, userName);
					break;
				case HiveParser.TOK_UNLOCKTABLE:
					checkUserPrivilege(key, PERMISSION_DROP, userName);
					break;
				case HiveParser.TOK_ALTERTABLE_PARTITION:
				case HiveParser.TOK_ALTERTABLE_RENAME:
				case HiveParser.TOK_ALTERTABLE_TOUCH:
				case HiveParser.TOK_ALTERTABLE_ARCHIVE:
				case HiveParser.TOK_ALTERTABLE_UNARCHIVE:
				case HiveParser.TOK_ALTERTABLE_ADDCOLS:
				case HiveParser.TOK_ALTERTABLE_REPLACECOLS:
				case HiveParser.TOK_ALTERTABLE_RENAMECOL:
				case HiveParser.TOK_ALTERTABLE_ADDPARTS:
				case HiveParser.TOK_ALTERTABLE_DROPPARTS:
				case HiveParser.TOK_ALTERTABLE_PROPERTIES:
				case HiveParser.TOK_ALTERTABLE_CLUSTER_SORT:
					checkUserPrivilege(key, PERMISSION_ALTER, userName);
					break;
				default:
					break;
				}
			}

		}
	}

	/**
	 * 
	 * @Title: getCheckTable
	 * @Description: ��ȡ��Ҫ���ı�����)
	 * @param @param plan sqlִ�мƻ�
	 * @param @param dbName hive����
	 * @param @return ����
	 * @return List<String> ��������
	 * @throws
	 */
	@SuppressWarnings("unchecked")
	private List<String> getCheckTable(String plan, String dbName) {
		List<String> list = new ArrayList<String>();
		Set<String> sets = (Set<String>) LoadPrivelegeFile
				.getDatabasetablemaps().get(dbName);
		// l4j.warn(dbName+" setss.."+sets.toString());
		for (String s : sets) {
			if (plan.toUpperCase().indexOf(s.toUpperCase()) >= 0) {
				list.add(dbName + s);
			}
		}
		// l4j.warn("**********list**********"+list);
		return list;
	}

	/**
	 * @Title: checkUserPrivilege
	 * @Description: �ж��û��Ƿ񶼱���Ȩ��
	 * @param @param key ����
	 * @param @param type sql�������� (select\drop\alter)
	 * @param @param userName �û���
	 * @param @throws SemanticException
	 * @return void ��������
	 * @throws
	 */
	@SuppressWarnings("unchecked")
	private void checkUserPrivilege(String key, String type, String userName)
			throws SemanticException {
		Set<String> users = (Set<String>) LoadPrivelegeFile.getUsermaps().get(
				key + type);
		if (users != null) {
			if (users.contains(userName)
					|| users.contains(userName.toUpperCase())
					|| users.contains(userName.toLowerCase())) {
			} else {
				l4j.warn(userName + " " + type + " Permission denied");
				throw new SemanticException(type + " Permission denied .");
			}
		} else {
			l4j.warn(userName + " " + type + " Permission denied");
			throw new SemanticException(type + " Permission denied ");
		}
	}

}
