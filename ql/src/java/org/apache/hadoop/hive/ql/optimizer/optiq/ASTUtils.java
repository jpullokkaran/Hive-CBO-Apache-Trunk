package org.apache.hadoop.hive.ql.optimizer.optiq;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.apache.hadoop.hive.ql.parse.ASTErrorNode;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

public class ASTUtils {

	public static ASTNode createColRefAST(String tabAlias, String colName) {
		
		if ( tabAlias == null ) {
			return createTabOrColRefAST(colName);
		}
		
		ASTNode dot = (ASTNode) ASTUtils.adaptor.create(HiveParser.DOT, ".");
		ASTNode tabAst = createTabOrColRefAST(tabAlias);
		ASTNode colAst = (ASTNode) ASTUtils.adaptor.create(
				HiveParser.Identifier, colName);
		dot.addChild(tabAst);
		dot.addChild(colAst);
		return dot;
	}

	public static ASTNode createAliasAST(String colName) {
		return (ASTNode) ASTUtils.adaptor
				.create(HiveParser.Identifier, colName);
	}

	public static ASTNode createTabOrColRefAST(String tabAlias) {
		ASTNode tabAst = (ASTNode) ASTUtils.adaptor.create(
				HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
		ASTNode tabName = (ASTNode) ASTUtils.adaptor.create(
				HiveParser.Identifier, tabAlias);
		tabAst.addChild(tabName);
		return tabAst;
	}

	/**
	 * Copied from ParseDriver
	 */
	static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
		/**
		 * Creates an ASTNode for the given token. The ASTNode is a wrapper
		 * around antlr's CommonTree class that implements the Node interface.
		 * 
		 * @param payload
		 *            The token.
		 * @return Object (which is actually an ASTNode) for the token.
		 */
		@Override
		public Object create(Token payload) {
			return new ASTNode(payload);
		}

		@Override
		public Object dupNode(Object t) {

			return create(((CommonTree) t).token);
		};

		@Override
		public Object errorNode(TokenStream input, Token start, Token stop,
				RecognitionException e) {
			return new ASTErrorNode(input, start, stop, e);
		};
	};

}
