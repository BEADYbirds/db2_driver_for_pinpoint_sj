/*
 * Copyright 2017 Steve McDuff
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.issw.jdbc.wrappers;


import com.ibm.db2.jcc.*;
import com.ibm.issw.jdbc.profiler.JdbcEvent;
import com.ibm.issw.jdbc.profiler.JdbcProfiler;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * 
 * WrappedPreparedStatement
 */

public class WrappedDB2PreparedStatement extends WrappedDB2Statement implements DB2PreparedStatement {

	private static final Logger LOG = Logger
			.getLogger(WrappedDB2PreparedStatement.class.getName());
	private String sqlStatement;
	private final PreparedStatement pstmt;
	private boolean supportsMultiRowInsert;


	/**
	 * ctor
	 * @param preparedStatement the prepared statement to wrap
	 * @param sql the sql
	 * @param ref the execution reference
	 * @param transaction the transaction string
	 * @param connection the connection
	 */
	public WrappedDB2PreparedStatement(PreparedStatement preparedStatement,
			String sql, String ref, String transaction, Connection connection) {
		super(preparedStatement, ref, transaction, connection);
		this.pstmt = preparedStatement;
		this.sqlStatement = sql;
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#executeQuery()
	 */
	@Override
    public final ResultSet executeQuery() throws SQLException {
		if (LOG.isLoggable(Level.FINE)) {
			LOG.fine("Executing query: " + this.sqlStatement);
		}

		ResultSet rslt = null;

		if ((this instanceof CallableStatement)){
			JdbcProfiler.getInstance().setStatementType(JdbcProfiler.CALLABLE, this.ref);}
		else {
			JdbcProfiler.getInstance().setStatementType(JdbcProfiler.PREPARED, this.ref);
		}
		profileSqlStatement(this.sqlStatement);
		JdbcProfiler.getInstance().start(JdbcProfiler.OP_EXECUTE_QUERY, this.ref);
		JdbcEvent jdbcEvent = JdbcProfiler.getInstance().getJdbcEvent(ref);

		ResultSet resultSet = this.pstmt.executeQuery();
		String currentRef = this.ref;
		rslt = wrapResultSet(jdbcEvent, resultSet, currentRef);
		JdbcProfiler.getInstance().stop(JdbcProfiler.OP_EXECUTE_QUERY, this.ref);
		JdbcProfiler.getInstance().addStack(this.ref);

		allocateNewRef();

		return rslt;
	}

	/**
	 * 
	 * allocateNewRef
	 */
	public void allocateNewRef() {
		this.ref = WrappedConnection.getNextRefCount();
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#executeUpdate()
	 */
	@Override
    public final int executeUpdate() throws SQLException {
		if (LOG.isLoggable(Level.FINE)) {
			LOG.fine("Executing update: " + this.sqlStatement);
		}

		int rows = 0;
		if ((this instanceof CallableStatement)) {
			JdbcProfiler.getInstance().setStatementType(JdbcProfiler.CALLABLE, this.ref);
		} else {
			JdbcProfiler.getInstance().setStatementType(JdbcProfiler.PREPARED, this.ref);
		}
		profileSqlStatement(this.sqlStatement);
		JdbcProfiler.getInstance().start(JdbcProfiler.OP_EXECUTE_UPDATE, this.ref);
		
        boolean success = false;
        try
        {
            rows = this.pstmt.executeUpdate();
            success = true;
        }
        finally
        {
            JdbcProfiler.getInstance().stop(JdbcProfiler.OP_EXECUTE_UPDATE, this.ref);
            JdbcProfiler.getInstance().addStack(this.ref);
            JdbcProfiler.getInstance().addRowsUpdated(rows, this.ref, success);
        }

		allocateNewRef();

		return rows;
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setNull(int, int)
	 */
	@Override
    public final void setNull(int parameterIndex, int sqlType)
			throws SQLException {
		setData(parameterIndex, "null");
		this.pstmt.setNull(parameterIndex, sqlType);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setBoolean(int, boolean)
	 */
	@Override
    public final void setBoolean(int parameterIndex, boolean x)
			throws SQLException {
		setData(parameterIndex, new Boolean(x));
		this.pstmt.setBoolean(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setByte(int, byte)
	 */
	@Override
    public final void setByte(int parameterIndex, byte x) throws SQLException {
		setData(parameterIndex, new Byte(x));
		this.pstmt.setByte(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setShort(int, short)
	 */
	@Override
    public final void setShort(int parameterIndex, short x) throws SQLException {
		setData(parameterIndex, new Short(x));
		this.pstmt.setShort(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setInt(int, int)
	 */
	@Override
    public final void setInt(int parameterIndex, int x) throws SQLException {
		setData(parameterIndex, new Integer(x));
		this.pstmt.setInt(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setLong(int, long)
	 */
	@Override
    public final void setLong(int parameterIndex, long x) throws SQLException {
		setData(parameterIndex, new Long(x));
		this.pstmt.setLong(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setFloat(int, float)
	 */
	@Override
    public final void setFloat(int parameterIndex, float x) throws SQLException {
		setData(parameterIndex, new Float(x));
		this.pstmt.setFloat(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setDouble(int, double)
	 */
	@Override
    public final void setDouble(int parameterIndex, double x)
			throws SQLException {
		setData(parameterIndex, new Double(x));
		this.pstmt.setDouble(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
	 */
	@Override
    public final void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		setData(parameterIndex, x);
		this.pstmt.setBigDecimal(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setString(int, java.lang.String)
	 */
	@Override
    public final void setString(int parameterIndex, String x)
			throws SQLException {
		setData(parameterIndex, "'" + x + "'");
		this.pstmt.setString(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setBytes(int, byte[])
	 */
	@Override
    public final void setBytes(int parameterIndex, byte[] x)
			throws SQLException {
		this.pstmt.setBytes(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
	 */
	@Override
    public final void setDate(int parameterIndex, Date x) throws SQLException {
		setData(parameterIndex, x);
		this.pstmt.setDate(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
	 */
	@Override
    public final void setTime(int parameterIndex, Time x) throws SQLException {
		setData(parameterIndex, x);
		this.pstmt.setTime(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
	 */
	@Override
    public final void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		setData(parameterIndex, x);
		this.pstmt.setTimestamp(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream, int)
	 */
	@Override
    public final void setAsciiStream(int parameterIndex, InputStream x,
			int length) throws SQLException {
		this.pstmt.setAsciiStream(parameterIndex, x, length);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setUnicodeStream(int, java.io.InputStream, int)
	 */
	@Override
    @Deprecated
	public final void setUnicodeStream(int parameterIndex, InputStream x,
			int length) throws SQLException {
		//$ANALYSIS-IGNORE
		this.pstmt.setUnicodeStream(parameterIndex, x, length);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, int)
	 */
	@Override
    public final void setBinaryStream(int parameterIndex, InputStream x,
			int length) throws SQLException {
		setData(parameterIndex, x);
		this.pstmt.setBinaryStream(parameterIndex, x, length);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#clearParameters()
	 */
	@Override
    public final void clearParameters() throws SQLException {
		this.pstmt.clearParameters();
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int, int)
	 */
	@Override
    public final void setObject(int parameterIndex, Object x,
			int targetSqlType, int scale) throws SQLException {
		if ((x instanceof String)){
			setData(parameterIndex, "'" + (String) x + "'");}
		else {
			setData(parameterIndex, x);
		}
		this.pstmt.setObject(parameterIndex, x, targetSqlType, scale);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
	 */
	@Override
    public final void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		if ((x instanceof String)){
			setData(parameterIndex, "'" + (String) x + "'");}
		else {
			setData(parameterIndex, x);
		}
		this.pstmt.setObject(parameterIndex, x, targetSqlType);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
	 */
	@Override
    public final void setObject(int parameterIndex, Object x)
			throws SQLException {
		if ((x instanceof String)){
			setData(parameterIndex, "'" + (String) x + "'");}
		else {
			setData(parameterIndex, x);
		}
		this.pstmt.setObject(parameterIndex, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#execute()
	 */
	@Override
    public final boolean execute() throws SQLException {
		if ((this instanceof CallableStatement)){
			JdbcProfiler.getInstance().setStatementType(JdbcProfiler.CALLABLE, this.ref);}
		else {
			JdbcProfiler.getInstance().setStatementType(JdbcProfiler.PREPARED, this.ref);
		}

        profileSqlStatement(this.sqlStatement);
        JdbcProfiler.getInstance().start(JdbcProfiler.OP_EXECUTE_QUERY, this.ref);
        boolean bool = false;
        boolean success = false;
        try
        {
            bool = this.pstmt.execute();
            success = true;
        }
        finally
        {
            JdbcProfiler.getInstance().stop(JdbcProfiler.OP_EXECUTE_QUERY, this.ref);
            JdbcProfiler.getInstance().addRowsUpdated(1, this.ref, success);
        }

		allocateNewRef();

		return bool;
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#addBatch()
	 */
	@Override
    public final void addBatch() throws SQLException {
		getBatchList().add(sqlStatement);
		this.pstmt.addBatch();
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader, int)
	 */
	@Override
    public final void setCharacterStream(int parameterIndex, Reader reader,
			int length) throws SQLException {
		this.pstmt.setCharacterStream(parameterIndex, reader, length);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
	 */
	@Override
    public final void setRef(int i, Ref x) throws SQLException {
		this.pstmt.setRef(i, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
	 */
	@Override
    public final void setBlob(int i, Blob x) throws SQLException {
		setData(i, x);
		this.pstmt.setBlob(i, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
	 */
	@Override
    public final void setClob(int i, Clob x) throws SQLException {
		setData(i, x);
		this.pstmt.setClob(i, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
	 */
	@Override
    public final void setArray(int i, Array x) throws SQLException {
		setData(i, x);
		this.pstmt.setArray(i, x);
	}

	
	private void setData(int i, Object x) throws SQLException {
		JdbcProfiler.getInstance().addSetData(i, x, this.ref);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#getMetaData()
	 */
	@Override
    public final ResultSetMetaData getMetaData() throws SQLException {
		return this.pstmt.getMetaData();
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date, java.util.Calendar)
	 */
	@Override
    public final void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		setData(parameterIndex, x);
		this.pstmt.setDate(parameterIndex, x, cal);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time, java.util.Calendar)
	 */
	@Override
    public final void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		setData(parameterIndex, x);
		this.pstmt.setTime(parameterIndex, x, cal);
	}
	
	/*
	 * 
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp, java.util.Calendar)
	 */
	@Override
    public final void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		setData(parameterIndex, x);
		this.pstmt.setTimestamp(parameterIndex, x, cal);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
	 */
	@Override
    public final void setNull(int paramIndex, int sqlType, String typeName)
			throws SQLException {
		setData(paramIndex, "null");
		this.pstmt.setNull(paramIndex, sqlType, typeName);
	}
	/*
	 * 
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
	 */
	@Override
    public final void setURL(int parameterIndex, URL x) throws SQLException {
		setData(parameterIndex, x);
		this.pstmt.setURL(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#getParameterMetaData()
	 */
	@Override
    public final ParameterMetaData getParameterMetaData() throws SQLException {
		return this.pstmt.getParameterMetaData();
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream)
	 */
	@Override
	public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setAsciiStream(arg0, arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream, long)
	 */
	@Override
	public void setAsciiStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setAsciiStream(arg0, arg1, arg2);

	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream)
	 */
	@Override
	public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setBinaryStream(arg0, arg1);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, long)
	 */
	@Override
	public void setBinaryStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setBinaryStream(arg0, arg1, arg2);

	}

	
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setBlob(int, java.io.InputStream)
	 */
	@Override
	public void setBlob(int arg0, InputStream arg1) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setBlob(arg0, arg1);

	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setBlob(int, java.io.InputStream, long)
	 */
	@Override
	public void setBlob(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setBlob(arg0, arg1, arg2);

	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader)
	 */
	@Override
	public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setCharacterStream(arg0, arg1);

	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader, long)
	 */
	@Override
	public void setCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setCharacterStream(arg0, arg1, arg2);

	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setClob(int, java.io.Reader)
	 */
	@Override
	public void setClob(int arg0, Reader arg1) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setClob(arg0, arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setClob(int, java.io.Reader, long)
	 */
	@Override
	public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setClob(arg0, arg1, arg2);

	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader)
	 */
	@Override
	public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setNCharacterStream(arg0, arg1);

	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader, long)
	 */
	@Override
	public void setNCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setNCharacterStream(arg0, arg1, arg2);

	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setNClob(int, java.sql.NClob)
	 */
	@Override
	public void setNClob(int arg0, NClob arg1) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setNClob(arg0, arg1);

	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setNClob(int, java.io.Reader)
	 */
	@Override
	public void setNClob(int arg0, Reader arg1) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setNClob(arg0, arg1);

	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setNClob(int, java.io.Reader, long)
	 */
	@Override
	public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setNClob(arg0, arg1, arg2);

	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setNString(int, java.lang.String)
	 */
	@Override
	public void setNString(int arg0, String arg1) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setNString(arg0, arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setRowId(int, java.sql.RowId)
	 */
	@Override
	public void setRowId(int arg0, RowId arg1) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setRowId(arg0, arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.PreparedStatement#setSQLXML(int, java.sql.SQLXML)
	 */
	@Override
	public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
		setData(arg0, arg1);
		this.pstmt.setSQLXML(arg0, arg1);
	}


	@Override
	public void executeDB2QueryBatch() throws SQLException {
		this.pstmt.executeBatch();
	}

	@Override
	public DBPreparedStatementInfoInterface getDBPreparedStatementInfo() throws SQLException {
		return new DBPreparedStatementInfoInterface() {
			@Override
			public String getPrepareAttributes() {
				// PreparedStatement의 준비 속성을 반환하는 예시
				return "Prepare Attributes Example";
			}

			@Override
			public String[] getSQLString() {
				// PreparedStatement의 SQL 문자열을 반환하는 예시
				return new String[]{"SELECT * FROM exampleTable"};
			}

			@Override
			public DBMetaData getDBParameterMetaData() {
				// 매개변수 메타데이터를 반환하는 예시
				return new DBMetaData(0);
			}

			@Override
			public DBMetaData getDBResultSetMetaData() {
				// 결과 집합 메타데이터를 반환하는 예시
				return new DBMetaData(0);
			}

			@Override
			public Object getObject(int attributeType) {
				// 특정 속성 유형에 따라 객체를 반환하는 예시
				return "Example Object";
			}

			@Override
			public HashMap getNamedParameterMarkerMap() {
				// 명명된 매개변수 마커 맵을 반환하는 예시
				HashMap<String, Integer> namedParameterMarkerMap = new HashMap<>();
				namedParameterMarkerMap.put("parameter1", 1);
				return namedParameterMarkerMap;
			}
		};
	}

	@Override
	public void setJccArrayAtName(String s, Array array) throws SQLException {
		this.pstmt.setArray(Integer.parseInt(s), array);
	}

	@Override
	public void setJccAsciiStreamAtName(String s, InputStream inputStream) throws SQLException {
		this.pstmt.setAsciiStream(Integer.parseInt(s), inputStream);
	}

	@Override
	public void setJccAsciiStreamAtName(String s, InputStream inputStream, int i) throws SQLException {
		this.pstmt.setAsciiStream(Integer.parseInt(s), inputStream, i);
	}

	@Override
	public void setJccAsciiStreamAtName(String s, InputStream inputStream, long l) throws SQLException {
		this.pstmt.setAsciiStream(Integer.parseInt(s), inputStream, l);
	}

	@Override
	public void setJccBigDecimalAtName(String s, BigDecimal bigDecimal) throws SQLException {
		this.pstmt.setBigDecimal(Integer.parseInt(s), bigDecimal);
	}

	@Override
	public void setJccBinaryStreamAtName(String s, InputStream inputStream) throws SQLException {
		this.pstmt.setBinaryStream(Integer.parseInt(s), inputStream);
	}

	@Override
	public void setJccBinaryStreamAtName(String s, InputStream inputStream, int i) throws SQLException {
		this.pstmt.setBinaryStream(Integer.parseInt(s), inputStream, i);
	}

	@Override
	public void setJccBinaryStreamAtName(String s, InputStream inputStream, long l) throws SQLException {
		this.pstmt.setBinaryStream(Integer.parseInt(s), inputStream, l);
	}

	@Override
	public void setJccBlobAtName(String s, Blob blob) throws SQLException {
		this.pstmt.setBlob(Integer.parseInt(s),blob);
	}

	@Override
	public void setJccBlobAtName(String s, InputStream inputStream) throws SQLException {
		this.pstmt.setBlob(Integer.parseInt(s),inputStream);
	}

	@Override
	public void setJccBlobAtName(String s, InputStream inputStream, long l) throws SQLException {
		this.pstmt.setBlob(Integer.parseInt(s),inputStream,l);
	}

	@Override
	public void setJccBooleanAtName(String s, boolean b) throws SQLException {
		this.pstmt.setBoolean(Integer.parseInt(s),b);
	}

	@Override
	public void setJccByteAtName(String s, byte b) throws SQLException {
		this.pstmt.setByte(Integer.parseInt(s),b);
	}

	@Override
	public void setJccBytesAtName(String s, byte[] bytes) throws SQLException {
		this.pstmt.setBytes(Integer.parseInt(s),bytes);
	}

	@Override
	public void setJccCharacterStreamAtName(String s, Reader reader) throws SQLException {
		this.pstmt.setCharacterStream(Integer.parseInt(s),reader);
	}

	@Override
	public void setJccCharacterStreamAtName(String s, Reader reader, int i) throws SQLException {
		this.pstmt.setCharacterStream(Integer.parseInt(s),reader,i);
	}

	@Override
	public void setJccCharacterStreamAtName(String s, Reader reader, long l) throws SQLException {
		this.pstmt.setCharacterStream(Integer.parseInt(s),reader,l);
	}

	@Override
	public void setJccClobAtName(String s, Clob clob) throws SQLException {
		this.pstmt.setClob(Integer.parseInt(s),clob);
	}

	@Override
	public void setJccClobAtName(String s, Reader reader) throws SQLException {
		this.pstmt.setClob(Integer.parseInt(s),reader);
	}

	@Override
	public void setJccClobAtName(String s, Reader reader, long l) throws SQLException {
		this.pstmt.setClob(Integer.parseInt(s),reader,l);
	}

	@Override
	public void setJccDateAtName(String s, Date date) throws SQLException {
		this.pstmt.setDate(Integer.parseInt(s),date);
	}

	@Override
	public void setJccDateAtName(String s, Date date, Calendar calendar) throws SQLException {
		this.pstmt.setDate(Integer.parseInt(s),date,calendar);
	}

	@Override
	public void setJccDoubleAtName(String s, double v) throws SQLException {
		this.pstmt.setDouble(Integer.parseInt(s),v);
	}

	@Override
	public void setJccFloatAtName(String s, float v) throws SQLException {
		this.pstmt.setFloat(Integer.parseInt(s),v);
	}

	@Override
	public void setJccIntAtName(String s, int i) throws SQLException {
		this.pstmt.setInt(Integer.parseInt(s),i);
	}

	@Override
	public void setJccLongAtName(String s, long l) throws SQLException {
		this.pstmt.setLong(Integer.parseInt(s),l);
	}

	@Override
	public void setJccNullAtName(String s, int i) throws SQLException {
		this.pstmt.setNull(Integer.parseInt(s),i);
	}

	@Override
	public void setJccNullAtName(String s, int i, String s1) throws SQLException {
		this.pstmt.setNull(Integer.parseInt(s),i,s1);
	}

	@Override
	public void setJccObjectAtName(String s, Object o) throws SQLException {
	this.pstmt.setObject(Integer.parseInt(s),o);
	}

	@Override
	public void setJccObjectAtName(String s, Object o, int i) throws SQLException {
		this.pstmt.setObject(Integer.parseInt(s), o, i);
	}

	@Override
	public void setJccObjectAtName(String s, Object o, int i, int i1) throws SQLException {
		this.pstmt.setObject(Integer.parseInt(s), o, i, i1);
	}

	@Override
	public void setJccRowIdAtName(String s, RowId rowId) throws SQLException {
		this.pstmt.setRowId(Integer.parseInt(s), rowId);
	}

	@Override
	public void setJccShortAtName(String s, short i) throws SQLException {
		this.pstmt.setShort(Integer.parseInt(s),i);
	}

	@Override
	public void setJccSQLXMLAtName(String s, SQLXML sqlxml) throws SQLException {
		this.pstmt.setSQLXML(Integer.parseInt(s),sqlxml);
	}

	@Override
	public void setJccStringAtName(String s, String s1) throws SQLException {
		this.pstmt.setString(Integer.parseInt(s),s1);
	}

	@Override
	public void setJccTimeAtName(String s, Time time) throws SQLException {
		this.pstmt.setTime(Integer.parseInt(s),time);
	}

	@Override
	public void setJccTimeAtName(String s, Time time, Calendar calendar) throws SQLException {
		this.pstmt.setTime(Integer.parseInt(s),time,calendar);
	}

	@Override
	public void setJccTimestampAtName(String s, Timestamp timestamp) throws SQLException {
		this.pstmt.setTimestamp(Integer.parseInt(s),timestamp);
	}

	@Override
	public void setJccTimestampAtName(String s, Timestamp timestamp, Calendar calendar) throws SQLException {
		this.pstmt.setTimestamp(Integer.parseInt(s),timestamp,calendar);
	}

	@Override
	public void setJccUnicodeStreamAtName(String s, InputStream inputStream, int i) throws SQLException {
		this.pstmt.setUnicodeStream(Integer.parseInt(s),inputStream,i);
	}

	@Override
	public ResultSet[] getDBGeneratedKeys() throws SQLException {
		return new ResultSet[]{getGeneratedKeys()};
	}

	@Override
	public boolean isEligibleForMultiRowInsert() throws SQLException {
		if (this.sqlStatement == null) {
			return false;
		}

		// SQL 문을 소문자로 변환하여 'insert'로 시작하는지 확인합니다.
		String sqlLower = this.sqlStatement.trim().toLowerCase();
		return sqlLower.startsWith("insert into");
	}

	@Override
	public void setSupportsMultiRowInsert(boolean b) throws SQLException {
		this.supportsMultiRowInsert = b;
	}
	public boolean isSupportsMultiRowInsert() {
		return this.supportsMultiRowInsert;
	}

	@Override
	public void setDB2BlobFileReference(int i, DB2BlobFileReference db2BlobFileReference) throws SQLException {
		Blob blob = this.connection.createBlob(); // Assuming a method to convert to Blob
		this.pstmt.setBlob(i, blob);
	}


	@Override
	public void setDB2ClobFileReference(int i, DB2ClobFileReference db2ClobFileReference) throws SQLException {
		Clob clob = this.connection.createClob(); // Assuming a method to convert to Clob
		this.pstmt.setClob(i, clob);
	}

	@Override
	public void setDB2XmlAsBlobFileReference(int i, DB2XmlAsBlobFileReference db2XmlAsBlobFileReference) throws SQLException {Blob blob = this.connection.createBlob();

		// 파일 경로 얻기 (가정: DB2BlobFileReference 클래스에 getFilePath 메서드가 있다고 가정)
		String filePath = db2XmlAsBlobFileReference.getFileName();

		// 파일에서 데이터를 읽어 Blob 객체에 쓰기
		readFileIntoBlob(filePath, blob);

		// PreparedStatement에 Blob 객체를 이름 기반으로 설정
		this.pstmt.setBlob(i, blob);
	}

	@Override
	public void setDB2XmlAsClobFileReference(int i, DB2XmlAsClobFileReference db2XmlAsClobFileReference) throws SQLException {
		Clob clob = this.connection.createClob();

		// 파일 경로 얻기 (DB2FileReference에서 제공하는 getFileName 메서드 사용)
		String filePath = db2XmlAsClobFileReference.getFileName();

		// 파일에서 데이터를 읽어 Clob 객체에 쓰기
		readFileIntoClob(filePath, clob);

		// PreparedStatement에 Clob 객체를 이름 기반으로 설정
		this.pstmt.setClob(i, clob);
	}

	@Override
	public void setJccDB2BlobFileReferenceAtName(String s, DB2BlobFileReference db2BlobFileReference) throws SQLException {
		Blob blob = this.connection.createBlob();

		// 파일 경로 얻기 (가정: DB2BlobFileReference 클래스에 getFilePath 메서드가 있다고 가정)
		String filePath = db2BlobFileReference.getFileName();

		// 파일에서 데이터를 읽어 Blob 객체에 쓰기
		readFileIntoBlob(filePath, blob);

		// PreparedStatement에 Blob 객체를 이름 기반으로 설정
		this.pstmt.setBlob(Integer.parseInt(s), blob);
	}

	@Override
	public void setJccDB2ClobFileReferenceAtName(String s, DB2ClobFileReference db2ClobFileReference) throws SQLException {
		Clob clob = this.connection.createClob();

		// 파일 경로 얻기 (DB2FileReference에서 제공하는 getFileName 메서드 사용)
		String filePath = db2ClobFileReference.getFileName();

		// 파일에서 데이터를 읽어 Clob 객체에 쓰기
		readFileIntoClob(filePath, clob);

		// PreparedStatement에 Clob 객체를 이름 기반으로 설정
		this.pstmt.setClob(Integer.parseInt(s), clob);
	}

	@Override
	public void setJccDB2XmlAsBlobFileReferenceAtName(String s, DB2XmlAsBlobFileReference db2XmlAsBlobFileReference) throws SQLException {
		Blob blob = this.connection.createBlob();

		// 파일 경로 얻기 (DB2FileReference에서 제공하는 getFileName 메서드 사용)
		String filePath = db2XmlAsBlobFileReference.getFileName();

		// 파일에서 데이터를 읽어 Blob 객체에 쓰기
		readFileIntoBlob(filePath, blob);

		// PreparedStatement에 Blob 객체를 이름 기반으로 설정
		this.pstmt.setBlob(Integer.parseInt(s), blob);

	}

	@Override
	public void setJccDB2XmlAsClobFileReferenceAtName(String s, DB2XmlAsClobFileReference db2XmlAsClobFileReference) throws SQLException {
		Clob clob = this.connection.createClob();

		// 파일 경로 얻기 (DB2FileReference에서 제공하는 getFileName 메서드 사용)
		String filePath = db2XmlAsClobFileReference.getFileName();

		// 파일에서 데이터를 읽어 Clob 객체에 쓰기
		readFileIntoClob(filePath, clob);

		// PreparedStatement에 Clob 객체를 이름 기반으로 설정
		this.pstmt.setClob(Integer.parseInt(s), clob);
	}

	@Override
	public void setDBDefault(int i) throws SQLException {
		this.pstmt.setNull(i, java.sql.Types.NULL);
	}

	@Override
	public void setJccDBDefaultAtName(String s) throws SQLException {
		this.pstmt.setObject(Integer.parseInt(s), null);
	}

	@Override
	public void setDBUnassigned(int i) throws SQLException {
		this.pstmt.setNull(i, java.sql.Types.NULL);
	}

	@Override
	public void setJccDBUnassignedAtName(String s) throws SQLException {
		this.pstmt.setObject(Integer.parseInt(s), null);
	}

	@Override
	public void setDBTimestamp(int i, DBTimestamp dbTimestamp) throws SQLException {
		this.pstmt.setTimestamp(i, dbTimestamp);

	}

	@Override
	public void setJccDBTimestampAtName(String s, DBTimestamp dbTimestamp) throws SQLException {
		this.pstmt.setTimestamp(Integer.parseInt(s), dbTimestamp);
	}

	@Override
	public int getEstimateRowCount() throws SQLException {
		int estimatedRowCount = -1; // 가정된 값

		// 실제 구현 시에는 이 부분을 DB2 JDBC 드라이버의 실제 메소드로 대체해야 합니다.
	estimatedRowCount = this.pstmt.getUpdateCount();

		// 여기에서는 예시로 -1을 반환합니다. 실제로는 예상 행 수를 반환해야 합니다.
		return estimatedRowCount;

	}

	@Override
	public int getEstimateCost() throws SQLException {
		int estimatedCost = -1; // 예시로 -1을 설정 (비용을 계산할 수 없음을 의미)

		try {
		estimatedCost = this.pstmt.getUpdateCount();

			// ... 비용 계산 로직 ...

		} catch (SQLException e) {
			// SQL 예외 처리
			throw e;
		}

		// 계산된 비용 반환
		return estimatedCost;
	}

	@Override
	public void setDBStringAsBytes(int i, byte[] bytes, int i1) throws SQLException {
		String value = new String(bytes, 0, bytes.length, StandardCharsets.UTF_8);
		this.pstmt.setString(i, value);
	}

	@Override
	public void setDBDateAsBytes(int i, byte[] bytes, int i1) throws SQLException {
		String dateString = new String(bytes, 0, bytes.length, StandardCharsets.UTF_8);
		Date date = Date.valueOf(dateString);
		this.pstmt.setDate(i, date);
	}

	@Override
	public void setDBTimeAsBytes(int i, byte[] bytes, int i1) throws SQLException {
		String timeString = new String(bytes, 0, bytes.length, StandardCharsets.UTF_8);
		Time time = Time.valueOf(timeString);
		this.pstmt.setTime(i, time);
	}

	@Override
	public void setDBTimestampAsBytes(int i, byte[] bytes, int i1) throws SQLException {
		String timestampString = new String(bytes, 0, bytes.length, StandardCharsets.UTF_8);
		Timestamp timestamp = Timestamp.valueOf(timestampString);
		this.pstmt.setTimestamp(i, timestamp);
	}
	private void readFileIntoBlob(String filePath, Blob blob) throws SQLException {
		try (InputStream inputStream = Files.newInputStream(Paths.get(filePath))) {
			byte[] buffer = new byte[1024];
			int bytesRead;
			while ((bytesRead = inputStream.read(buffer)) != -1) {
				blob.setBytes(blob.length() + 1, buffer, 0, bytesRead);
			}
		} catch (IOException e) {
			throw new SQLException("Error reading file", e);
		}
	}
	private void readFileIntoClob(String filePath, Clob clob) throws SQLException {
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(
						Files.newInputStream(Paths.get(filePath)), StandardCharsets.UTF_8))) {
			String line;
			while ((line = reader.readLine()) != null) {
				clob.setString(clob.length() + 1, line + "\n");
			}
		} catch (IOException e) {
			throw new SQLException("Error reading file", e);
		}
}}
