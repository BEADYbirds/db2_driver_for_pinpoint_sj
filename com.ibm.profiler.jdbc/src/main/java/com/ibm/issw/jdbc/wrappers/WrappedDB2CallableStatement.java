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


import com.ibm.db2.jcc.DB2CallableStatement;
import com.ibm.db2.jcc.DBTimestamp;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * 
 * WrappedCallableStatement
 */
public class WrappedDB2CallableStatement extends WrappedDB2PreparedStatement
		implements DB2CallableStatement {

	private final DB2CallableStatement stmt;

	/**
	 * 
	 * @param statement the statement
	 * @param sql sql string
	 * @param ref reference string
	 * @param transaction transaction string
	 * @param connection the connection
	 */
	public WrappedDB2CallableStatement(DB2CallableStatement statement, String sql,
			String ref, String transaction, Connection connection) {
		super(statement, sql, ref, transaction, connection);
		this.stmt = statement;
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#registerOutParameter(int, int)
	 */
	@Override
    public void registerOutParameter(int parameterIndex, int sqlType)
			throws SQLException {
		this.stmt.registerOutParameter(parameterIndex, sqlType);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#registerOutParameter(int, int, int)
	 */
	@Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale)
			throws SQLException {
		this.stmt.registerOutParameter(parameterIndex, sqlType, scale);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#wasNull()
	 */
	@Override
    public boolean wasNull() throws SQLException {
		return this.stmt.wasNull();
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getString(int)
	 */
	@Override
    public String getString(int parameterIndex) throws SQLException {
		return this.stmt.getString(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getBoolean(int)
	 */
	@Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
		return this.stmt.getBoolean(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getByte(int)
	 */
	@Override
    public byte getByte(int parameterIndex) throws SQLException {
		return this.stmt.getByte(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getShort(int)
	 */
	@Override
    public short getShort(int parameterIndex) throws SQLException {
		return this.stmt.getShort(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getInt(int)
	 */
	@Override
    public int getInt(int parameterIndex) throws SQLException {
		return this.stmt.getInt(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getLong(int)
	 */
	@Override
    public long getLong(int parameterIndex) throws SQLException {
		return this.stmt.getLong(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getFloat(int)
	 */
	@Override
    public float getFloat(int parameterIndex) throws SQLException {
		return this.stmt.getFloat(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getDouble(int)
	 */
	@Override
    public double getDouble(int parameterIndex) throws SQLException {
		return this.stmt.getDouble(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getBigDecimal(int, int)
	 */
	@Override
    @Deprecated
	public BigDecimal getBigDecimal(int parameterIndex, int scale)
			throws SQLException {
		//$ANALYSIS-IGNORE
		return this.stmt.getBigDecimal(parameterIndex, scale);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getBytes(int)
	 */
	@Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
		return this.stmt.getBytes(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getDate(int)
	 */
	@Override
    public Date getDate(int parameterIndex) throws SQLException {
		return this.stmt.getDate(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getTime(int)
	 */
	@Override
    public Time getTime(int parameterIndex) throws SQLException {
		return this.stmt.getTime(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getTimestamp(int)
	 */
	@Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
		return this.stmt.getTimestamp(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getObject(int)
	 */
	@Override
    public Object getObject(int parameterIndex) throws SQLException {
		return this.stmt.getObject(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getBigDecimal(int)
	 */
	@Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
		return this.stmt.getBigDecimal(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getRef(int)
	 */
	@Override
    public Ref getRef(int i) throws SQLException {
		return this.stmt.getRef(i);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getBlob(int)
	 */
	@Override
    public Blob getBlob(int i) throws SQLException {
		return this.stmt.getBlob(i);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getClob(int)
	 */
	@Override
    public Clob getClob(int i) throws SQLException {
		return this.stmt.getClob(i);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getArray(int)
	 */
	@Override
    public Array getArray(int i) throws SQLException {
		return this.stmt.getArray(i);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getDate(int, java.util.Calendar)
	 */
	@Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
		return this.stmt.getDate(parameterIndex, cal);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getTime(int, java.util.Calendar)
	 */
	@Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
		return this.stmt.getTime(parameterIndex, cal);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getTimestamp(int, java.util.Calendar)
	 */
	@Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal)
			throws SQLException {
		return this.stmt.getTimestamp(parameterIndex, cal);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#registerOutParameter(int, int, java.lang.String)
	 */
	@Override
    public void registerOutParameter(int paramIndex, int sqlType,
			String typeName) throws SQLException {
		this.stmt.registerOutParameter(paramIndex, sqlType, typeName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int)
	 */
	@Override
    public void registerOutParameter(String parameterName, int sqlType)
			throws SQLException {
		this.stmt.registerOutParameter(parameterName, sqlType);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, int)
	 */
	@Override
    public void registerOutParameter(String parameterName, int sqlType,
			int scale) throws SQLException {
		this.stmt.registerOutParameter(parameterName, sqlType, scale);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, java.lang.String)
	 */
	@Override
    public void registerOutParameter(String parameterName, int sqlType,
			String typeName) throws SQLException {
		this.stmt.registerOutParameter(parameterName, sqlType, typeName);
	}
	/*
	 * 
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getURL(int)
	 */
	@Override
    public URL getURL(int parameterIndex) throws SQLException {
		return this.stmt.getURL(parameterIndex);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setURL(java.lang.String, java.net.URL)
	 */
	@Override
    public void setURL(String parameterName, URL val) throws SQLException {
		this.stmt.setURL(parameterName, val);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int)
	 */
	@Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
		this.stmt.setNull(parameterName, sqlType);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setBoolean(java.lang.String, boolean)
	 */
	@Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
		this.stmt.setBoolean(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setByte(java.lang.String, byte)
	 */
	@Override
    public void setByte(String parameterName, byte x) throws SQLException {
		this.stmt.setByte(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setShort(java.lang.String, short)
	 */
	@Override
    public void setShort(String parameterName, short x) throws SQLException {
		this.stmt.setShort(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setInt(java.lang.String, int)
	 */
	@Override
    public void setInt(String parameterName, int x) throws SQLException {
		this.stmt.setInt(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setLong(java.lang.String, long)
	 */
	@Override
    public void setLong(String parameterName, long x) throws SQLException {
		this.stmt.setLong(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setFloat(java.lang.String, float)
	 */
	@Override
    public void setFloat(String parameterName, float x) throws SQLException {
		this.stmt.setFloat(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setDouble(java.lang.String, double)
	 */
	@Override
    public void setDouble(String parameterName, double x) throws SQLException {
		this.stmt.setDouble(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setBigDecimal(java.lang.String, java.math.BigDecimal)
	 */
	@Override
    public void setBigDecimal(String parameterName, BigDecimal x)
			throws SQLException {
		this.stmt.setBigDecimal(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setString(java.lang.String, java.lang.String)
	 */
	@Override
    public void setString(String parameterName, String x) throws SQLException {
		this.stmt.setString(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setBytes(java.lang.String, byte[])
	 */
	@Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
		this.stmt.setBytes(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date)
	 */
	@Override
    public void setDate(String parameterName, Date x) throws SQLException {
		this.stmt.setDate(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time)
	 */
	@Override
    public void setTime(String parameterName, Time x) throws SQLException {
		this.stmt.setTime(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	@Override
    public void setTimestamp(String parameterName, Timestamp x)
			throws SQLException {
		this.stmt.setTimestamp(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream, int)
	 */
	@Override
    public void setAsciiStream(String parameterName, InputStream x, int length)
			throws SQLException {
		this.stmt.setAsciiStream(parameterName, x, length);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, int)
	 */
	@Override
    public void setBinaryStream(String parameterName, InputStream x, int length)
			throws SQLException {
		this.stmt.setBinaryStream(parameterName, x, length);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int, int)
	 */
	@Override
    public void setObject(String parameterName, Object x, int targetSqlType,
			int scale) throws SQLException {
		this.stmt.setObject(parameterName, x, targetSqlType, scale);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int)
	 */
	@Override
    public void setObject(String parameterName, Object x, int targetSqlType)
			throws SQLException {
		this.stmt.setObject(parameterName, x, targetSqlType);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object)
	 */
	@Override
    public void setObject(String parameterName, Object x) throws SQLException {
		this.stmt.setObject(parameterName, x);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, int)
	 */
	@Override
    public void setCharacterStream(String parameterName, Reader reader,
			int length) throws SQLException {
		this.stmt.setCharacterStream(parameterName, reader, length);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date, java.util.Calendar)
	 */
	@Override
    public void setDate(String parameterName, Date x, Calendar cal)
			throws SQLException {
		this.stmt.setDate(parameterName, x, cal);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time, java.util.Calendar)
	 */
	@Override
    public void setTime(String parameterName, Time x, Calendar cal)
			throws SQLException {
		this.stmt.setTime(parameterName, x, cal);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp, java.util.Calendar)
	 */
	@Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
			throws SQLException {
		this.stmt.setTimestamp(parameterName, x, cal);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int, java.lang.String)
	 */
	@Override
    public void setNull(String parameterName, int sqlType, String typeName)
			throws SQLException {
		this.stmt.setNull(parameterName, sqlType, typeName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getString(java.lang.String)
	 */
	@Override
    public String getString(String parameterName) throws SQLException {
		return this.stmt.getString(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getBoolean(java.lang.String)
	 */
	@Override
    public boolean getBoolean(String parameterName) throws SQLException {
		return this.stmt.getBoolean(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getByte(java.lang.String)
	 */
	@Override
    public byte getByte(String parameterName) throws SQLException {
		return this.stmt.getByte(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getShort(java.lang.String)
	 */
	@Override
    public short getShort(String parameterName) throws SQLException {
		return this.stmt.getShort(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getInt(java.lang.String)
	 */
	@Override
    public int getInt(String parameterName) throws SQLException {
		return this.stmt.getInt(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getLong(java.lang.String)
	 */
	@Override
    public long getLong(String parameterName) throws SQLException {
		return this.stmt.getLong(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getFloat(java.lang.String)
	 */
	@Override
    public float getFloat(String parameterName) throws SQLException {
		return this.stmt.getFloat(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getDouble(java.lang.String)
	 */
	@Override
    public double getDouble(String parameterName) throws SQLException {
		return this.stmt.getDouble(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getBytes(java.lang.String)
	 */
	@Override
    public byte[] getBytes(String parameterName) throws SQLException {
		return this.stmt.getBytes(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getDate(java.lang.String)
	 */
	@Override
    public Date getDate(String parameterName) throws SQLException {
		return this.stmt.getDate(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getTime(java.lang.String)
	 */
	@Override
    public Time getTime(String parameterName) throws SQLException {
		return getTime(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getTimestamp(java.lang.String)
	 */
	@Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
		return this.stmt.getTimestamp(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getObject(java.lang.String)
	 */
	@Override
    public Object getObject(String parameterName) throws SQLException {
		return this.stmt.getObject(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getBigDecimal(java.lang.String)
	 */
	@Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		return this.stmt.getBigDecimal(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getRef(java.lang.String)
	 */
	@Override
    public Ref getRef(String parameterName) throws SQLException {
		return this.stmt.getRef(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getBlob(java.lang.String)
	 */
	@Override
    public Blob getBlob(String parameterName) throws SQLException {
		return this.stmt.getBlob(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getClob(java.lang.String)
	 */
	@Override
    public Clob getClob(String parameterName) throws SQLException {
		return this.stmt.getClob(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getArray(java.lang.String)
	 */
	@Override
    public Array getArray(String parameterName) throws SQLException {
		return this.stmt.getArray(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getDate(java.lang.String, java.util.Calendar)
	 */
	@Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
		return this.stmt.getDate(parameterName, cal);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getTime(java.lang.String, java.util.Calendar)
	 */
	@Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
		return this.stmt.getTime(parameterName, cal);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getTimestamp(java.lang.String, java.util.Calendar)
	 */
	@Override
    public Timestamp getTimestamp(String parameterName, Calendar cal)
			throws SQLException {
		return this.stmt.getTimestamp(parameterName, cal);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getURL(java.lang.String)
	 */
	@Override
    public URL getURL(String parameterName) throws SQLException {
		return this.stmt.getURL(parameterName);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getCharacterStream(int)
	 */
	@Override
	public Reader getCharacterStream(int arg0) throws SQLException {
		return this.stmt.getCharacterStream(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getCharacterStream(java.lang.String)
	 */
	@Override
	public Reader getCharacterStream(String arg0) throws SQLException {
		return this.stmt.getCharacterStream(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getNCharacterStream(int)
	 */
	@Override
	public Reader getNCharacterStream(int arg0) throws SQLException {
		return this.stmt.getNCharacterStream(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getNCharacterStream(java.lang.String)
	 */
	@Override
	public Reader getNCharacterStream(String arg0) throws SQLException {
		return this.stmt.getNCharacterStream(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getNClob(int)
	 */
	@Override
	public NClob getNClob(int arg0) throws SQLException {
		return this.stmt.getNClob(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getNClob(java.lang.String)
	 */
	@Override
	public NClob getNClob(String arg0) throws SQLException {
		return this.stmt.getNClob(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getNString(int)
	 */
	@Override
	public String getNString(int arg0) throws SQLException {
		return this.stmt.getNString(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getNString(java.lang.String)
	 */
	@Override
	public String getNString(String arg0) throws SQLException {
		return this.stmt.getNString(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getObject(int, java.util.Map)
	 */
	@Override
	public Object getObject(int arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		return this.stmt.getObject(arg0,arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getObject(java.lang.String, java.util.Map)
	 */
	@Override
	public Object getObject(String arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		return this.stmt.getObject(arg0,arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getRowId(int)
	 */
	@Override
	public RowId getRowId(int arg0) throws SQLException {
		return this.stmt.getRowId(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getRowId(java.lang.String)
	 */
	@Override
	public RowId getRowId(String arg0) throws SQLException {
		return this.stmt.getRowId(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getSQLXML(int)
	 */
	@Override
	public SQLXML getSQLXML(int arg0) throws SQLException {
		return this.stmt.getSQLXML(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getSQLXML(java.lang.String)
	 */
	@Override
	public SQLXML getSQLXML(String arg0) throws SQLException {
		return this.stmt.getSQLXML(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream)
	 */
	@Override
	public void setAsciiStream(String arg0, InputStream arg1)
			throws SQLException {
		this.stmt.setAsciiStream(arg0,arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream, long)
	 */
	@Override
	public void setAsciiStream(String arg0, InputStream arg1, long arg2)
			throws SQLException {
		this.stmt.setAsciiStream(arg0,arg1, arg2);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream)
	 */
	@Override
	public void setBinaryStream(String arg0, InputStream arg1)
			throws SQLException {
		this.stmt.setBinaryStream(arg0,arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, long)
	 */
	@Override
	public void setBinaryStream(String arg0, InputStream arg1, long arg2)
			throws SQLException {
		this.stmt.setBinaryStream(arg0,arg1, arg2);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setBlob(java.lang.String, java.sql.Blob)
	 */
	@Override
	public void setBlob(String arg0, Blob arg1) throws SQLException {
		this.stmt.setBlob(arg0,arg1);
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setBlob(java.lang.String, java.io.InputStream)
	 */
	@Override
	public void setBlob(String arg0, InputStream arg1) throws SQLException {
		this.stmt.setBlob(arg0,arg1);
	}
	/*
	 * 
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setBlob(java.lang.String, java.io.InputStream, long)
	 */
	@Override
	public void setBlob(String arg0, InputStream arg1, long arg2)
			throws SQLException {
		this.stmt.setBlob(arg0,arg1, arg2);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader)
	 */
	@Override
	public void setCharacterStream(String arg0, Reader arg1)
			throws SQLException {
		this.stmt.setCharacterStream(arg0,arg1);
		
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, long)
	 */
	@Override
	public void setCharacterStream(String arg0, Reader arg1, long arg2)
			throws SQLException {
		this.stmt.setCharacterStream(arg0,arg1, arg2);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setClob(java.lang.String, java.sql.Clob)
	 */
	@Override
	public void setClob(String arg0, Clob arg1) throws SQLException {
		this.stmt.setClob(arg0,arg1);
		
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setClob(java.lang.String, java.io.Reader)
	 */
	@Override
	public void setClob(String arg0, Reader arg1) throws SQLException {
		this.stmt.setClob(arg0,arg1);
		
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setClob(java.lang.String, java.io.Reader, long)
	 */
	@Override
	public void setClob(String arg0, Reader arg1, long arg2)
			throws SQLException {
		this.stmt.setClob(arg0,arg1, arg2);
		
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setNCharacterStream(java.lang.String, java.io.Reader)
	 */
	@Override
	public void setNCharacterStream(String arg0, Reader arg1)
			throws SQLException {
		this.stmt.setNCharacterStream(arg0,arg1);
		
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setNCharacterStream(java.lang.String, java.io.Reader, long)
	 */
	@Override
	public void setNCharacterStream(String arg0, Reader arg1, long arg2)
			throws SQLException {
		this.stmt.setNCharacterStream(arg0,arg1, arg2);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setNClob(java.lang.String, java.sql.NClob)
	 */
	@Override
	public void setNClob(String arg0, NClob arg1) throws SQLException {
		this.stmt.setNClob(arg0,arg1);

		
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setNClob(java.lang.String, java.io.Reader)
	 */
	@Override
	public void setNClob(String arg0, Reader arg1) throws SQLException {
		this.stmt.setNClob(arg0,arg1);
		
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setNClob(java.lang.String, java.io.Reader, long)
	 */
	@Override
	public void setNClob(String arg0, Reader arg1, long arg2)
			throws SQLException {
		this.stmt.setNClob(arg0,arg1, arg2);
		
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setNString(java.lang.String, java.lang.String)
	 */
	@Override
	public void setNString(String arg0, String arg1) throws SQLException {
		this.stmt.setNString(arg0,arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setRowId(java.lang.String, java.sql.RowId)
	 */
	@Override
	public void setRowId(String arg0, RowId arg1) throws SQLException {
		this.stmt.setRowId(arg0,arg1);
		
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#setSQLXML(java.lang.String, java.sql.SQLXML)
	 */
	@Override
	public void setSQLXML(String arg0, SQLXML arg1) throws SQLException {
		this.stmt.setSQLXML(arg0,arg1);
		
	}

	/*
	 * (non-Javadoc)
	 * @see java.sql.Statement#closeOnCompletion()
	 */
	@Override
	public void closeOnCompletion() throws SQLException {
		this.stmt.closeOnCompletion();
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.Statement#isCloseOnCompletion()
	 */
	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return this.stmt.isCloseOnCompletion();
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getObject(int, java.lang.Class)
	 */
	@Override
	public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
		return this.stmt.getObject(arg0, arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see java.sql.CallableStatement#getObject(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
		return this.stmt.getObject(arg0, arg1);
	}

	@Override
	public void registerJccOutParameterAtName(String s, int i) throws SQLException {
		this.stmt.registerJccOutParameterAtName(s, i);
	}

	@Override
	public void registerJccOutParameterAtName(String s, int i, int i1) throws SQLException {
		this.stmt.registerJccOutParameterAtName(s, i,i1);
	}

	@Override
	public void registerJccOutParameterAtName(String s, int i, String s1) throws SQLException {
		this.stmt.registerJccOutParameterAtName(s, i,s1);
	}

	@Override
	public DBTimestamp getDBTimestamp(int i) throws SQLException {
		java.sql.Timestamp sqlTimestamp = this.stmt.getTimestamp(i);
		return sqlTimestamp != null ? new DBTimestamp(sqlTimestamp) : null;
	}

	@Override
	public DBTimestamp getDBTimestamp(String s) throws SQLException {
		java.sql.Timestamp sqlTimestamp = this.stmt.getTimestamp(s);
		return sqlTimestamp != null ? new DBTimestamp(sqlTimestamp) : null;
	}

	@Override
	public void setDBTimestamp(String s, DBTimestamp dbTimestamp) throws SQLException {
		String timeStr = dbTimestamp.toString(); // DBTimestamp의 시간 정보를 문자열로 가져옵니다.

		// 문자열을 파싱하여 시, 분, 초를 추출합니다.
		int hour = Integer.parseInt(timeStr.substring(0, 2));
		int minute = Integer.parseInt(timeStr.substring(3, 5));
		int second = Integer.parseInt(timeStr.substring(6, 8));

		// java.sql.Timestamp 객체 생성
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, hour);
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, second);
		java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(calendar.getTimeInMillis());

		// 내부 DB2CallableStatement 객체의 setTimestamp 메서드를 호출합니다.
		this.stmt.setTimestamp(s, sqlTimestamp);
	}

	@Override
	public Array getJccArrayAtName(String s) throws SQLException {
		return this.stmt.getArray(s);
	}

	@Override
	public BigDecimal getJccBigDecimalAtName(String s, int i) throws SQLException {
		BigDecimal bd = this.stmt.getBigDecimal(s);
		if (bd != null) {
			bd = bd.setScale(i);
		}
		return bd;
	}

	@Override
	public BigDecimal getJccBigDecimalAtName(String s) throws SQLException {
		return this.stmt.getBigDecimal(s);
	}

	@Override
	public Blob getJccBlobAtName(String s) throws SQLException {
		return this.stmt.getBlob(s);
	}

	@Override
	public boolean getJccBooleanAtName(String s) throws SQLException {
		return this.stmt.getBoolean(s);
	}

	@Override
	public byte getJccByteAtName(String s) throws SQLException {
		return this.stmt.getByte(s);
	}

	@Override
	public byte[] getJccBytesAtName(String s) throws SQLException {
		return this.stmt.getBytes(s);
	}

	@Override
	public Clob getJccClobAtName(String s) throws SQLException {
		return this.stmt.getClob(s);
	}

	@Override
	public Date getJccDateAtName(String s) throws SQLException {
		return this.stmt.getDate(s);
	}

	@Override
	public Date getJccDateAtName(String s, Calendar calendar) throws SQLException {
		return this.stmt.getDate(s, calendar);
	}

	@Override
	public double getJccDoubleAtName(String s) throws SQLException {
		return this.stmt.getDouble(s);
	}

	@Override
	public float getJccFloatAtName(String s) throws SQLException {
		return this.stmt.getFloat(s);
	}

	@Override
	public int getJccIntAtName(String s) throws SQLException {
		return this.stmt.getInt(s);
	}

	@Override
	public long getJccLongAtName(String s) throws SQLException {
		return this.stmt.getLong(s);
	}

	@Override
	public Object getJccObjectAtName(String s) throws SQLException {
		return this.stmt.getObject(s);
	}

	@Override
	public short getJccShortAtName(String s) throws SQLException {
		return this.stmt.getShort(s);
	}

	@Override
	public String getJccStringAtName(String s) throws SQLException {
		return this.stmt.getString(s);
	}

	@Override
	public Time getJccTimeAtName(String s) throws SQLException {
		return this.stmt.getTime(s);
	}

	@Override
	public Time getJccTimeAtName(String s, Calendar calendar) throws SQLException {
		return this.stmt.getTime(s, calendar);
	}

	@Override
	public Timestamp getJccTimestampAtName(String s) throws SQLException {
		return this.stmt.getTimestamp(s);
	}

	@Override
	public Timestamp getJccTimestampAtName(String s, Calendar calendar) throws SQLException {
		return this.stmt.getTimestamp(s, calendar);
	}

	@Override
	public RowId getJccRowIdAtName(String s) throws SQLException {
		return this.stmt.getRowId(s);
	}

	@Override
	public SQLXML getJccSQLXMLAtName(String s) throws SQLException {
		return this.stmt.getSQLXML(s);
	}
}
