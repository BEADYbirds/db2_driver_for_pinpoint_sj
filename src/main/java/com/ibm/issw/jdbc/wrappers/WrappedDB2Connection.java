
package com.ibm.issw.jdbc.wrappers;


import com.ibm.db2.jcc.DB2Connection;
import com.ibm.db2.jcc.DB2SystemMonitor;
import com.ibm.db2.jcc.LoadResult;
import com.ibm.issw.jdbc.profiler.JdbcProfiler;
import org.ietf.jgss.GSSCredential;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 
 * WrappedDB2Connection
 */
public class WrappedDB2Connection extends WrappedConnection implements DB2Connection{


	private final DB2Connection connection;

	/**
	 * ctor
	 * @param connection the connection
	 */
	public WrappedDB2Connection(DB2Connection connection) {
		super(connection);
		this.connection = connection;
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#alternateWasUsedOnConnect()
	 */
	@Override
	public boolean alternateWasUsedOnConnect() throws SQLException {
		return connection.alternateWasUsedOnConnect();
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#changeDB2Password(java.lang.String, java.lang.String)
	 */
	@Override
	public void changeDB2Password(String arg0, String arg1) throws SQLException {
		connection.changeDB2Password(arg0, arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#deregisterDB2XmlObject(java.lang.String, java.lang.String)
	 */
	@Override
	public void deregisterDB2XmlObject(String arg0, String arg1)
			throws SQLException {
		connection.deregisterDB2XmlObject(arg0, arg1);
		
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#enableJccDateTimeMutation(boolean)
	 */
	@Override
	public void enableJccDateTimeMutation(boolean arg0) throws SQLException {
		connection.enableJccDateTimeMutation(arg0);
		
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#fct()
	 */
	@Override
	public void fct() {
		connection.fct();
		
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getDB2ClientAccountingInformation()
	 */
	@Deprecated
	@Override
	public String getDB2ClientAccountingInformation() throws SQLException {
		//$ANALYSIS-IGNORE
		return connection.getDB2ClientAccountingInformation();
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getDB2ClientApplicationInformation()
	 */
	@Deprecated
	@Override
	public String getDB2ClientApplicationInformation() throws SQLException {
		//$ANALYSIS-IGNORE
		return connection.getDB2ClientApplicationInformation();
	}
	/*
	 * 
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getDB2ClientProgramId()
	 */
	@Override
	public String getDB2ClientProgramId() throws SQLException {
		return connection.getDB2ClientProgramId();
	}

	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getDB2ClientUser()
	 */
	@Deprecated
	@Override
	public String getDB2ClientUser() throws SQLException {
		//$ANALYSIS-IGNORE
		return connection.getDB2ClientUser();
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getDB2ClientWorkstation()
	 */
	@Deprecated
	@Override
	public String getDB2ClientWorkstation() throws SQLException {
		//$ANALYSIS-IGNORE
		return connection.getDB2ClientWorkstation();
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getDB2Correlator()
	 */
	@Override
	public String getDB2Correlator() throws SQLException {
		return connection.getDB2Correlator();
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getDB2CurrentPackagePath()
	 */
	@Override
	public String getDB2CurrentPackagePath() throws SQLException {
		return connection.getDB2CurrentPackagePath();
	}

	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getDB2CurrentPackageSet()
	 */
	@Override
	public String getDB2CurrentPackageSet() throws SQLException {
		
		return connection.getDB2CurrentPackageSet();
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getDB2SecurityMechanism()
	 */
	@Override
	public int getDB2SecurityMechanism() throws SQLException {
		
		return connection.getDB2SecurityMechanism();
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getDB2SystemMonitor()
	 */
	@Override
	public DB2SystemMonitor getDB2SystemMonitor() throws SQLException {
		
		return connection.getDB2SystemMonitor();
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getDBProgressiveStreaming()
	 */
	@Override
	public int getDBProgressiveStreaming() throws SQLException {
		
		return connection.getDBProgressiveStreaming();
	}

	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getJCCLogWriter()
	 */
	@Override
	public PrintWriter getJCCLogWriter() throws SQLException {
		
		return connection.getJCCLogWriter();
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#getJccLogWriter()
	 */
	@Override
	public PrintWriter getJccLogWriter() throws SQLException {
		
		return connection.getJccLogWriter();
	}
//	/*
//	 * (non-Javadoc)
//	 * @see com.ibm.db2.jcc.DB2Connection#getJccSpecialRegisterProperties()
//	 */
//	@Override
//	public Properties getJccSpecialRegisterProperties() throws SQLException {
//		
//		return connection.getJccSpecialRegisterProperties();
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * @see com.ibm.db2.jcc.DB2Connection#getMaxRowsetSize()
//	 */
//	@Override
//	public int getMaxRowsetSize() throws SQLException {
//		
//		return connection.getMaxRowsetSize();
//	}

	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#installDB2JavaStoredProcedure(java.io.InputStream, int, java.lang.String)
	 */
	@Override
	public void installDB2JavaStoredProcedure(InputStream arg0, int arg1,
			String arg2) throws SQLException {
		
		connection.installDB2JavaStoredProcedure(arg0, arg1, arg2);
		
	}

	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#isDB2Alive()
	 */
	@Deprecated
	@Override
	public boolean isDB2Alive() throws SQLException {
		
		//$ANALYSIS-IGNORE
		return connection.isDB2Alive();
	}

	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#isDB2GatewayConnection()
	 */
	@Override
	public boolean isDB2GatewayConnection() throws SQLException {
		
		return connection.isDB2GatewayConnection();
	}

	
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#isDBValid(boolean, int)
	 */
	@Override
	public boolean isDBValid(boolean arg0, int arg1) throws SQLException {
		
		return connection.isDBValid(arg0, arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#isInDB2UnitOfWork()
	 */
	@Override
	public boolean isInDB2UnitOfWork() throws SQLException {
		
		return connection.isInDB2UnitOfWork();
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#prepareDB2OptimisticLockingQuery(java.lang.String, int)
	 */
	@Override
    public PreparedStatement prepareDB2OptimisticLockingQuery(String arg0, int arg1) throws SQLException
    {

        boolean wrappingEnabled = isWrappingEnabled();
        String ref = null;
        if (wrappingEnabled)
        {
            ref = getNextRefCount();
            JdbcProfiler.getInstance().setStatementType(JdbcProfiler.PREPARED, ref);
            JdbcProfiler.getInstance().start(JdbcProfiler.OP_PREPARE, ref);
        }
        PreparedStatement prepareStatement = connection.prepareDB2OptimisticLockingQuery(arg0, arg1);
        if (wrappingEnabled)
        {

            PreparedStatement pstmt = null;
            pstmt = wrapPreparedStatement(arg0, ref, prepareStatement);
            JdbcProfiler.getInstance().stop(JdbcProfiler.OP_PREPARE, ref);
            return pstmt;

        }
        return prepareStatement;
    }
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#reconfigureDB2Connection(java.util.Properties)
	 */
	@Override
	public void reconfigureDB2Connection(Properties arg0) throws SQLException {
		
		connection.reconfigureDB2Connection(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#registerDB2XmlDtd(java.lang.String[], java.lang.String[], java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void registerDB2XmlDtd(String[] arg0, String[] arg1, String arg2,
			String arg3, String arg4) throws SQLException {
		
		connection.registerDB2XmlDtd(arg0, arg1, arg2, arg3, arg4);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#registerDB2XmlDtd(java.lang.String[], java.lang.String[], java.lang.String, java.lang.String, java.io.InputStream, int)
	 */
	@Override
	public void registerDB2XmlDtd(String[] arg0, String[] arg1, String arg2,
			String arg3, InputStream arg4, int arg5) throws SQLException {
		
		connection.registerDB2XmlDtd(arg0, arg1, arg2, arg3, arg4, arg5);
		
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#registerDB2XmlExternalEntity(java.lang.String[], java.lang.String[], java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void registerDB2XmlExternalEntity(String[] arg0, String[] arg1,
			String arg2, String arg3, String arg4) throws SQLException {
		
		connection.registerDB2XmlExternalEntity(arg0, arg1, arg2, arg3, arg4);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#registerDB2XmlExternalEntity(java.lang.String[], java.lang.String[], java.lang.String, java.lang.String, java.io.InputStream, int)
	 */
	@Override
	public void registerDB2XmlExternalEntity(String[] arg0, String[] arg1,
			String arg2, String arg3, InputStream arg4, int arg5)
			throws SQLException {
		
		connection.registerDB2XmlExternalEntity(arg0, arg1, arg2, arg3, arg4, arg5);
	}

	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#registerDB2XmlSchema(java.lang.String[], java.lang.String[], java.lang.String[], java.lang.String[], java.lang.String[], java.lang.String, boolean)
	 */
	@Override
	public void registerDB2XmlSchema(String[] arg0, String[] arg1,
			String[] arg2, String[] arg3, String[] arg4, String arg5,
			boolean arg6) throws SQLException {
		
		connection.registerDB2XmlSchema(arg0, arg1, arg2, arg3, arg4, arg5, arg6);
	}

	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#registerDB2XmlSchema(java.lang.String[], java.lang.String[], java.lang.String[], java.io.InputStream[], int[], java.io.InputStream[], int[], java.io.InputStream, int, boolean)
	 */
	@Override
	public void registerDB2XmlSchema(String[] arg0, String[] arg1,
			String[] arg2, InputStream[] arg3, int[] arg4, InputStream[] arg5,
			int[] arg6, InputStream arg7, int arg8, boolean arg9)
			throws SQLException {
		
		connection.registerDB2XmlSchema(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#removeDB2JavaStoredProcedure(java.lang.String)
	 */
	@Override
	public void removeDB2JavaStoredProcedure(String arg0) throws SQLException {
		
		connection.removeDB2JavaStoredProcedure(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#replaceDB2JavaStoredProcedure(java.io.InputStream, int, java.lang.String)
	 */
	@Override
	public void replaceDB2JavaStoredProcedure(InputStream arg0, int arg1,
			String arg2) throws SQLException {
		
		connection.replaceDB2JavaStoredProcedure(arg0, arg1, arg2);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#resetDB2Connection()
	 */
	@Override
	public void resetDB2Connection() throws SQLException {
		
		connection.resetDB2Connection();
	}

	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#resetDB2Connection(java.lang.String, java.lang.String)
	 */
	@Override
	public void resetDB2Connection(String arg0, String arg1)
			throws SQLException {
		
		connection.resetDB2Connection(arg0, arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#reuseDB2Connection(java.util.Properties)
	 */
	@Override
	public void reuseDB2Connection(Properties arg0) throws SQLException {
		
		connection.reuseDB2Connection(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#reuseDB2Connection(org.ietf.jgss.GSSCredential, java.util.Properties)
	 */
	@Override
	public void reuseDB2Connection(GSSCredential arg0, Properties arg1)
			throws SQLException {
		connection.reuseDB2Connection(arg0, arg1);
		
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#reuseDB2Connection(java.lang.String, java.lang.String, java.util.Properties)
	 */
	@Override
	public void reuseDB2Connection(String arg0, String arg1, Properties arg2)
			throws SQLException {
		
		connection.reuseDB2Connection(arg0, arg1, arg2);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#reuseDB2Connection(byte[], org.ietf.jgss.GSSCredential, java.lang.String, byte[], java.lang.String, java.util.Properties)
	 */
	@Override
	public void reuseDB2Connection(byte[] arg0, GSSCredential arg1,
			String arg2, byte[] arg3, String arg4, Properties arg5)
			throws SQLException {
		
		connection.reuseDB2Connection(arg0, arg1, arg2, arg3, arg4, arg5);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#reuseDB2Connection(byte[], java.lang.String, java.lang.String, java.lang.String, byte[], java.lang.String, java.util.Properties)
	 */
	@Override
	public void reuseDB2Connection(byte[] arg0, String arg1, String arg2,
			String arg3, byte[] arg4, String arg5, Properties arg6)
			throws SQLException {
		
		connection.reuseDB2Connection(arg0, arg1, arg2, arg3, arg4, arg5, arg6);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setDB2ClientAccountingInformation(java.lang.String)
	 */
	@Deprecated
	@Override
	public void setDB2ClientAccountingInformation(String arg0)
			throws SQLException {
		
		//$ANALYSIS-IGNORE
		connection.setDB2ClientAccountingInformation(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setDB2ClientApplicationInformation(java.lang.String)
	 */
	@Deprecated
	@Override
	public void setDB2ClientApplicationInformation(String arg0)
			throws SQLException {
		//$ANALYSIS-IGNORE
		connection.setDB2ClientApplicationInformation(arg0);
		
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setDB2ClientDebugInfo(java.lang.String)
	 */
	@Override
	public void setDB2ClientDebugInfo(String arg0) throws SQLException {
		connection.setDB2ClientDebugInfo(arg0);
		
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setDB2ClientDebugInfo(java.lang.String, java.lang.String)
	 */
	@Override
	public void setDB2ClientDebugInfo(String arg0, String arg1)
			throws SQLException {
		
		connection.setDB2ClientDebugInfo(arg0, arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setDB2ClientProgramId(java.lang.String)
	 */
	@Override
	public void setDB2ClientProgramId(String arg0) throws SQLException {
		
		connection.setDB2ClientProgramId(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setDB2ClientUser(java.lang.String)
	 */
	@Deprecated
	@Override
	public void setDB2ClientUser(String arg0) throws SQLException {
		
		//$ANALYSIS-IGNORE
		connection.setDB2ClientUser(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setDB2ClientWorkstation(java.lang.String)
	 */
	@Deprecated
	@Override
	public void setDB2ClientWorkstation(String arg0) throws SQLException {
		
		//$ANALYSIS-IGNORE
		connection.setDB2ClientWorkstation(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setDB2CurrentPackagePath(java.lang.String)
	 */
	@Override
	public void setDB2CurrentPackagePath(String arg0) throws SQLException {
		
		connection.setDB2CurrentPackagePath(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setDB2CurrentPackageSet(java.lang.String)
	 */
	@Override
	public void setDB2CurrentPackageSet(String arg0) throws SQLException {
		
		connection.setDB2CurrentPackageSet(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setDB2eWLMCorrelator(byte[])
	 */
	@Override
	public void setDB2eWLMCorrelator(byte[] arg0) throws SQLException {
		
		connection.setDB2eWLMCorrelator(arg0);
		
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setDBProgressiveStreaming(int)
	 */
	@Override
	public void setDBProgressiveStreaming(int arg0) throws SQLException {
		connection.setDBProgressiveStreaming(arg0);
		
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setJCCLogWriter(java.io.PrintWriter)
	 */
	@Override
	public void setJCCLogWriter(PrintWriter arg0) throws SQLException {
		
		connection.setJCCLogWriter(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setJCCLogWriter(java.io.PrintWriter, int)
	 */
	@Override
	public void setJCCLogWriter(PrintWriter arg0, int arg1) throws SQLException {
		
		connection.setJCCLogWriter(arg0, arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setJccLogWriter(java.io.PrintWriter)
	 */
	@Override
	public void setJccLogWriter(PrintWriter arg0) throws SQLException {
		
		connection.setJccLogWriter(arg0);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setJccLogWriter(java.io.PrintWriter, int)
	 */
	@Override
	public void setJccLogWriter(PrintWriter arg0, int arg1) throws SQLException {
		
		connection.setJccLogWriter(arg0, arg1);
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setJccLogWriter(java.lang.String, boolean, int)
	 */
	@Override
	public void setJccLogWriter(String arg0, boolean arg1, int arg2)
			throws SQLException {
		connection.setJccLogWriter(arg0, arg1, arg2);
		
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#setMaxRowsetSize(int)
	 */
//	@Override
//	public void setMaxRowsetSize(int arg0) throws SQLException {
//		
//		connection.setMaxRowsetSize(arg0);
//	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#unsetJccLogWriter()
	 */
	@Override
	public void unsetJccLogWriter() throws SQLException {
		
		connection.unsetJccLogWriter();
	}
	/*
	 * (non-Javadoc)
	 * @see com.ibm.db2.jcc.DB2Connection#updateDB2XmlSchema(java.lang.String, java.lang.String, java.lang.String, java.lang.String, boolean)
	 */
	@Override
	public void updateDB2XmlSchema(String arg0, String arg1, String arg2,
			String arg3, boolean arg4) throws SQLException {
		
		connection.updateDB2XmlSchema(arg0, arg1, arg2, arg3, arg4);
	}
	@Override
	public String getDB2ClientCorrelationToken() throws SQLException {
		return connection.getDB2ClientCorrelationToken();
	}
	@Override
	public int getDBConcurrentAccessResolution() throws SQLException {
		return connection.getDBConcurrentAccessResolution();
	}
	@Override
	public int getDBStatementConcentrator() throws SQLException {
		return connection.getDBStatementConcentrator();
	}
	@Override
	public Properties getJccSpecialRegisterProperties() throws SQLException {
		return connection.getJccSpecialRegisterProperties();
	}
	@Override
	public int getMaxRowsetSize() throws SQLException {
		return connection.getMaxRowsetSize();
	}
	@Override
	public boolean getSavePointUniqueOption() throws SQLException {
		return connection.getSavePointUniqueOption();
	}
	@Override
	public void setDB2ClientCorrelationToken(String arg0) throws SQLException {
		connection.setDB2ClientCorrelationToken(arg0);
	}
	@Override
	public void setDBConcurrentAccessResolution(int arg0) throws SQLException {
		connection.setDBConcurrentAccessResolution(arg0);
	}
	@Override
	public void setDBStatementConcentrator(int arg0) throws SQLException {
		connection.setDBStatementConcentrator(arg0);
		
	}
	@Override
	public void setGlobalSessionVariable(String arg0, String arg1)
			throws SQLException {
		connection.setGlobalSessionVariable(arg0, arg1);
		
	}

	@Override
	public LoadResult zLoad(String s, String s1, String s2) throws SQLException {
		try {
			// 파일을 읽기 위한 준비
			Path filePath = Paths.get(s1);
			List<String> allLines = Files.readAllLines(filePath, StandardCharsets.UTF_8);

			// 데이터베이스 테이블에 데이터를 삽입하기 위한 SQL 준비
			String sql = "INSERT INTO " + s + " VALUES (?)"; // 간단한 예시
			try (PreparedStatement pstmt = this.connection.prepareStatement(sql)) {
				for (String line : allLines) {
					pstmt.setString(1, line); // 각 줄을 테이블에 삽입
					pstmt.executeUpdate();
				}
			}

			// 성공적으로 데이터 로드
			return new LoadResult(0, "Data loaded successfully into " + s);

		} catch (IOException e) {
			// 파일 읽기 오류 처리
			return new LoadResult(1, "Failed to read data file: " + e.getMessage());
		} catch (SQLException e) {
			// 데이터베이스 오류 처리
			return new LoadResult(1, "Failed to load data into database: " + e.getMessage());
		}
	}

	@Override
	public LoadResult zLoad(String s, String s1) throws SQLException {
		try {
			List<String> lines = Files.lines(Paths.get(s1))
					.collect(Collectors.toList());

			// 간단한 예시로, 파일의 각 줄을 데이터베이스 테이블의 단일 컬럼에 삽입
			String sql = "INSERT INTO " + s + " (your_column_name) VALUES (?)";
			try (PreparedStatement pstmt = this.connection.prepareStatement(sql)) {
				for (String line : lines) {
					pstmt.setString(1, line);
					pstmt.executeUpdate();
				}
			}

			// 데이터 로드 성공
			return new LoadResult(0, "Data loaded successfully into " + s);

		} catch (Exception e) {
			// 데이터 로드 중 오류 발생
			return new LoadResult(1, "Failed to load data: " + e.getMessage());
		}
	}

	@Override
	public LoadResult zLoad(String s, boolean b, String s1, String s2) throws SQLException {
		try {
			// 파일 읽기 및 파싱 로직
			Path path = Paths.get(s1);
			List<String> lines = Files.readAllLines(path);

			// 데이터베이스 테이블에 로드
			String sql = "INSERT INTO " + s + " VALUES (?)"; // 단순화된 예시
			try (PreparedStatement pstmt = this.connection.prepareStatement(sql)) {
				for (String line : lines) {
					pstmt.setString(1, line); // 파일의 각 줄을 테이블에 삽입
					pstmt.executeUpdate();
				}
			}

			// 로드 성공 메시지 반환
			return new LoadResult(0, "Data loaded successfully into " + s);

		} catch (IOException e) {
			return new LoadResult(1, "Failed to read data file: " + e.getMessage());
		} catch (SQLException e) {
			return new LoadResult(1, "Failed to load data into database: " + e.getMessage());
		}
	}

	@Override
	public LoadResult zLoad(String s, boolean b, String s1) throws SQLException {
		try {
			// 파일 읽기 및 파싱 로직
			Path path = Paths.get(s1);
			List<String> lines = Files.readAllLines(path);

			// 데이터베이스 테이블에 로드
			String sql = "INSERT INTO " + s1 + " VALUES (?)"; // 단순화된 예시
			try (PreparedStatement pstmt = this.connection.prepareStatement(sql)) {
				for (String line : lines) {
					pstmt.setString(1, line); // 파일의 각 줄을 테이블에 삽입
					pstmt.executeUpdate();
				}
			}

			// 로드 성공 메시지 반환
			return new LoadResult(0, "Data loaded successfully into " + s);

		} catch (IOException e) {
			return new LoadResult(1, "Failed to read data file: " + e.getMessage());
		} catch (SQLException e) {
			return new LoadResult(1, "Failed to load data into database: " + e.getMessage());
		}
	}

	@Override
	public void setMaxRowsetSize(int arg0) throws SQLException {
		connection.setMaxRowsetSize(arg0);
		
	}
	@Override
	public void setSavePointUniqueOption(boolean arg0) throws SQLException {
		connection.setSavePointUniqueOption(arg0);
	}

}
