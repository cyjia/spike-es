package ia.jdbc.util;

import java.sql.*;
import java.util.function.Function;

public class DB {
    private String connectionUrl;
    private String userName;
    private String password;

    public DB() {
        connectionUrl = System.getenv("url");
        userName = System.getenv("user");
        password = System.getenv("password");
    }

    public <T> T withConnection(Function<Connection, T> function) throws Exception {
        Connection connection = getConnection();
        try {
            return function.apply(connection);
        } finally {
            connection.close();
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(connectionUrl, userName, password);
    }

    public <T> T query(String sql, Function<ResultSet, T> function) {
        try {
            return withConnection(conn -> {
                Statement statement = null;
                try {
                    statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(sql);
                    return function.apply(rs);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } finally {
                    if (statement != null) {
                        try {
                            statement.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
