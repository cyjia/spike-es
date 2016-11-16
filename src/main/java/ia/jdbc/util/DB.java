package ia.jdbc.util;

import java.sql.*;
import java.util.function.Consumer;

public class DB {
    private String connectionUrl;
    private String userName;
    private String password;

    public DB() {
        connectionUrl = System.getenv("url");
        userName = System.getenv("user");
        password = System.getenv("password");
    }

    public void withConnection(Consumer<Connection> consumer) throws Exception {
        Connection connection = getConnection();
        try {
            consumer.accept(connection);
        } finally {
            connection.close();
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(connectionUrl, userName, password);
    }

    public void query(String sql, Consumer<ResultSet> tConsumer) {
        try {
            withConnection(conn -> {
                Statement statement = null;
                try {
                    statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(sql);
                    tConsumer.accept(rs);
                } catch (SQLException e) {
                    e.printStackTrace();
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
            e.printStackTrace();
        }
    }
}
