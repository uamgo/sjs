import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TestBatch {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
//        String sql ="upsert into t8  SELECT nvl(a.id,b.id),nvl(a.num,0)+nvl(b.num,'0') from t8 a right join (values (?,?)) b(id, num) on a.id = b.id";
        String sql ="UPDATE t8  SELECT nvl(a.id,b.id),nvl(a.num,0)+nvl(b.num,'0') from t8 a right join (values (?,?)) b(id, num) on a.id = b.id";
        Class.forName("org.trafodion.jdbc.t4.T4Driver");
        String url = "jdbc:t4jdbc://10.9.0.220:23400/:";
        Connection conn = DriverManager.getConnection(url, "trafodion", "traf123");

        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1,"2");
        ps.setString(2,"10");
        ps.addBatch();
        ps.setString(1,"2");
        ps.setString(2,"20");
        ps.addBatch();
        ps.executeBatch();
        ps.close();
        conn.close();
    }
}
