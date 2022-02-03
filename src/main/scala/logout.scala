import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
class logout(hiveCtx:HiveContext) {
    var a=new mai()
  def hk():Unit = {
      val driver = "com.mysql.cj.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/p1" // Modify for whatever port you are running your DB on
        val username = "root"
        val password = "H@rdik480" // Update to include your password
        var connection:Connection = null

        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
       a.rr(connection,hiveCtx)
  }
}
