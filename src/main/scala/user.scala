import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

class user {
  var scanner = new Scanner(System.in)


def user_op(hiveCtx:HiveContext):Unit = {
    println()
            var a=0
            do{
                println("Please select the below the option")
                println("(1)top 10 sales list by global")
                println("(2)top 10 sales list by North America")
                println("(3)top 10 sales list by Europ")
                println("(4)top 10 sales list by Japan")
                println("(5) List bt Platfrom")
                println("(6) List bt Genre")
                println("(7) List bt Publiser")
                println("(8) Find by Genre..")
                println("(9) Find by year")
                println("(10)Find with platfrom")
                println("(11) Logout")
                println("please select the option........")
                a=scanner.nextInt()
                scanner.nextLine()
            if(!((a>=1)&&(a<=11))){
                println()
                println("Please select the number between 1 to 11 ....")
                println()
            }

            }while(!((a>=1)&&(a<=11)))


           

            var s="NA_Sales"
            //  top10Global(hiveCtx,s)
            a match {
    case 1  => top10Global(hiveCtx,"Global_sales")
    case 2  => top10Global(hiveCtx,"NA_Sales")
    case 3  => top10Global(hiveCtx,"EU_Sales")
    case 4  => top10Global(hiveCtx,"JP_Sales")
    case 5  => show(hiveCtx,"platform")
    case 6  => show(hiveCtx,"Genre")
    case 7  => show(hiveCtx,"publisher")
    case 8  => findGenre(hiveCtx)
    case 9  => findYear(hiveCtx)
    case 10 => findPlatfrom(hiveCtx)
    case 11 =>logout(hiveCtx)
    case default => println("Unexpected case: " )
}




}

def top10Global(hiveCtx:HiveContext,s:String): Unit = {
    
        val result = hiveCtx.sql("select * from data3 order by "+s+ " desc limit 10 ")
         println("Please wait loading a data.............")
         println()
        result.show()

        result.write.mode("overwrite").csv("results/top10Global")
        println("Press the Enter..........")
        scanner.nextLine()
        user_op(hiveCtx)
        

    }

def show(hiveCtx:HiveContext,s:String): Unit = {

    val result = hiveCtx.sql("select distinct "+s+" from data3 order by "+s+" ASC")
     println("Please wait loading a data.............")
     println()
     
        result.show(200,300)

       result.write.mode("overwrite").csv("results/show")
        println("Press the Enter..........")
        scanner.nextLine()
        user_op(hiveCtx)

}



def findGenre(hiveCtx:HiveContext): Unit = {
    try{
        println("Enter the Genre ")
        var g =scanner.nextLine().trim()
        var ge = g.toLowerCase.capitalize
        scanner.nextLine()

       val result = hiveCtx.sql("SELECT * FROM data3 where genre=" +"\"" +ge+"\"") 
       //var a=result.count
       //coun(a.toInt)
         println("Please wait loading a data.............")
        result.show(100)
        result.write.mode("overwrite").csv("results/findGenre")
        println("Press the Enter..........")
        scanner.nextLine()
        user_op(hiveCtx)
    }catch{
        case e:Exception =>{
            println(e)
            findGenre(hiveCtx)
        }
    }
    }

def coun(a:Int):Unit = {
    if(a==0){
         throw new MyException("Sorry this data its not avalible in our System,So can you try again ")
    }
}

def findYear(hiveCtx:HiveContext): Unit = {
    
    try{ 

    var s = "Enter the Year..."
    var check = new admin()
    var yy = check.userid(s)
        
        var ye =yy.toInt
        scanner.nextLine()
        
       val result = hiveCtx.sql("SELECT * FROM data2 where year= " +ye+" ")
       var a=result.count

       coun(a.toInt) 
        println("Please wait loading a data.............")
    
        
        result.show(300)
       result.write.mode("overwrite").csv("results/findYear")
        println("Press the Enter..........")
        scanner.nextLine()
        user_op(hiveCtx)
    }catch{
        case e:Exception =>{
            println(e)
            findYear(hiveCtx)
        }
    }
    }


def findPlatfrom(hiveCtx:HiveContext): Unit = {
        println("Enter the Platform ")
        var g =scanner.nextLine().trim()
        var ge = g.toUpperCase
        scanner.nextLine()

       val result = hiveCtx.sql("SELECT * FROM data3 where platform=" +"\"" +ge+"\" ") 
        println("Please wait loading a data.............")
        result.show(100)
        result.write.mode("overwrite").csv("results/findPlatfrom")
        println("Press the Enter..........")
        scanner.nextLine()
        user_op(hiveCtx)
    }
var dis = new mai()
def logout(hiveCtx:HiveContext):Unit = {
    println("Thank you for sign up")
    println()
    var lo = new logout(hiveCtx)
    lo.hk()
    
    
}





}
