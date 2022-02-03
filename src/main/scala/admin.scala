import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
class admin {
  
var sc =new Scanner(System.in)

def admin_part(connection: Connection,hiveCtx:HiveContext):Unit={
    println()
    var gg = new user()
   
            var a=0
            do{
                println("please select the below the option")
                println("(1)ADD a new user")
                println("(2) Delete a user")
                println("(3) show the list of user")
                println("(4)Go to user option ")
                println("(5) logout ")
                println("please select the option........")
                a=sc.nextInt()
                sc.nextLine()


            }while(a != 1 && a!=2 && a!=3 && a!=4 && a!=5)
            if(a==1){ new_user(connection,hiveCtx)}
            else if(a==2){delete_User(connection,hiveCtx)}
            else if(a==3){show_user_list(connection,hiveCtx)}
            else if(a==4){gg.user_op(hiveCtx)}
            else if(a==5){ logo(hiveCtx)}
            else{}
           
}
    
    
    
    def logo(hiveCtx:HiveContext):Unit = {
        var lo =new logout(hiveCtx)
    println("Thank you for sign out")
    println()
    lo.hk()
    
    
    
}
    
    
    
    

    def new_user(connection: Connection,hiveCtx:HiveContext):Unit={
        var s1= "Please Enter the name:"
        var first_name = name(s1)
        sc.nextLine()

        var s2 = "plesase enter the last name:"
        var last_name = name(s2)
        sc.nextLine()
        var user_id =""
        do{
        println("please enter user_id: ")
        user_id = sc.nextLine().trim()
        sc.nextLine()
        if(user_id.length==0){println("Please enter the user id")}
        }while(user_id.length==0)

        var password1 =""
        do{
         println("Enter the password : ")
        password1 = sc.nextLine().trim()
        sc.nextLine()
        if(password1.length==0){println("Please enter the user id")}
        }while(password1.length==0)
   
    

    var result =true
    var matchPassword=""   
    
 do{

    println("Enter the again password : ")
    matchPassword = sc.nextLine()
    sc.nextLine()
     
    result = password1.equals(matchPassword)

     if(result == false){
         println("Password Not match ")}
        
    }while(result  == false)

        val statement = connection.createStatement()
            var query = "INSERT INTO  user_accounts (first_name,last_name,username,password) values ('" + first_name +
           "','" + last_name +"','"+ user_id + "','"+matchPassword + "')"
            //var query= "Insert into user_accounts (first_name,last_name,username,password) values("irbaz","pathan","irbaz9537","343434");"';"" +"" +
              //println(query)
            val resultSet = statement.executeUpdate(query)
            sc.nextLine()

        println("Congratulation you successfully added the user ")
        println()
        println("************************")
        println()
        admin_part(connection,hiveCtx)





    }

    def delete_User(connection: Connection,hiveCtx:HiveContext):Unit={
           var bl = false
           var g=0
           var id =0
           while(bl==false){
            println()
            var s = "Please Enter User ID:"
            var i =userid(s)
            id =i.toInt;
           
            val statement = connection.createStatement()
           
            val resultSet = statement.executeQuery("SELECT COUNT(*) FROM user_accounts WHERE id="+id+";")
            while ( resultSet.next() ) {
                if (resultSet.getString(1) == "1") {
                    g=1
                    
                }
            }
            if(g==1){bl=true}
            else{bl=false
            println("Sorry this user id its not available in data,So can you try again..... ")
            
            }
             


        }


         val statement1 = connection.createStatement()
            var query = "delete from user_accounts where id=" + id +";"
            //var query= "Insert into user_accounts (first_name,last_name,username,password) values("irbaz","pathan","irbaz9537","343434");"';"" +"" +
              //println(query)
            val resultSet = statement1.executeUpdate(query)

            println("Congratulation you successfully Delete the user ")
            println()
        println("************************")
        println()
        admin_part(connection,hiveCtx)

    }
      def userid(s:String):String={
        
        try{ 
                println()
                println(s)
                var firstname =sc.nextLine()
                sc.nextLine()
                vali(firstname)
                return firstname
                
            }
        catch{
                    case e: MyException => {println(e)
                   userid(s)}; 
                    
            }
    }

    //check the datavalid data in string
    def vali(s:String):Unit=
    {
        var asd =s.toUpperCase()
          if(s.length==0){
                throw new MyException("Invalid data Enter(Please enter the Digit) ")
            }
        if(s.isEmpty){
                throw new MyException("Invalid data Enter(Please enter the Digit) ")
            }
        for(i <- 0 to asd.length-1 by 1)
        {
            if(!(asd(i)>='0'&&asd(i)<='9')){
            throw new MyException("Invalid data Enter(Please enter the Digit) ")
            }
        }

     
   
    }
    def show_user_list(connection: Connection,hiveCtx:HiveContext):Unit={

           val statement1 = connection.createStatement()
            var query = "select * from user_accounts ;"
            //var query= "Insert into user_accounts (first_name,last_name,username,password) values("irbaz","pathan","irbaz9537","343434");"';"" +"" +
              //println(query)
            val rs = statement1.executeQuery(query)
             println("****************************************")
              println("****************************************")
              printf("%1s %10s %20s %20s", "id", "First_name","Last_name","User_id");
              println()
             while (rs.next()) {
           
              printf("%1s %10s %20s %20s", rs.getInt(1), rs.getString(2),rs.getString(3),rs.getString(4));
           //println(rs.getInt(1)+"       "+rs.getString(2)+"         "+rs.getString(3)+"           "+rs.getString(4))
        println() }
         println("****************************************")
          println("****************************************")
        println()
        println("Press the enter........ ")
        sc.nextLine()
        admin_part(connection,hiveCtx)
    
    
    





    }
    


def name(s:String):String={
        
        try{ 
                println()
                println(s)
                var firstname =sc.nextLine()
               
                sc.nextLine()
                validate(firstname)
                return firstname
                
            }
        catch{
                    case e: MyException => {println(e)
                   name(s)}; 
                    
            }
    }

    //check the datavalid data in string
    def validate(s:String):Unit=
    {
        var asd =s.toUpperCase()
        if(s.length==0){
                throw new MyException("Invalid data Enter(Please enter the Alphabet) ")
            }
        if(s.isEmpty){
                throw new MyException("Invalid data Enter(Please enter the Alphabet) ")
            }
        for(i <- 0 to asd.length-1 by 1)
        { 
            
            
            if(!(asd(i)>='A'&&asd(i)<='Z')){
            throw new MyException("Invalid data Enter(Please enter the Alphabet) ")
            }
        }

     
   
    }

















}
