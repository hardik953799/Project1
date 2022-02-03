import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

class update(s:Int){

var sc = new Scanner(System.in)
    def upda(us:Int):Unit={
    var uu=us
   
    var a=0
    do{
        println("************************************")
        println("Update the Information! ")
        println("(1)first name")
        println("(2)last name")
        println("(3)Phone Number")
        println("(4) Address")
        println("(5) Password")
        println("(6) Exit")
        println("************************************")
        a = sc.nextInt()
       

    }while(a<=0 && a>8)
    
   val url = "jdbc:mysql://localhost:3306/sqlexamples"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "root"
    val password = "H@rdik480"
    var connection:Connection = null
    try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement()

        if(a==1){
            println("************************************")
            println("Enter the first name: ")
            var firstname =sc.next()
            sc.nextLine()
            var query = "UPDATE persons SET FirstName = '" + firstname +
           "' Where UserId="+ uu
             val rs = statement.executeUpdate(query)
              println("************************************")
              println("Congratulation you are successfully change your name")
               println("************************************")

        }
        else if(a==2){
            println("Enter the last Name: ")
            var lastname =sc.next()
            sc.nextLine()
            var query = "UPDATE persons SET LastName = '" + lastname +
           "' Where UserId="+ uu

           //println(query)
            val rs = statement.executeUpdate(query)
             println("************************************")
              println("Congratulation you are successfully change your name")
               println("************************************")

        }
       else if(a==3){
            println("Enter the phone-Number: ")
            var phoneNumber =sc.nextLong()
            sc.nextLine()
            var query = "UPDATE persons SET PhoneNumber = '" + phoneNumber +
           "' Where UserId="+ uu

           //println(query)
            val rs = statement.executeUpdate(query)
             println("************************************")
              println("Congratulation you are successfully change your Phone Number")
               println("************************************")

        }
        else if(a==4){
                println("Enter the address : ")
                var address = sc.next()
                sc.nextLine()

                println("Enter the city : ")
                var city = sc.nextLine()
                sc.nextLine()

                println("Enter the State : ")
                var state = sc.nextLine()

                println("Enter the Zip Code : ")
                var zip = sc.nextInt()
                sc.nextLine()
                

            var query = "UPDATE persons SET Address = '" + address +
                        "',"+ "City = '" + city +
                        "'," +"state = '" + state +
                        "',"+ "zip = '" + zip +
                        "'"+ "Where UserId"+ uu

                        


           println(query)
            var rs = statement.executeUpdate(query)
             println("************************************")
              println("Congratulation you are successfully change your address")
               println("************************************")

            

        }
        else if(a==5){
            println("Enter the password: ")
            var password =sc.next()
            sc.nextLine()
            var query = "UPDATE persons SET Password = '" + password +
           "' Where UserId="+ uu

           println(query)
            val rs = statement.executeUpdate(query)
             println("************************************")
              println("Congratulation you are successfully change your Password")
               println("************************************")

        }
        else{
             //afl(uu) 
        }




    
        
    } catch {
        case e: Exception => e.printStackTrace
    }
    connection.close()
   upda(uu)
    

}

// def afl(u:Int):Unit={
//     var af = new afterLogin(u)
    
//        af.after()
// }



}