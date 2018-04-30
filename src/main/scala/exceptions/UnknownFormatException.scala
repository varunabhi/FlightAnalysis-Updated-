package exceptions

class UnknownFormatException(message:String) extends RuntimeException{

  def this(){
    this("Error in String Parsing")
  }
  
  override def getMessage: String ={
    message
  }
}
