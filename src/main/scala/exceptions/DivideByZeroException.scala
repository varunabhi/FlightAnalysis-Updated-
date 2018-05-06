package exceptions

class DivideByZeroException(str:String) extends RuntimeException{

  private val serialVersionUID = -897856973823710492L

  override def getMessage: String = {
    str
  }
}
