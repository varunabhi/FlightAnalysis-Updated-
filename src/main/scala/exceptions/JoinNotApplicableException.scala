package exceptions

class JoinNotApplicableException(str:String) extends RuntimeException{

  private val serialVersionUID = -897856973823710492L

  def this(){
    this("Can't Join With This Column Name")
  }
  override def getMessage: String = {
    str
  }
}
