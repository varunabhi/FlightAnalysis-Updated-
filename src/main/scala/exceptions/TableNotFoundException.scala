package exceptions

case class TableNotFoundException(tableName:String) extends RuntimeException{

  def this(){
    this("table not found")
  }

  override def getMessage: String = {
    tableName +" not found"
  }
}
