package exceptions

class ColumnNotFoundException extends RuntimeException{

  private val serialVersionUID = -897856973823710492L

  override def getMessage: String ={
    "Column name is not Accessible"
  }
}
