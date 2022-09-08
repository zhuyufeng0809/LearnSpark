package conf

import java.util.Properties

object MysqlInstance extends Enumeration {
  type MysqlInstance = Instance

  protected case class Instance(url: String, username: String, password: String)
      extends super.Val {
    private val prefix = "jdbc:mysql://"

    def getUrl: String = prefix + url

    def getProperties: Properties = {
      val properties = new Properties()
      properties.put("user", username)
      properties.put("password", password)
      properties
    }
  }

  val POLAR: MysqlInstance = Instance(
    "",
    "",
    ""
  )

  val ADB: MysqlInstance = Instance(
    "",
    "",
    ""
  )
}
