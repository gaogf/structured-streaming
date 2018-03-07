package com.gaogf.dataloader

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._

import scala.util.Try

/**
  * Created by pactera on 2018/3/6.
  */
object JdbcUtils extends Logging {

  val  mode = SaveMode.Append


  def jdbc(url: String,df: DataFrame, table: String, connectionProperties: Properties): Unit = {
    val props = new Properties()
    props.putAll(connectionProperties)
    val conn = JdbcUtils.createConnection(url, props)

    try {
      var tableExists = JdbcUtils.tableExists(conn, table)

      if (mode == SaveMode.Ignore && tableExists) {
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }

      if (mode == SaveMode.Overwrite && tableExists) {
        JdbcUtils.dropTable(conn, table)
        tableExists = false
      }

      // Create the table if the table didn't exist.
      if (!tableExists) {
        val schema = JdbcUtils.schemaString(df, url)
        val sql = s"CREATE TABLE $table ($schema)"
        conn.prepareStatement(sql).executeUpdate()
      }
    } finally {
      conn.close()
    }

    JdbcUtils.saveTable(df, url, table, props)
  }

  /**
    * Establishes a JDBC connection.
    */
  def createConnection(url: String, connectionProperties: Properties): Connection = {
    com.gaogf.dataloader.JDBCRDD.getConnector(connectionProperties.getProperty("driver"), url, connectionProperties)()
  }

  /**
    * Returns true if the table already exists in the JDBC database.
    */
  def tableExists(conn: Connection, table: String): Boolean = {
    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems, considering "table" could also include the database name.
    Try(conn.prepareStatement(s"SELECT 1 FROM $table LIMIT 1").executeQuery().next()).isSuccess
  }

  /**
    * Drops a table from the JDBC database.
    */
  def dropTable(conn: Connection, table: String): Unit = {
    conn.prepareStatement(s"DROP TABLE $table").executeUpdate()
  }

  /**
    * Returns a PreparedStatement that inserts a row into table via conn.
    */
  def insertStatement(conn: Connection, table: String, rddSchema: StructType): PreparedStatement = {
    val fields = rddSchema.fields
    val fieldsSql = new StringBuilder(s"(")
    var i=0;
    for(f <- fields){
      fieldsSql.append(f.name)

      if(i==fields.length-1){
        fieldsSql.append(")")
      }else{
        fieldsSql.append(",")
      }
      i+=1
    }

    val sql = new StringBuilder(s"INSERT INTO $table ")
    sql.append(fieldsSql.toString())
    sql.append(" VALUES (")
    var fieldsLeft = rddSchema.fields.length
    while (fieldsLeft > 0) {
      sql.append("?")
      if (fieldsLeft > 1) sql.append(", ") else sql.append(")")
      fieldsLeft = fieldsLeft - 1
    }
    //println(sql.toString())
    conn.prepareStatement(sql.toString())
  }

  /**
    * Saves a partition of a DataFrame to the JDBC database.  This is done in
    * a single database transaction in order to avoid repeatedly inserting
    * data as much as possible.
    *
    * It is still theoretically possible for rows in a DataFrame to be
    * inserted into the database more than once if a stage somehow fails after
    * the commit occurs but before the stage can return successfully.
    *
    * This is not a closure inside saveTable() because apparently cosmetic
    * implementation changes elsewhere might easily render such a closure
    * non-Serializable.  Instead, we explicitly close over all variables that
    * are used.
    */
  def savePartition(
                     getConnection: () => Connection,
                     table: String,
                     iterator: Iterator[Row],
                     rddSchema: StructType,
                     nullTypes: Array[Int]): Iterator[Byte] = {
    val conn = getConnection()
    var committed = false
    try {
      conn.setAutoCommit(false) // Everything in the same db transaction.
      val stmt = insertStatement(conn, table, rddSchema)
      try {
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = rddSchema.fields.length
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              rddSchema.fields(i).dataType match {
                case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                case LongType => stmt.setLong(i + 1, row.getLong(i))
                case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                case ShortType => stmt.setInt(i + 1, row.getShort(i))
                case ByteType => stmt.setInt(i + 1, row.getByte(i))
                case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                case StringType => stmt.setString(i + 1, row.getString(i))
                case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
                case _ => throw new IllegalArgumentException(
                  s"Can't translate non-null value for field $i")
              }
            }
            i = i + 1
          }
          stmt.executeUpdate()
        }
      } finally {
        stmt.close()
      }
      conn.commit()
      committed = true
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        conn.rollback()
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
    Array[Byte]().iterator
  }
  /**
    * Compute the schema string for this RDD.
    */
  def schemaString(df: DataFrame, url: String): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    df.schema.fields foreach { field => {
      val name = field.name
      val typ: String =
        dialect.getJDBCType(field.dataType).map(_.databaseTypeDefinition).getOrElse(
          field.dataType match {
            case IntegerType => "INTEGER"
            case LongType => "BIGINT"
            case DoubleType => "DOUBLE PRECISION"
            case FloatType => "REAL"
            case ShortType => "INTEGER"
            case ByteType => "BYTE"
            case BooleanType => "BIT(1)"
            case StringType => "TEXT"
            case BinaryType => "BLOB"
            case TimestampType => "TIMESTAMP"
            case DateType => "DATE"
            case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
            case _ => throw new IllegalArgumentException(s"Don't know how to save $field to JDBC")
          })
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }}
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
    * Saves the RDD to the database in a single transaction.
    */
  def saveTable(
                 df: DataFrame,
                 url: String,
                 table: String,
                 properties: Properties = new Properties()) {
    val dialect = JdbcDialects.get(url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      dialect.getJDBCType(field.dataType).map(_.jdbcNullType).getOrElse(
        field.dataType match {
          case IntegerType => java.sql.Types.INTEGER
          case LongType => java.sql.Types.BIGINT
          case DoubleType => java.sql.Types.DOUBLE
          case FloatType => java.sql.Types.REAL
          case ShortType => java.sql.Types.INTEGER
          case ByteType => java.sql.Types.INTEGER
          case BooleanType => java.sql.Types.BIT
          case StringType => java.sql.Types.CLOB
          case BinaryType => java.sql.Types.BLOB
          case TimestampType => java.sql.Types.TIMESTAMP
          case DateType => java.sql.Types.DATE
          case t: DecimalType => java.sql.Types.DECIMAL
          case _ => throw new IllegalArgumentException(
            s"Can't translate null value for field $field")
        })
    }

    val rddSchema = df.schema
    val driver: String = DriverRegistry.getDriverClassName(url)
    val getConnection: () => Connection = com.gaogf.dataloader.JDBCRDD.getConnector(driver, url, properties)
    df.foreachPartition { iterator =>
      savePartition(getConnection, table, iterator, rddSchema, nullTypes)
    }
  }

}
