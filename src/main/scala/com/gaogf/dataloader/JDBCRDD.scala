package com.gaogf.dataloader

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry

/**
  * Created by pactera on 2018/3/6.
  */
private  object JDBCRDD extends Logging {
  def getConnector(driver: String, url: String, properties: Properties): () => Connection = {
    () => {
      try {
        if (driver != null) DriverRegistry.register(driver)
      } catch {
        case e: ClassNotFoundException =>
          logWarning(s"Couldn't find class $driver", e)
      }
      DriverManager.getConnection(url, properties)
    }
  }
}
