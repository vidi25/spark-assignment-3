package edu.knoldus.services

import edu.knoldus.configuration.GlobalObject._
import edu.knoldus.entities.Football
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

class FootballMatchesService {

  val footballDataFrame: DataFrame = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(filePath)
    .select("HomeTeam", "Awayteam", "FTHG", "FTAG", "FTR")

  /*
  function to find home team matches count
   */
  def findHomeTeamMatchCount: DataFrame = {

    footballDataFrame.groupBy("HomeTeam").count.withColumnRenamed("count", "HomeTeamMatchCount")

  }

  /*
  function to find top 10 highest winning percentage teams
   */
  def findTopTenHighestWinningPercentageTeams: DataFrame = {

    footballDataFrame.createOrReplaceTempView("footballMatches")
    val homeTeamMatchesDF = sparkSession.sql("SELECT HomeTeam,count(*) AS HomeTeamCounts FROM footballMatches GROUP BY HomeTeam")
    val awayTeamMatchesDF = sparkSession.sql("SELECT AwayTeam,count(*) AS AwayTeamCounts FROM footballMatches GROUP BY AwayTeam")
    val homeTeamWinsDF = sparkSession.sql("SELECT HomeTeam,count(*) AS HomeTeamWins FROM footballMatches where FTR='H' GROUP BY HomeTeam")
    val awayTeamWinsDF = sparkSession.sql("SELECT AwayTeam,count(*) AS AwayTeamWins FROM footballMatches where FTR='A' GROUP BY AwayTeam")
    homeTeamMatchesDF.join(awayTeamMatchesDF, homeTeamMatchesDF.col("HomeTeam") === awayTeamMatchesDF.col("AwayTeam")).createOrReplaceTempView("TotalMatchesDF")
    val totalMatchesDF = sparkSession.sql("SELECT HomeTeam AS HTeam,(HomeTeamCounts + AwayTeamCounts) AS TotalMatches FROM TotalMatchesDF")
    homeTeamWinsDF.join(awayTeamWinsDF, homeTeamWinsDF.col("HomeTeam") === awayTeamWinsDF.col("AwayTeam")).createOrReplaceTempView("TotalWinsDF")
    val totalWinsDF = sparkSession.sql("SELECT AwayTeam AS ATeam,(HomeTeamWins + AwayTeamWins) AS TotalWins FROM TotalWinsDF")
    totalMatchesDF.join(totalWinsDF, totalMatchesDF.col("HTeam") === totalWinsDF.col("ATeam")).createOrReplaceTempView("TeamsDF")
    sparkSession.sql("SELECT HTeam AS Team,(TotalWins/TotalMatches)*100 AS Win_Percentage FROM TeamsDF ORDER BY Win_Percentage DESC LIMIT 10 ")

  }

  import sparkSession.implicits._

  val footballDataSet: Dataset[Football] = footballDataFrame.map(data => Football(data.getString(0), data.getString(1), data.getInt(2), data.getInt(3), data.getString(4)))

  /*
  function to find all teams matches count
   */
  def findTeamsMatchCount: DataFrame = {

    footballDataSet.createOrReplaceTempView("footballMatches")
    val homeTeamMatchesDF = sparkSession.sql("SELECT HomeTeam,count(*) AS HomeTeamCounts from footballMatches GROUP BY HomeTeam")
    val awayTeamMatchesDF = sparkSession.sql("SELECT AwayTeam,count(*) AS AwayTeamCounts from footballMatches GROUP BY AwayTeam")
    homeTeamMatchesDF.join(awayTeamMatchesDF, homeTeamMatchesDF.col("HomeTeam") === awayTeamMatchesDF.col("AwayTeam")).createOrReplaceTempView("TotalMatchesDF")
    sparkSession.sql("SELECT HomeTeam AS Team,(HomeTeamCounts + AwayTeamCounts) AS TotalMatches FROM TotalMatchesDF")

  }

  /*
  function to find top 10 highest winning teams
   */
  def findTopTenHighestWinTeams: DataFrame = {

    val homeTeamDF = footballDataSet.select("HomeTeam", "FTR").where("FTR = 'H'").groupBy("HomeTeam").count().withColumnRenamed("count", "HomeWins")
    val awayTeamDF = footballDataSet.select("AwayTeam", "FTR").where("FTR = 'A'").groupBy("AwayTeam").count().withColumnRenamed("count", "AwayWins")
    val teamsDF = homeTeamDF.join(awayTeamDF, homeTeamDF.col("HomeTeam") === awayTeamDF.col("AwayTeam"))
    val add: (Int, Int) => Int = (HomeMatches: Int, TeamMatches: Int) => HomeMatches + TeamMatches
    val total = udf(add)
    teamsDF.withColumn("TotalWins", total(col("HomeWins"), col("AwayWins"))).select("HomeTeam", "TotalWins")
      .withColumnRenamed("HomeTeam", "Team").sort(desc("TotalWins")).limit(10)

  }

}
