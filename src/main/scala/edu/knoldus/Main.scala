package edu.knoldus

import edu.knoldus.services.FootballMatchesService
import org.apache.log4j.{Level, Logger}

object Main extends App{

    Logger.getLogger("org").setLevel(Level.OFF)
    val footballMatchesService = new FootballMatchesService
    footballMatchesService.findHomeTeamMatchCount.show()
    footballMatchesService.findTopTenHighestWinningPercentageTeams.show()
    footballMatchesService.findTeamsMatchCount.show()
    footballMatchesService.findTopTenHighestWinTeams.show()

}
