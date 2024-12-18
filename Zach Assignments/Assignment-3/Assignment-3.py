# import all necessary libraries 
from pyspark.sql.functions import broadcast , lit, split, count,col
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import StorageLevel
import pandas as pd
pd.set_option('display.max_rows', None)

# create a spark session by disabling autoBroadcast and few other configuration
spark = SparkSession.builder \
.appName("IcebergTableManagement") \
.config("spark.sql.autoBroadcastJoinThreshold", "-1")\
.config('spark.sql.sources.v2.bucketing.enabled','true') \
.config('spark.sql.sources.v2.bucketing.pushPartValues.enabled','true')\
.config('spark.sql.iceberg.planning.preserve-data-grouping','true')\
.config('spark.sql.requireAllClusterKeysForCoPartition','false')\
.config('spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled','true')\
.config("spark.executor.memory", "8g")\
.config("spark.driver.memory", "8g")\
.config("spark.sql.shuffle.partitions", "200")\
.config("spark.sql.files.maxPartitionBytes", "134217728")\
.config("spark.sql.autoBroadcastJoinThreshold", "-1")\
.config("spark.dynamicAllocation.enabled", "true")\
.config("spark.dynamicAllocation.minExecutors", "1")\
.config("spark.dynamicAllocation.maxExecutors", "50")\
.config("spark.sql.debug.maxToStringFields","100") \
.getOrCreate()

# create a dataframe  by reading all files from path notebooks/data/*
# match_details.csv
match_details_df = spark.read.option("header","True")\
                                .option("inferschema","True")\
                                .csv('data/match_details.csv')

matches_df = spark.read.option("header","True")\
                        .option("inferschema","True")\
                        .csv('data/matches.csv')

# medals_matches_players.csv
medals_matches_players_df = spark.read.option("header","True")\
                            .option("inferschema","True")\
                            .csv('data/medals_matches_players.csv')

# medals.csv
medals_df = spark.read.option("header","True")\
                    .option("inferschema","True")\
                    .csv('data/medals.csv')

# maps.csv
maps_df = spark.read.option("header","True")\
                    .option("inferschema","True")\
                    .csv('data/maps.csv')


# create an iceberg table  for loading data from above dataframes
# games.medals
spark.sql("DROP TABLE IF EXISTS games.medals")
medals_table = ("""CREATE TABLE IF NOT EXISTS games.medals(
medal_id bigint,
sprite_uri string,
sprite_left int,
sprite_top int,
sprite_sheet_width int,
sprite_sheet_height int,
sprite_width int,
sprite_height int,
classification string,
description string,
name string,
difficulty int
)using ICEBERG"""
)

# games.maps
spark.sql("DROP TABLE IF EXISTS games.maps")
maps_table = """CREATE TABLE IF NOT EXISTS games.maps( 
mapid string,
name string,
description string
) using ICEBERG"""

#  games.match_details
spark.sql("DROP TABLE IF EXISTS games.match_details")
match_details_table = """CREATE TABLE IF NOT EXISTS games.match_details(
match_id string,
player_gamertag  string,
previous_spartan_rank int,
spartan_rank int,
previous_total_xp int,
total_xp int,
previous_csr_tier int,
previous_csr_designation int,
previous_csr int,
previous_csr_percent_to_next_tier int,
previous_csr_rank int,
current_csr_tier int,
current_csr_designation int,
current_csr int,
current_csr_percent_to_next_tier int,
current_csr_rank int,
player_rank_on_team int,
player_finished boolean,
player_average_life string,
player_total_kills int,
player_total_headshots int,
player_total_weapon_damage int,
player_total_shots_landed int,
player_total_melee_kills int,
player_total_melee_damage double,
player_total_assassinations int,
player_total_ground_pound_kills int,
player_total_shoulder_bash_kills int,
player_total_grenade_damage double,
player_total_power_weapon_damage double,
player_total_power_weapon_grabs int,
player_total_deaths int,
player_total_assists int,
player_total_grenade_kills int,
did_win int,
team_id int
)using ICEBERG
partitioned by (bucket(16,match_id))
"""

# games.matches
spark.sql("DROP TABLE IF EXISTS games.matches")
matches_table = """CREATE TABLE IF NOT EXISTS games.matches(
match_id string,
mapid string,
playlist_id string,
completion_date timestamp
)using ICEBERG
partitioned by (bucket(16,match_id))
"""
# games.medal_matches_players
spark.sql("DROP TABLE IF EXISTS games.medal_matches_players")
medal_matches_players_table = """CREATE TABLE IF NOT EXISTS games.medal_matches_players(
match_id string,
player_gamertag string,
medal_id bigint,
count int
)using ICEBERG
partitioned by (bucket(16,match_id))"""


# execute the create statements from above
spark.sql(medals_table)
spark.sql(maps_table)
spark.sql(match_details_table)
spark.sql(matches_table)
spark.sql(medal_matches_players_table)

# load the created table with dataframes
maps_df.write.mode("append").saveAsTable("games.maps")
medals_df.write.mode("append").saveAsTable("games.medals")
match_details_df.write.mode("append").bucketBy(16,"match_id").saveAsTable("games.match_details")
medals_matches_players_df.write.mode("append").bucketBy(16,"match_id").saveAsTable("games.medal_matches_players")

# create loop to load the data for matches date wise to avoid OOM issue during bucketing data
distinct_date = matches_df.select("completion_date").distinct().orderBy("completion_date").collect()
for row in distinct_date:
    date = row["completion_date"] 
    filtered_matches_df = matches_df.select("match_id","mapid","playlist_id","completion_date").where(F.col("completion_date") == F.lit(date))
    optimized_matches_df = filtered_matches_df.repartition(16,"match_id").persist(StorageLevel.MEMORY_AND_DISK)
    optimized_matches_df.write.mode("append").bucketBy(16,"match_id").saveAsTable("games.matches")


# create a Joined dataframe 

matches_join_detail_df = spark.table("games.matches").alias("m")\
            .join(spark.table("games.match_details").alias("md"),(F.col("m.match_id") == F.col("md.match_id")),"left")\
            .select("m.match_id","m.mapid","m.playlist_id","md.player_total_kills","md.player_gamertag")\
            .repartition(16,"m.match_id")

match_final_df = matches_join_detail_df.alias("mjd")\
        .join(spark.table("games.medal_matches_players").alias("mmp"),((F.col("mjd.match_id") == F.col("mmp.match_id")) & (F.col("mjd.player_gamertag") == F.col("mmp.player_gamertag"))),"left")\
        .select("mjd.match_id","mjd.mapid","mjd.playlist_id","mmp.medal_id","mjd.player_gamertag","mjd.player_total_kills") 
        

final_match_map_joined_df = match_final_df.alias("mf")\
                    .select("mf.match_id","mf.playlist_id","mf.mapid","mf.player_gamertag","mf.player_total_kills").distinct()\
                    .join(broadcast(spark.table("games.maps").alias("mp")),(F.col("mp.mapid") == F.col("mf.mapid")),"left")\
                    .select("mf.match_id",\
                            "mf.playlist_id",\
                            "mf.mapid",\
                            "mf.player_gamertag",\
                            "mf.player_total_kills",\
                            F.col("mp.name").alias("mapName"))


# which player averages the most kills per game
most_avg_kill_per_game = final_match_map_joined_df.alias("fmmj")\
        .groupBy("player_gamertag")\
        .agg(F.avg("fmmj.player_total_kills").alias("avg_kill_per_game"))\
        .orderBy("avg_kill_per_game",ascending = False)\
        .limit(1)
most_avg_kill_per_game.show()


# Which playlist gets played the most?
most_played_playlist = final_match_map_joined_df.alias("fmmj")\
                                .groupBy("playlist_id")\
                                .agg(count("playlist_id").alias("played_count"))\
                                .orderBy(F.col("played_count").desc()).limit(1)
most_played_playlist.show()



#  Which map gets played the most?
most_map_played = final_match_map_joined_df.alias("fmmj")\
                        .groupBy("mapName")\
                        .agg(count("match_id").alias("map_played_count"))\
                        .orderBy(F.col("map_played_count").desc()).limit(1)
most_map_played.show()


# dataframe to join killingspree medal and then join with match_final_df
map_medal = match_final_df.alias("mf")\
            .join(broadcast(spark.table("games.medals").alias("mdl").filter(F.col("mdl.name") == "Killing Spree")),\
                F.col("mdl.medal_id") == F.col("mf.medal_id"),"inner")\
            .join(broadcast(spark.table("games.maps").alias("m")),F.col("mf.mapid") == F.col("m.mapid"),"inner")\
            .select("mf.match_id",\
                    F.col("m.name").alias("mapName"),\
                    F.col("mdl.name").alias("medalName"))



# Which map do players get the most Killing Spree medals on?
most_spree_medal_map = map_medal.alias("mm")\
                        .groupBy("mapName")\
                        .agg(count("medalName").alias("medal_count"))\
                        .orderBy(F.col("medal_count").desc()).limit(1)
most_spree_medal_map.show()

# to try which column give efficient partiton size using sortWithinPartitions
spark.sql("drop table games.joined_table")
match_final_df.sortWithinPartitions("mapid")\
                        .write.format("iceberg")\
                        .mode("append")\
                        .saveAsTable("games.joined_table")

spark.sql("select round(sum(file_size_in_bytes)/ (1024*1024),2)  as file_size_in_mb from games.joined_table.files").show()
# mapid =7572931 --> 7.22 mb
# playlist_id = 7585821 -->7.23 mb
# match_id = 7913664 --> 7.55 mb
# medal_id = 8476548 --> 8.08 mb
