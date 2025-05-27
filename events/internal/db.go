package internal

import "fmt"

type column string

// names of columns in database tables
const (
	columnPoints    column = "points"
	columnRebounds  column = "rebounds"
	columnAssists   column = "assists"
	columnSteals    column = "steals"
	columnBlocks    column = "blocks"
	columnFouls     column = "fouls"
	columnTurnovers column = "turnovers"
)

type table string

const (
	tableEvents            table = "events"
	tablePlayersByGames    table = "players_by_games"
	tablePlayersStatistics table = "players_statistics"
	tableTeamsStatistics   table = "teams_statistics"
)

var statisticsTables = []table{tablePlayersStatistics, tableTeamsStatistics}

// SQL statements to create tables
const (
	createTableEventsSQL = `CREATE TABLE IF NOT EXISTS "public"."events" (
"player" text NOT NULL,
"team" text NOT NULL,
"timestamp" timestamp NOT NULL,
"event" text NOT NULL CHECK (event IN ('shot', 'rebound', 'assist', 'steal', 'block', 'foul', 'turnover', 'enter', 'exit')),
"game_date" date NOT NULL,
"value" int2 NOT NULL DEFAULT 0,
PRIMARY KEY ("player", "timestamp")
);`

	createTablePlayersByGamesSQL = `CREATE TABLE IF NOT EXISTS "public"."players_by_games" (
"player" text NOT NULL,
"team" text NOT NULL,
"game_date" date NOT NULL,
"season" text NOT NULL,
"points" int4 NOT NULL DEFAULT 0 CHECK (points >= 0),
"rebounds" int4 NOT NULL DEFAULT 0 CHECK (rebounds >= 0),
"assists" int4 NOT NULL DEFAULT 0 CHECK (assists >= 0),
"steals" int4 NOT NULL DEFAULT 0 CHECK (steals >= 0),
"blocks" int4 NOT NULL DEFAULT 0 CHECK (blocks >= 0),
"fouls" int2 NOT NULL DEFAULT '0'::smallint CHECK ((fouls >= 0) AND (fouls <= 6)),
"turnovers" int4 NOT NULL DEFAULT 0 CHECK (turnovers >= 0),
"minutes_played" float4 NOT NULL DEFAULT 0 CHECK ((minutes_played >= (0.0)::double precision) AND (minutes_played <= (48.0)::double precision)),
"entered" timestamp,
PRIMARY KEY ("player","game_date"));`

	createTablePlayersStatisticsSQL = `CREATE TABLE IF NOT EXISTS "public"."players_statistics" (
"player" text NOT NULL,
"season" text NOT NULL,
"points" float4 NOT NULL DEFAULT 0 CHECK (points >= (0.0)::double precision),
"rebounds" float4 NOT NULL DEFAULT 0 CHECK (rebounds >= (0.0)::double precision),
"assists" float4 NOT NULL DEFAULT 0 CHECK (assists >= (0.0)::double precision),
"steals" float4 NOT NULL DEFAULT 0 CHECK (steals >= (0.0)::double precision),
"blocks" float4 NOT NULL DEFAULT 0 CHECK (blocks >= (0.0)::double precision),
"fouls" float4 NOT NULL DEFAULT 0 CHECK ((fouls >= (0.0)::double precision) AND (fouls <= (6.0)::double precision)),
"turnovers" float4 NOT NULL DEFAULT 0 CHECK (turnovers >= (0.0)::double precision),
"minutes_played" float4 NOT NULL DEFAULT 0 CHECK ((minutes_played >= (0.0)::double precision) AND (minutes_played <= (48.0)::double precision)),
"processed" bool NOT NULL DEFAULT false,
PRIMARY KEY ("player","season"));`

	createTableTeamsStatisticsSQL = `CREATE TABLE IF NOT EXISTS "public"."teams_statistics" (
"team" text NOT NULL,
"season" text NOT NULL,
"points" float4 NOT NULL DEFAULT 0 CHECK (points >= (0.0)::double precision),
"rebounds" float4 NOT NULL DEFAULT 0 CHECK (rebounds >= (0.0)::double precision),
"assists" float4 NOT NULL DEFAULT 0 CHECK (assists >= (0.0)::double precision),
"steals" float4 NOT NULL DEFAULT 0 CHECK (steals >= (0.0)::double precision),
"blocks" float4 NOT NULL DEFAULT 0 CHECK (blocks >= (0.0)::double precision),
"fouls" float4 NOT NULL DEFAULT 0 CHECK ((fouls >= (0.0)::double precision) AND (fouls <= (6.0)::double precision)),
"turnovers" float4 NOT NULL DEFAULT 0 CHECK (turnovers >= (0.0)::double precision),
"minutes_played" float4 NOT NULL DEFAULT 0 CHECK ((minutes_played >= (0.0)::double precision) AND (minutes_played <= (48.0)::double precision)),
"processed" bool NOT NULL DEFAULT false,
PRIMARY KEY ("team", "season"));`
)

// upsertEventSQL is an SQL statement to upsert event
// Parameter placeholders are intended for:
// $1: player
// $2: team
// $3: timestamp
// $4: event
// $5: game date
// $6: value -- 0 for enter and exit events; 1, 2, or 3 for shot events; 1 for other event types
const upsertEventSQL = `
INSERT INTO "events" ("player", "team", "timestamp", "event", "game_date", "value") values ($1, $2, $3, $4, $5, $6)
ON CONFLICT ("player", "timestamp") DO UPDATE SET "event" = EXCLUDED."event", "value" = EXCLUDED."value" 
`

type operation string

const (
	operationUpdateStatistics  operation = "update_statistics"
	operationSelectUnprocessed operation = "select_unprocessed"
	operationUpdateUnprocessed operation = "update_unprocessed"
)

const (
	updatePlayersStatisticsSQL = `INSERT INTO "players_statistics" ("player", "season", "points", "rebounds", "assists", "steals", "blocks", "fouls", "turnovers", "minutes_played", "processed")
SELECT "player", "season", 
	CAST(AVG("points") as float4), 
	CAST(AVG("rebounds") as float4), 
	CAST(AVG("assists") as float4), 
	CAST(AVG("steals") as float4), 
	CAST(AVG("blocks") as float4), 
	CAST(AVG("fouls") as float4), 
	CAST(AVG("turnovers") as float4), 
	CAST(AVG("minutes_played") as float4),
    false
FROM "players_by_games"
WHERE "player" = $1 AND "season" = $2 
GROUP BY "player", "season" 
ON CONFLICT ("player", "season") DO 
UPDATE SET 
	"points" = EXCLUDED."points", 
	"rebounds" = EXCLUDED."rebounds", 
	"assists" = EXCLUDED."assists", 
	"steals" = EXCLUDED."steals", 
	"blocks" = EXCLUDED."blocks", 
	"fouls" = EXCLUDED."fouls", 
	"turnovers" = EXCLUDED."turnovers", 
	"minutes_played" = EXCLUDED."minutes_played",
	"processed" = EXCLUDED."processed";`

	updateTeamsStatisticsSQL = `INSERT INTO "teams_statistics" ("team", "season", "points", "rebounds", "assists", "steals", "blocks", "fouls", "turnovers", "minutes_played", "processed")
SELECT "team", "season", 
	CAST(AVG("points") as float4), 
	CAST(AVG("rebounds") as float4), 
	CAST(AVG("assists") as float4), 
	CAST(AVG("steals") as float4), 
	CAST(AVG("blocks") as float4), 
	CAST(AVG("fouls") as float4), 
	CAST(AVG("turnovers") as float4), 
	CAST(AVG("minutes_played") as float4),
    false
FROM "players_by_games"
WHERE "team" = $1 AND "season" = $2 
GROUP BY "team", "season" 
ON CONFLICT ("team", "season") DO 
UPDATE SET 
	"points" = EXCLUDED."points", 
	"rebounds" = EXCLUDED."rebounds", 
	"assists" = EXCLUDED."assists", 
	"steals" = EXCLUDED."steals", 
	"blocks" = EXCLUDED."blocks", 
	"fouls" = EXCLUDED."fouls", 
	"turnovers" = EXCLUDED."turnovers", 
	"minutes_played" = EXCLUDED."minutes_played",
	"processed" = EXCLUDED."processed";`

	selectUnprocessedPlayersStatisticsSQL = `SELECT "player", "season", "points", "rebounds", "assists", "steals", "blocks", "fouls", "turnovers", "minutes_played" FROM "players_statistics" WHERE "processed" = false`
	selectUnprocessedTeamsStatisticsSQL   = `SELECT "team", "season", "points", "rebounds", "assists", "steals", "blocks", "fouls", "turnovers", "minutes_played" FROM "teams_statistics" WHERE "processed" = false`

	updateUnprocessedPlayersStatisticsSQL = `UPDATE "players_statistics" SET "processed" = true WHERE "player" = $1 AND "season" = $2;`
	updateUnprocessedTeamsStatisticsSQL   = `UPDATE "teams_statistics" SET "processed" = true WHERE "team" = $1 AND "season" = $2;`
)

var statisticsTableOperationsSQLs = map[operation]map[table]string{
	operationUpdateStatistics: {
		tablePlayersStatistics: updatePlayersStatisticsSQL,
		tableTeamsStatistics:   updateTeamsStatisticsSQL,
	},
	operationSelectUnprocessed: {
		tablePlayersStatistics: selectUnprocessedPlayersStatisticsSQL,
		tableTeamsStatistics:   selectUnprocessedTeamsStatisticsSQL,
	},
	operationUpdateUnprocessed: {
		tablePlayersStatistics: updateUnprocessedPlayersStatisticsSQL,
		tableTeamsStatistics:   updateUnprocessedTeamsStatisticsSQL,
	},
}

// updateGameOnTimeEventSQL is an SQL statement to be prepared for updating the `players_by_games` table on events changing `minutes_played`, i.e. 'enter' and 'exit' events
// Parameter placeholders are intended for:
// $1: player
// $2: team
// $3: game date in format "2006-01-02"
// $4: season in format "2006-07",
const updateGameOnTimeEventSQL = `INSERT INTO "players_by_games" ("player", "team", "game_date", "season", "minutes_played")
(
	SELECT "player", "team", "game_date", $4, EXTRACT(EPOCH FROM SUM("next_timestamp" - "timestamp")) / 60.0 AS "minutes_played"
	FROM (
		SELECT "player", "team", "game_date", "event", "timestamp", "next_timestamp" 
		FROM (
			SELECT "player", "team", "game_date", "event", "timestamp", 
			LEAD("timestamp") OVER (PARTITION BY "player", "team", "game_date" ORDER BY "timestamp") AS "next_timestamp"
			FROM "events"
			WHERE "player" = $1 AND "team" = $2 AND "game_date" = $3 AND "event" IN ('enter', 'exit')
		)
		WHERE "event" = 'enter' AND "next_timestamp" IS NOT NULL
	)
	GROUP BY "player", "team", "game_date"
)
ON CONFLICT ("player", "game_date") DO UPDATE SET "minutes_played" = EXCLUDED."minutes_played";`

// updateGameOnCounterEventSQL returns an SQL statement to be prepared for updating the `players_by_games` table on events incrementing counters
// Parameter placeholders are intended for:
// $1: player
// $2: team
// $3: game date in format "2006-01-02"
// $4: season in format "2006-07",
//
// counterColumn -- the column to be incremented
func updateGameOnCounterEventSQL(event eventType, counterColumn column) string {
	return fmt.Sprintf(`INSERT INTO "players_by_games" ("player", "team", "game_date", "season", "%s") 
(
	select "player", "team", "game_date", $4, sum("value") 
	from "events" 
	where "player" = $1 and "team" = $2 and "game_date" = $3 and "event" = '%s' 
	group by "player", "team", "game_date"
)
ON CONFLICT ("player", "game_date") DO UPDATE SET "%s" = EXCLUDED."%s";`,
		counterColumn, event, counterColumn, counterColumn,
	)
}
