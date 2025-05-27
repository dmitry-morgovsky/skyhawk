package internal

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Releases resources from signal.NotifyContext

	// Set up the Postgres
	dataSourceName := os.Getenv("POSTGRES_DSN")
	//dataSourceName := "postgres://skyhawk:skyhawk@localhost:5432/NBA?sslmode=disable"
	db, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return fmt.Errorf("failed to open DB %q: %w", dataSourceName, err)
	}

	defer closeIt("DB", db)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping DB %q: %w", dataSourceName, err)
	}
	log.Println("Successfully connected to PostgreSQL")

	// Set up the Redis
	redisAddr := os.Getenv("REDIS_ADDR")
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer closeIt("redis", rdb)

	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to ping Redis at %q: %v", redisAddr, err)
	}
	log.Println(fmt.Sprintf("Successfully connected to Redis at %q", redisAddr))

	// Create needed tables
	for table, createTableSQL := range map[table]string{
		tableEvents:            createTableEventsSQL,
		tablePlayersByGames:    createTablePlayersByGamesSQL,
		tablePlayersStatistics: createTablePlayersStatisticsSQL,
		tableTeamsStatistics:   createTableTeamsStatisticsSQL,
	} {
		if _, err := db.ExecContext(ctx, createTableSQL); err != nil {
			return fmt.Errorf("failed to create %q DB table: %w", table, err)
		}
		log.Println(fmt.Sprintf("Successfully created or updated %q table", table))
	}

	// Prepare statements for future usage
	stmts := preparedStatements{
		forUpdatesByEventType:    map[eventType]*sql.Stmt{},
		forStatisticsByOperation: map[operation]map[table]*sql.Stmt{},
	}

	stmts.upsertEvent, err = db.PrepareContext(ctx, upsertEventSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare statement to upsert event: %w", err)
	}
	defer closeIt("statement", stmts.upsertEvent)
	log.Println("Successfully prepared statement to upsert event")

	// Prepare SQL statements corresponding to event types
	for eventType, updateSQL := range map[eventType]string{
		eventShot:     updateGameOnCounterEventSQL(eventShot, columnPoints),
		eventRebound:  updateGameOnCounterEventSQL(eventRebound, columnRebounds),
		eventAssist:   updateGameOnCounterEventSQL(eventAssist, columnAssists),
		eventSteal:    updateGameOnCounterEventSQL(eventSteal, columnSteals),
		eventBlock:    updateGameOnCounterEventSQL(eventBlock, columnBlocks),
		eventFoul:     updateGameOnCounterEventSQL(eventFoul, columnFouls),
		eventTurnover: updateGameOnCounterEventSQL(eventTurnover, columnTurnovers),
		eventEnter:    updateGameOnTimeEventSQL,
		eventExit:     updateGameOnTimeEventSQL,
	} {
		statement, err := db.PrepareContext(ctx, updateSQL)
		if err != nil {
			return fmt.Errorf("failed to prepare statement to update players by game for event type %q: %w", eventType, err)
		}

		//goland:noinspection GoDeferInLoop // the warning is intentionally suppressed because it works correctly
		defer closeIt("statement", statement)
		log.Println(fmt.Sprintf("Successfully prepared statement to update players by game for event type %q", eventType))

		stmts.forUpdatesByEventType[eventType] = statement
	}

	for operation, sqlsByTable := range statisticsTableOperationsSQLs {
		stmts.forStatisticsByOperation[operation] = map[table]*sql.Stmt{}
		for _, table := range statisticsTables {
			statement, err := db.PrepareContext(ctx, sqlsByTable[table])
			if err != nil {
				return fmt.Errorf("failed to prepare statement to %s for %s table: %w", operation, table, err)
			}

			//goland:noinspection GoDeferInLoop // the warning is intentionally suppressed because it works correctly
			defer closeIt("statement", statement)
			log.Println("Successfully prepared statement to update player statistics")

			stmts.forStatisticsByOperation[operation][table] = statement
		}
	}

	if err := startServer(ctx, db, stmts, rdb); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}
