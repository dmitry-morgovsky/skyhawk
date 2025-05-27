package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"net/http"
)

type preparedStatements struct {
	upsertEvent              *sql.Stmt
	forUpdatesByEventType    map[eventType]*sql.Stmt
	forStatisticsByOperation map[operation]map[table]*sql.Stmt
}

func startServer(ctx context.Context, db *sql.DB, stmts preparedStatements, rdb *redis.Client) error {
	http.HandleFunc("/api/v1/event", eventHandler(ctx, db, stmts, rdb))

	log.Println("NBA Player events consumer is running")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		return fmt.Errorf("failed to listen and serve: %w", err)
	}

	return nil
}

func eventHandler(ctx context.Context, db *sql.DB, stmts preparedStatements, rdb *redis.Client) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		var event event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			respondError(w, http.StatusBadRequest, fmt.Errorf("failed to decode JSON: %w", err))
			return
		}

		if err := event.validate(); err != nil {
			respondError(w, http.StatusBadRequest, fmt.Errorf("failed to validate event: %w", err))
			return
		}

		if err := processEvent(ctx, event, db, stmts); err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Errorf("failed to process event %q: %w", event, err))
			return
		}
		log.Println(fmt.Sprintf("Event %q processed successfully", event))

		for _, table := range statisticsTables {
			if err := updateCache(ctx, table, stmts, rdb); err != nil {
				respondError(w, http.StatusInternalServerError, fmt.Errorf("failed to update %q cache: %w", table, err))
				return
			}
			log.Println(fmt.Sprintf("Cache table %q updated successfully after event %q", table, event))
		}
	}
}

// respondError logs the error and write it to http.ResponseWriter with the given statusCode
func respondError(w http.ResponseWriter, statusCode int, err error) {
	log.Println(err)
	http.Error(w, fmt.Sprintf("ERROR: %s", err.Error()), statusCode)
}

func processEvent(ctx context.Context, event event, db *sql.DB, preparedStatements preparedStatements) error {
	tx, err := db.BeginTx(ctx, nil) // nil *TxOptions means default
	if err != nil {
		return fmt.Errorf("failed to open transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				if p, ok := p.(error); ok {
					p = errors.Join(p, rollbackErr)
				}
			}
			panic(p) // re-panic if it was a reason of the rollback
		}

		if err != nil {
			if rollbackErr := tx.Rollback(); err != nil {
				err = errors.Join(err, rollbackErr)
			}
		}
	}()

	season, gameDate := event.season(), event.gameDate()

	if err = txExec(ctx, tx, preparedStatements.upsertEvent, event.Player, event.Team, event.Timestamp, event.Event, gameDate, event.value()); err != nil {
		return fmt.Errorf("failed to upsert event %q: %w", event, err)
	}

	if err = txExec(ctx, tx, preparedStatements.forUpdatesByEventType[event.Event], event.Player, event.Team, gameDate, season); err != nil {
		return fmt.Errorf("failed to update %q table after %q event: %w", tablePlayersByGames, event, err)
	}

	if err = txExec(ctx, tx, preparedStatements.forStatisticsByOperation[operationUpdateStatistics][tablePlayersStatistics], event.Player, season); err != nil {
		return fmt.Errorf("failed to update %q table: %w", tablePlayersStatistics, err)
	}

	if err = txExec(ctx, tx, preparedStatements.forStatisticsByOperation[operationUpdateStatistics][tableTeamsStatistics], event.Team, season); err != nil {
		return fmt.Errorf("failed to update %q table: %w", tableTeamsStatistics, err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// txExec executes a prepared statement stmt in the tx transaction using given args arguments, and responding error to w http.ResponseWriter
func txExec(ctx context.Context, tx *sql.Tx, stmt *sql.Stmt, args ...any) error {
	if _, err := tx.StmtContext(ctx, stmt).ExecContext(ctx, args...); err != nil {
		return fmt.Errorf("failed to execute statement: %w", err)
	}

	return nil
}

var subjectsByTables = map[table]string{
	tablePlayersStatistics: "player",
	tableTeamsStatistics:   "team",
}

type Statistics struct {
	Points        float64 `json:"points"`
	Rebounds      float64 `json:"rebounds"`
	Assists       float64 `json:"assists"`
	Steals        float64 `json:"steals"`
	Blocks        float64 `json:"blocks"`
	Fouls         float64 `json:"fouls"`
	Turnovers     float64 `json:"turnovers"`
	MinutesPlayed float64 `json:"minutesPlayed"`
}

// updateCache copies unprocessed rows to Redis and marks them processed
func updateCache(ctx context.Context, table table, stmts preparedStatements, rdb *redis.Client) error {
	subject := subjectsByTables[table]

	rows, err := stmts.forStatisticsByOperation[operationSelectUnprocessed][table].QueryContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to query rows from %q: %w", table, err)
	}
	defer closeIt("rows", rows)

	for rows.Next() {
		var key string
		var season string
		var s Statistics
		if err := rows.Scan(&key, &season, &s.Points, &s.Rebounds, &s.Assists, &s.Steals, &s.Blocks, &s.Fouls, &s.Turnovers, &s.MinutesPlayed); err != nil {
			return fmt.Errorf("failed to scan row from %q: %w", table, err)
		}

		valueJSON, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("failed to marshal statistics from %q: %w", table, err)
		}

		statsKey := fmt.Sprintf("%s:%s:%s", subject, key, season)
		log.Println(fmt.Sprintf("Going to set the %q key to the value %q in Redis. ", statsKey, valueJSON))

		if err := rdb.Set(ctx, statsKey, valueJSON, 0).Err(); err != nil {
			return fmt.Errorf("failed to set to Redis statistics of %s %q for season %s: %w", subject, key, season, err)
		}

		if _, err := stmts.forStatisticsByOperation[operationUpdateUnprocessed][table].ExecContext(ctx, key, season); err != nil {
			return fmt.Errorf("failed to update processed row of %s where %s is %q for season %s: %w", table, subject, key, season, err)
		}
	}

	return nil
}
