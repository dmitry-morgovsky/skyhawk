package internal

import (
	"database/sql"
	"database/sql/driver"
	"github.com/DATA-DOG/go-sqlmock"
	"strings"
	"testing"
	"time"
)

const (
	leBronJames      = "LeBron James"
	losAngelesLakers = "Los Angeles Lakers"
)

func esc(s string) string {
	for _, c := range []string{"(", ")", "$", ".", "+"} {
		s = strings.ReplaceAll(s, c, "\\"+c)
	}
	return s
}

func prepareMockStmt(t *testing.T, db *sql.DB, mock sqlmock.Sqlmock, sql string) (*sqlmock.ExpectedPrepare, *sql.Stmt) {
	expectedPrepare := mock.ExpectPrepare(esc(sql))
	stmt, err := db.PrepareContext(t.Context(), sql)
	if err != nil {
		t.Fatalf("failed to prepare event: %v", err)
	}
	return expectedPrepare, stmt
}

func prepareUpdateStatisticsMockStmts(t *testing.T, db *sql.DB, mock sqlmock.Sqlmock) (map[table]*sqlmock.ExpectedPrepare, map[table]*sql.Stmt) {
	expectedPrepares := map[table]*sqlmock.ExpectedPrepare{}
	stmts := map[table]*sql.Stmt{}
	for _, table := range statisticsTables {
		expectedPrepare, stmt := prepareMockStmt(t, db, mock, statisticsTableOperationsSQLs[operationUpdateStatistics][table])
		expectedPrepares[table] = expectedPrepare
		stmts[table] = stmt
	}
	return expectedPrepares, stmts
}

func testEventHandler(t *testing.T, e event, eventSQL string) {
	ctx := t.Context()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer closeIt("DB", db)

	upsertEventExpectedPrepare, upsertEventStmt := prepareMockStmt(t, db, mock, upsertEventSQL)
	eventExpectedPrepare, eventStmt := prepareMockStmt(t, db, mock, eventSQL)
	updateStatisticsExpectedPrepares, updateStatisticsStmts := prepareUpdateStatisticsMockStmts(t, db, mock)

	stmts := preparedStatements{
		upsertEvent:              upsertEventStmt,
		forUpdatesByEventType:    map[eventType]*sql.Stmt{e.Event: eventStmt},
		forStatisticsByOperation: map[operation]map[table]*sql.Stmt{operationUpdateStatistics: updateStatisticsStmts},
	}

	season, gameDate := e.season(), e.gameDate()

	mock.ExpectBegin()
	upsertEventExpectedPrepare.ExpectExec().WithArgs(e.Player, e.Team, e.Timestamp, e.Event, gameDate, e.value()).WillReturnResult(driver.RowsAffected(1))
	eventExpectedPrepare.ExpectExec().WithArgs(e.Player, e.Team, gameDate, season).WillReturnResult(driver.RowsAffected(0))
	updateStatisticsExpectedPrepares[tablePlayersStatistics].ExpectExec().WithArgs(e.Player, season).WillReturnResult(driver.RowsAffected(1))
	updateStatisticsExpectedPrepares[tableTeamsStatistics].ExpectExec().WithArgs(e.Team, season).WillReturnResult(driver.RowsAffected(1))
	mock.ExpectCommit()

	if err := processEvent(ctx, e, db, stmts); err != nil {
		t.Fatalf("failed to process event: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestEventHandler_EventEnter(t *testing.T) {
	testEventHandler(t,
		event{Player: leBronJames, Team: losAngelesLakers, Timestamp: time.Date(2025, time.May, 23, 15, 0, 0, 0, time.Local), Event: eventEnter},
		updateGameOnTimeEventSQL,
	)
}

func TestEventHandler_EventExit(t *testing.T) {
	testEventHandler(t,
		event{Player: leBronJames, Team: losAngelesLakers, Timestamp: time.Date(2025, time.May, 23, 15, 0, 0, 0, time.Local), Event: eventExit},
		updateGameOnTimeEventSQL,
	)
}

func TestEventHandler_EventShot1Point(t *testing.T) {
	testEventHandler(t,
		event{Player: leBronJames, Team: losAngelesLakers, Timestamp: time.Date(2025, time.May, 23, 15, 0, 0, 0, time.Local), Event: eventShot, Points: 1},
		updateGameOnCounterEventSQL(eventShot, columnPoints),
	)
}

func TestEventHandler_EventShot2Points(t *testing.T) {
	testEventHandler(t,
		event{Player: leBronJames, Team: losAngelesLakers, Timestamp: time.Date(2025, time.May, 23, 15, 0, 0, 0, time.Local), Event: eventShot, Points: 2},
		updateGameOnCounterEventSQL(eventShot, columnPoints),
	)
}

func TestEventHandler_EventShot3Points(t *testing.T) {
	testEventHandler(t,
		event{Player: leBronJames, Team: losAngelesLakers, Timestamp: time.Date(2025, time.May, 23, 15, 0, 0, 0, time.Local), Event: eventShot, Points: 3},
		updateGameOnCounterEventSQL(eventShot, columnPoints),
	)
}

func TestEventHandler_EventRebound(t *testing.T) {
	testEventHandler(t,
		event{Player: leBronJames, Team: losAngelesLakers, Timestamp: time.Date(2025, time.May, 23, 15, 0, 0, 0, time.Local), Event: eventRebound},
		updateGameOnCounterEventSQL(eventRebound, columnRebounds),
	)
}

func TestEventHandler_EventAssist(t *testing.T) {
	testEventHandler(t,
		event{Player: leBronJames, Team: losAngelesLakers, Timestamp: time.Date(2025, time.May, 23, 15, 0, 0, 0, time.Local), Event: eventAssist},
		updateGameOnCounterEventSQL(eventAssist, columnAssists),
	)
}

func TestEventHandler_EventSteal(t *testing.T) {
	testEventHandler(t,
		event{Player: leBronJames, Team: losAngelesLakers, Timestamp: time.Date(2025, time.May, 23, 15, 0, 0, 0, time.Local), Event: eventSteal},
		updateGameOnCounterEventSQL(eventSteal, columnSteals),
	)
}

func TestEventHandler_EventBlock(t *testing.T) {
	testEventHandler(t,
		event{Player: leBronJames, Team: losAngelesLakers, Timestamp: time.Date(2025, time.May, 23, 15, 0, 0, 0, time.Local), Event: eventBlock},
		updateGameOnCounterEventSQL(eventBlock, columnBlocks),
	)
}

func TestEventHandler_EventFoul(t *testing.T) {
	testEventHandler(t,
		event{Player: leBronJames, Team: losAngelesLakers, Timestamp: time.Date(2025, time.May, 23, 15, 0, 0, 0, time.Local), Event: eventFoul},
		updateGameOnCounterEventSQL(eventFoul, columnFouls),
	)
}

func TestEventHandler_EventTurnover(t *testing.T) {
	testEventHandler(t,
		event{Player: leBronJames, Team: losAngelesLakers, Timestamp: time.Date(2025, time.May, 23, 15, 0, 0, 0, time.Local), Event: eventTurnover},
		updateGameOnCounterEventSQL(eventTurnover, columnTurnovers),
	)
}
