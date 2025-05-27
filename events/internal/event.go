package internal

import (
	"errors"
	"fmt"
	"time"
)

type eventType string

const (
	eventShot     eventType = "shot"
	eventRebound  eventType = "rebound"
	eventAssist   eventType = "assist"
	eventSteal    eventType = "steal"
	eventBlock    eventType = "block"
	eventFoul     eventType = "foul"
	eventTurnover eventType = "turnover"
	eventEnter    eventType = "enter" // "sub in", for Minutes Played calculation
	eventExit     eventType = "exit"  // "sub out", for Minutes Played calculation
)

var eventTypes = map[eventType]bool{
	eventShot:     true,
	eventRebound:  true,
	eventAssist:   true,
	eventSteal:    true,
	eventBlock:    true,
	eventFoul:     true,
	eventTurnover: true,
	eventEnter:    true,
	eventExit:     true,
}

type event struct {
	Player    string    `json:"player"`
	Team      string    `json:"team"`
	Timestamp time.Time `json:"timestamp"`
	Event     eventType `json:"event"`
	Points    int       `json:"points"` // only relevant for `eventShot` event type
}

func (e event) validate() error {
	if e.Player == "" {
		return errors.New("'player' is not specified")
	}

	if e.Team == "" {
		return errors.New("'team' is not specified")
	}

	if e.Timestamp.IsZero() {
		return errors.New("'timestamp' is not specified")
	}

	if e.Event == "" {
		return errors.New("'event' is not specified")
	}

	if !eventTypes[e.Event] {
		return fmt.Errorf("unknown 'event': %q", e.Event)
	}

	if e.Event == eventShot && (e.Points < 1 || e.Points > 3) ||
		e.Event != eventShot && e.Points != 0 {
		return fmt.Errorf("invalid 'points' value: %d", e.Points)
	}

	return nil
}

func (e event) String() string {
	return fmt.Sprintf("%s, %s, %s, %s, %d", e.Player, e.Team, e.Timestamp, e.Event, e.Points)
}

// value returns 0 for 'enter' and 'exit' events; number of points for 'shot' event and 1 for other event types
func (e event) value() int {
	switch e.Event {
	case eventEnter, eventExit:
		return 0
	case eventShot:
		return e.Points
	default:
		return 1
	}
}

// gameDate returns the date of the game in format "2006-01-02"
func (e event) gameDate() string {
	return e.Timestamp.Format(time.DateOnly)
}

// season returns the NBA season in the format "2006-07"
func (e event) season() string {
	startYear := e.Timestamp.Year()
	if e.Timestamp.Month() < time.October {
		startYear -= 1
	}
	return fmt.Sprintf("%d-%02d", startYear, (startYear+1)%100)
}
