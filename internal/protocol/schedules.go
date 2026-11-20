package protocol

const (
	ScheduleTargetKindRoom = "room"
	ScheduleTargetKindMail = "mail"

	ScheduleStatusActive    = "active"
	ScheduleStatusCompleted = "completed"
	ScheduleStatusCancelled = "cancelled"

	MinimumRecurringIntervalSeconds = 60
	MaximumSchedulesPerThread       = 32
	MaximumSchedulesPerCaller       = 64
	DefaultMaxRecurringOccurrences  = 100
	MaximumRecurringOccurrences     = 1000
)

var (
	ScheduleTargetKinds = []string{ScheduleTargetKindRoom, ScheduleTargetKindMail}
	ScheduleStatuses    = []string{ScheduleStatusActive, ScheduleStatusCompleted, ScheduleStatusCancelled}
)

type ScheduledMessage struct {
	ID                  string            `json:"id"`
	SourceKey           string            `json:"sourceKey"`
	IdempotencyKey      string            `json:"idempotencyKey"`
	Digest              string            `json:"digest"`
	CreatorKind         string            `json:"creatorKind"`
	CreatorClientID     string            `json:"creatorClientId,omitempty"`
	CreatorOperatorID   string            `json:"creatorOperatorId,omitempty"`
	CreatorRunID        string            `json:"creatorRunId,omitempty"`
	CreatorInstanceID   string            `json:"creatorInstanceId,omitempty"`
	PrincipalKind       string            `json:"principalKind,omitempty"`
	PrincipalID         string            `json:"principalId,omitempty"`
	VerifiedNode        string            `json:"verifiedNode,omitempty"`
	TargetKind          string            `json:"targetKind"`
	ThreadID            string            `json:"threadId,omitempty"`
	Channel             string            `json:"channel,omitempty"`
	Kind                string            `json:"kind,omitempty"`
	Topic               string            `json:"topic,omitempty"`
	Labels              map[string]string `json:"labels,omitempty"`
	Mentions            []string          `json:"mentions,omitempty"`
	To                  string            `json:"to,omitempty"`
	Subject             string            `json:"subject,omitempty"`
	Body                string            `json:"body"`
	Blocks              []StructuredBlock `json:"blocks,omitempty"`
	Wake                bool              `json:"wake"`
	EverySeconds        int               `json:"everySeconds,omitempty"`
	MaxOccurrences      int               `json:"maxOccurrences"`
	OccurrencesFired    int               `json:"occurrencesFired"`
	UntilAt             string            `json:"untilAt,omitempty"`
	CancelOnThreadClose bool              `json:"cancelOnThreadClose,omitempty"`
	NextDeliverAt       string            `json:"nextDeliverAt"`
	LastDeliveredAt     string            `json:"lastDeliveredAt,omitempty"`
	LastError           string            `json:"lastError,omitempty"`
	Status              string            `json:"status"`
	CreatedAt           string            `json:"createdAt"`
	UpdatedAt           string            `json:"updatedAt"`
	CancelledAt         string            `json:"cancelledAt,omitempty"`
}
