package events

import "time"

// DLQEnvelope — то, что попадает в DLQ-топик.
// Содержит всё для ручного разбора: откуда пришло, почему упало, когда.
type DLQEnvelope struct {
	OriginalTopic     string    `json:"original_topic"`
	OriginalPartition int       `json:"original_partition"`
	OriginalOffset    int64     `json:"original_offset"`
	Reason            string    `json:"reason"` // "decode_error", "db_error", ...
	ReasonDetail      string    `json:"reason_detail,omitempty"`
	RawPayload        string    `json:"raw_payload"` // исходное сообщение как есть
	FirstFailedAt     time.Time `json:"first_failed_at"`
}

const (
	ReasonDecodeError = "decode_error"
	ReasonDBError     = "db_error"
)
