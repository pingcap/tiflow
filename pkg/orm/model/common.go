package model

import (
	"time"
)

// CreatedAt/UpdatedAt will autoupdate in the gorm lib, not in sql backend
// TODO: refine the updatedAt to autoupdate backend, make `upsert` interface more elegant
type Model struct {
	SeqID     uint      `json:"seq-id" gorm:"primaryKey;autoIncrement"`
	CreatedAt time.Time `json:"created-at"`
	UpdatedAt time.Time `json:"updated-at"`
}
