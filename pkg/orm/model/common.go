package model

import (
	"time"
)

// CreatedAt/UpdatedAt will autoupdate in the gorm lib, not in sql backend
type Model struct {
	SeqID     uint `gorm:"primaryKey;autoIncrement"`
	CreatedAt time.Time
	UpdatedAt time.Time
}
