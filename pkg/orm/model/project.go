package model

import "time"

// ProjectInfo for Multi-projects support
type ProjectInfo struct {
	Model
	ID   string `gorm:"column:id;type:varchar(64) not null;uniqueIndex:uidx_id"`
	Name string `gorm:"type:varchar(64) not null"`
}

// ProjectOperation records each operation of a project
type ProjectOperation struct {
	SeqID     uint      `gorm:"primaryKey;auto_increment"`
	ProjectID string    `gorm:"type:varchar(64) not null;index:idx_op"`
	Operation string    `gorm:"type:varchar(16) not null"`
	JobID     string    `gorm:"type:varchar(64) not null"`
	CreatedAt time.Time `gorm:"autoCreateTime;index:idx_op"`
}
