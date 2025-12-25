package db

import "time"

type update struct {
	data       []byte
	updateTime time.Time
}
