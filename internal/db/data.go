package db

type Record struct {
	Version uint64
	Data    []byte
}

func NewRecord(version uint64, data []byte) *Record {
	return &Record{
		Version: version,
		Data:    data,
	}
}

func (r *Record) Bump() *Record {
	return NewRecord(r.Version+1, r.Data)
}
