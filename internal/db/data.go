package db

type DataVersion struct {
	Version uint64
	Data    []byte
}

func NewDataVersion(version uint64, data []byte) *DataVersion {
	return &DataVersion{
		Version: version,
		Data:    data,
	}
}

func (r *DataVersion) Bump() *DataVersion {
	return NewDataVersion(r.Version+1, r.Data)
}
