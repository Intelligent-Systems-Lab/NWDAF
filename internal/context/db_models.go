package context

import "time"

const UpfTrafficDataColl = "nwdaf.upfTrafficData"

// UpfTrafficRecord represents a single UPF measurement saved in MongoDB
type UpfTrafficRecord struct {
	CorrelationId string    `bson:"correlationId" json:"correlationId"`
	IpAddr        string    `bson:"ipAddr" json:"ipAddr"`
	Supi          string    `bson:"supi,omitempty" json:"supi,omitempty"`
	GroupId       string    `bson:"groupId,omitempty" json:"groupId,omitempty"`
	Dnn           string    `bson:"dnn,omitempty" json:"dnn,omitempty"`
	Timestamp     time.Time `bson:"timestamp" json:"timestamp"`
	UlVolume      int64     `bson:"ulVolume" json:"ulVolume"`
	DlVolume      int64     `bson:"dlVolume" json:"dlVolume"`
	UlThroughput  string    `bson:"ulThroughput,omitempty" json:"ulThroughput,omitempty"`
	DlThroughput  string    `bson:"dlThroughput,omitempty" json:"dlThroughput,omitempty"`
}
