package context

import "time"

const UpfTrafficDataColl = "nwdaf.upfTrafficData"

type UpfTrafficMetaData struct {
	IpAddr        string `bson:"ipAddr" json:"ipAddr"`
	CorrelationId string `bson:"correlationId" json:"correlationId"`
	Supi          string `bson:"supi,omitempty" json:"supi,omitempty"`
	GroupId       string `bson:"groupId,omitempty" json:"groupId,omitempty"`
	Dnn           string `bson:"dnn,omitempty" json:"dnn,omitempty"`
}

// UpfTrafficRecord represents a single UPF measurement saved in MongoDB Time Series
type UpfTrafficRecord struct {
	Metadata     UpfTrafficMetaData `bson:"metadata" json:"metadata"`
	Timestamp    time.Time          `bson:"timestamp" json:"timestamp"`
	UlVolume     int64              `bson:"ulVolume" json:"ulVolume"`
	DlVolume     int64              `bson:"dlVolume" json:"dlVolume"`
	UlThroughput string             `bson:"ulThroughput,omitempty" json:"ulThroughput,omitempty"`
	DlThroughput string             `bson:"dlThroughput,omitempty" json:"dlThroughput,omitempty"`
}
