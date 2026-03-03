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
	Metadata  UpfTrafficMetaData `bson:"metadata" json:"metadata"`
	Timestamp time.Time          `bson:"timestamp" json:"timestamp"`

	// Volume measurements (TS 29.564 VolumeMeasurement)
	TotalVolume      int64  `bson:"totalVolume,omitempty" json:"totalVolume,omitempty"`
	UlVolume         int64  `bson:"ulVolume,omitempty" json:"ulVolume,omitempty"`
	DlVolume         int64  `bson:"dlVolume,omitempty" json:"dlVolume,omitempty"`
	TotalNbOfPackets uint64 `bson:"totalNbOfPackets,omitempty" json:"totalNbOfPackets,omitempty"`
	UlNbOfPackets    uint64 `bson:"ulNbOfPackets,omitempty" json:"ulNbOfPackets,omitempty"`
	DlNbOfPackets    uint64 `bson:"dlNbOfPackets,omitempty" json:"dlNbOfPackets,omitempty"`

	// Throughput measurements (TS 29.564 ThroughputMeasurement)
	// Received as TS29571 string, stored as base units after parsing
	UlThroughput       float64 `bson:"ulThroughput,omitempty" json:"ulThroughput,omitempty"`             // bps
	DlThroughput       float64 `bson:"dlThroughput,omitempty" json:"dlThroughput,omitempty"`             // bps
	UlPacketThroughput float64 `bson:"ulPacketThroughput,omitempty" json:"ulPacketThroughput,omitempty"` // pps
	DlPacketThroughput float64 `bson:"dlPacketThroughput,omitempty" json:"dlPacketThroughput,omitempty"` // pps
}
