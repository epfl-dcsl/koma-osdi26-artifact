package main

const (
	START_LOAD = iota
	START_MEASURE
	REPORT_REQ
	REPLY
	TERMINATE
	CONN_OPEN
)

const (
	REPORT_THROUGHPUT = iota
	REPORT_LATENCY
)

const (
	REPLY_ACK = iota
	REPLY_STATS_THROUGHPUT
	REPLY_STATS_LATENCY
	REPLY_CONVERGENCE
	REPLY_IA_COMP
	REPLY_IID
)

type MsgHdr struct {
	MessageType   uint32
	MessageLength uint32
}

type Msg1 struct {
	Hdr  MsgHdr
	Info uint32
}

type Msg2 struct {
	Hdr   MsgHdr
	Info1 uint32
	Info2 uint32
}

type ThroughputReply struct {
	RxBytes    uint64
	TxBytes    uint64
	ReqCount   uint64
	Duration   uint64
	CorrectIAD uint64 // to avoid padding
}

type LatencyReply struct {
	ThData           ThroughputReply
	AvgLat           uint64
	P50I             uint64
	P50              uint64
	P50K             uint64
	P90I             uint64
	P90              uint64
	P90K             uint64
	P95I             uint64
	P95              uint64
	P95K             uint64
	P99I             uint64
	P99              uint64
	P99K             uint64
	P999I            uint64
	P999             uint64
	P999K            uint64
	P9999I           uint64
	P9999            uint64
	P9999K           uint64
	P99999I          uint64
	P99999           uint64
	P99999K          uint64
	P999999I         uint64
	P999999          uint64
	P999999K         uint64
	ToReduceSampling uint32
	IsIid            uint8
	IsStationary     uint8
}
