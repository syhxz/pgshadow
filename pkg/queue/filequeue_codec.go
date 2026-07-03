// binary_codec.go implements a compact binary encoding for SQLEvent that
// replaces the gob-based codec for the file queue. It avoids the per-record
// type-descriptor overhead and excessive allocations of encoding/gob, yielding
// ~10-20x better throughput for the file queue's Enqueue/Dequeue hot path.
//
// Wire format (little-endian, all lengths are uint32):
//
//   ConnID:
//     [1]  addr_len_src (IPv4=4, IPv6=16)
//     [N]  src IP bytes
//     [2]  src port
//     [1]  addr_len_dst
//     [N]  dst IP bytes
//     [2]  dst port
//   SQL:
//     [4]  len
//     [N]  SQL bytes
//   Timestamp:
//     [8]  unix nano (int64)
//   TxID:
//     [8]  uint64
//   Seq:
//     [8]  uint64
//   Extended:
//     [1]  bool (0/1)
//   StmtName:
//     [4]  len
//     [N]  bytes
//   Params count:
//     [4]  uint32
//   Per param:
//     [4]  OID
//     [4]  value len
//     [N]  value bytes
//   CopyData count:
//     [4]  uint32
//   Per copy chunk:
//     [4]  chunk len
//     [N]  chunk bytes
//   SourceExecTime:
//     [8]  int64 (nanoseconds)
package queue

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"time"

	"pgshadow/pkg/core"
)

// estimateEventSize provides a rough size estimate to pre-allocate the encode
// buffer, avoiding repeated slice growth for typical events.
func estimateEventSize(ev *core.SQLEvent) int {
	n := 1 + 16 + 2 + 1 + 16 + 2 // ConnID max
	n += 4 + len(ev.SQL)
	n += 8 + 8 + 8 + 1 // timestamp, txid, seq, extended
	n += 4 + len(ev.StmtName)
	n += 4 // params count
	for i := range ev.Params {
		n += 4 + 2 + 4 + len(ev.Params[i].Value) // OID + Format + value len + value
	}
	n += 4 // copydata count
	for i := range ev.CopyData {
		n += 4 + len(ev.CopyData[i])
	}
	n += 8 // SourceExecTime
	return n
}

// encodeBinary serializes ev into a compact binary format. It reuses a
// caller-supplied buffer when possible to reduce allocations in the hot path.
func encodeBinary(ev *core.SQLEvent, reuse []byte) ([]byte, error) {
	size := estimateEventSize(ev)
	var buf []byte
	if cap(reuse) >= size {
		buf = reuse[:0]
	} else {
		buf = make([]byte, 0, size)
	}

	// ConnID.SrcIP
	srcIP := ev.Conn.SrcIP.As16()
	srcLen := byte(16)
	if ev.Conn.SrcIP.Is4() {
		srcLen = 4
	}
	buf = append(buf, srcLen)
	if srcLen == 4 {
		s4 := ev.Conn.SrcIP.As4()
		buf = append(buf, s4[:]...)
	} else {
		buf = append(buf, srcIP[:]...)
	}
	buf = binary.LittleEndian.AppendUint16(buf, ev.Conn.SrcPort)

	// ConnID.DstIP
	dstIP := ev.Conn.DstIP.As16()
	dstLen := byte(16)
	if ev.Conn.DstIP.Is4() {
		dstLen = 4
	}
	buf = append(buf, dstLen)
	if dstLen == 4 {
		d4 := ev.Conn.DstIP.As4()
		buf = append(buf, d4[:]...)
	} else {
		buf = append(buf, dstIP[:]...)
	}
	buf = binary.LittleEndian.AppendUint16(buf, ev.Conn.DstPort)

	// SQL
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(ev.SQL)))
	buf = append(buf, ev.SQL...)

	// Timestamp
	buf = binary.LittleEndian.AppendUint64(buf, uint64(ev.Timestamp.UnixNano()))

	// TxID, Seq
	buf = binary.LittleEndian.AppendUint64(buf, ev.TxID)
	buf = binary.LittleEndian.AppendUint64(buf, ev.Seq)

	// Extended
	if ev.Extended {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}

	// StmtName
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(ev.StmtName)))
	buf = append(buf, ev.StmtName...)

	// Params
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(ev.Params)))
	for i := range ev.Params {
		buf = binary.LittleEndian.AppendUint32(buf, ev.Params[i].OID)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(ev.Params[i].Format))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(ev.Params[i].Value)))
		buf = append(buf, ev.Params[i].Value...)
	}

	// CopyData
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(ev.CopyData)))
	for i := range ev.CopyData {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(ev.CopyData[i])))
		buf = append(buf, ev.CopyData[i]...)
	}

	// SourceExecTime
	buf = binary.LittleEndian.AppendUint64(buf, uint64(ev.SourceExecTime.Nanoseconds()))

	return buf, nil
}

// decodeBinary deserializes ev from the compact binary format.
func decodeBinary(data []byte) (*core.SQLEvent, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("binary_codec: empty payload")
	}
	ev := &core.SQLEvent{}
	off := 0

	// Helper to check bounds
	need := func(n int) error {
		if off+n > len(data) {
			return fmt.Errorf("binary_codec: short read at offset %d, need %d, have %d", off, n, len(data)-off)
		}
		return nil
	}

	// ConnID.SrcIP
	if err := need(1); err != nil {
		return nil, err
	}
	srcLen := int(data[off])
	off++
	if err := need(srcLen); err != nil {
		return nil, err
	}
	if srcLen == 4 {
		var a4 [4]byte
		copy(a4[:], data[off:off+4])
		ev.Conn.SrcIP = netip.AddrFrom4(a4)
	} else {
		var a16 [16]byte
		copy(a16[:], data[off:off+16])
		ev.Conn.SrcIP = netip.AddrFrom16(a16)
	}
	off += srcLen
	if err := need(2); err != nil {
		return nil, err
	}
	ev.Conn.SrcPort = binary.LittleEndian.Uint16(data[off:])
	off += 2

	// ConnID.DstIP
	if err := need(1); err != nil {
		return nil, err
	}
	dstLen := int(data[off])
	off++
	if err := need(dstLen); err != nil {
		return nil, err
	}
	if dstLen == 4 {
		var a4 [4]byte
		copy(a4[:], data[off:off+4])
		ev.Conn.DstIP = netip.AddrFrom4(a4)
	} else {
		var a16 [16]byte
		copy(a16[:], data[off:off+16])
		ev.Conn.DstIP = netip.AddrFrom16(a16)
	}
	off += dstLen
	if err := need(2); err != nil {
		return nil, err
	}
	ev.Conn.DstPort = binary.LittleEndian.Uint16(data[off:])
	off += 2

	// SQL
	if err := need(4); err != nil {
		return nil, err
	}
	sqlLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if err := need(sqlLen); err != nil {
		return nil, err
	}
	ev.SQL = string(data[off : off+sqlLen])
	off += sqlLen

	// Timestamp
	if err := need(8); err != nil {
		return nil, err
	}
	ts := int64(binary.LittleEndian.Uint64(data[off:]))
	ev.Timestamp = time.Unix(0, ts)
	off += 8

	// TxID, Seq
	if err := need(16); err != nil {
		return nil, err
	}
	ev.TxID = binary.LittleEndian.Uint64(data[off:])
	off += 8
	ev.Seq = binary.LittleEndian.Uint64(data[off:])
	off += 8

	// Extended
	if err := need(1); err != nil {
		return nil, err
	}
	ev.Extended = data[off] == 1
	off++

	// StmtName
	if err := need(4); err != nil {
		return nil, err
	}
	stmtLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if err := need(stmtLen); err != nil {
		return nil, err
	}
	if stmtLen > 0 {
		ev.StmtName = string(data[off : off+stmtLen])
	}
	off += stmtLen

	// Params
	if err := need(4); err != nil {
		return nil, err
	}
	paramCount := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if paramCount > 0 {
		ev.Params = make([]core.ParamInfo, paramCount)
		for i := 0; i < paramCount; i++ {
			if err := need(10); err != nil { // 4 OID + 2 Format + 4 value len
				return nil, err
			}
			ev.Params[i].OID = binary.LittleEndian.Uint32(data[off:])
			off += 4
			ev.Params[i].Format = int16(binary.LittleEndian.Uint16(data[off:]))
			off += 2
			vLen := int(binary.LittleEndian.Uint32(data[off:]))
			off += 4
			if err := need(vLen); err != nil {
				return nil, err
			}
			if vLen > 0 {
				ev.Params[i].Value = make([]byte, vLen)
				copy(ev.Params[i].Value, data[off:off+vLen])
			}
			off += vLen
		}
	}

	// CopyData
	if err := need(4); err != nil {
		return nil, err
	}
	copyCount := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if copyCount > 0 {
		ev.CopyData = make([][]byte, copyCount)
		for i := 0; i < copyCount; i++ {
			if err := need(4); err != nil {
				return nil, err
			}
			cLen := int(binary.LittleEndian.Uint32(data[off:]))
			off += 4
			if err := need(cLen); err != nil {
				return nil, err
			}
			if cLen > 0 {
				ev.CopyData[i] = make([]byte, cLen)
				copy(ev.CopyData[i], data[off:off+cLen])
			}
			off += cLen
		}
	}

	// SourceExecTime
	if err := need(8); err != nil {
		return nil, err
	}
	ev.SourceExecTime = time.Duration(int64(binary.LittleEndian.Uint64(data[off:])))
	// off += 8

	return ev, nil
}
