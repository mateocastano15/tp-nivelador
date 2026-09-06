package client

import (
	"bytes"
	"encoding/binary"
)

const ENDOFBETS_BYTES = 2
const BET_BYTES = 1
const ACK_BYTES = 3
const AGENCYID_BYTES = 1
const FIRSTNAME_BYTES = 2
const LASTNAME_BYTES = 3
const DOCUMENT_BYTES = 4
const BIRTHDATE_BYTES = 5
const NUMBER_BYTES = 6
const BATCH_BYTES = 4
const TYPE_BYTES = 2
const SIZE_BYTES = 2

type Message interface {
	Type() int
}

type ErrResponseMismatch struct{}

func (ErrResponseMismatch) Error() string {
	return "received response does not match expected message type"
}

type Bet struct {
	agencyId  []byte
	firstName []byte
	lastName  []byte
	document  []byte
	birthdate []byte
	number    []byte
	byteBet   []byte
}

func (bet *Bet) Type() int {
	return BET_BYTES
}

type Ack struct{}

func (ack Ack) Type() int {
	return ACK_BYTES
}

type Batch struct {
	bets []*Bet
}

func (Batch) Type() int {
	return BATCH_BYTES
}

func encodeField(fieldType int, value []byte) []byte {
	header := make([]byte, TYPE_BYTES+SIZE_BYTES)
	binary.BigEndian.PutUint16(header[:TYPE_BYTES], uint16(fieldType))
	binary.BigEndian.PutUint16(header[TYPE_BYTES:], uint16(len(value)))
	return append(header, value...)
}

func createBet(data []byte, agencyId string) *Bet {
	splitData := bytes.Split(data, []byte(","))
	firstName := splitData[0]
	lastName := splitData[1]
	document := splitData[2]
	birthdate := splitData[3]
	number := splitData[4]

	value := encodeField(AGENCYID_BYTES, []byte(agencyId))
	value = append(value, encodeField(FIRSTNAME_BYTES, firstName)...)
	value = append(value, encodeField(LASTNAME_BYTES, lastName)...)
	value = append(value, encodeField(DOCUMENT_BYTES, document)...)
	value = append(value, encodeField(BIRTHDATE_BYTES, birthdate)...)
	value = append(value, encodeField(NUMBER_BYTES, number)...)

	byteBet := encodeField(BET_BYTES, value)

	return &Bet{
		agencyId:  []byte(agencyId),
		firstName: firstName,
		lastName:  lastName,
		document:  document,
		birthdate: birthdate,
		number:    number,
		byteBet:   byteBet,
	}
}

func splitMsg(data []byte) (msgType []byte, msg []byte, rest []byte) {
	msgType, size := parseHeader(data[:TYPE_BYTES+SIZE_BYTES])
	totalLen := TYPE_BYTES + SIZE_BYTES + size
	return msgType, data[:totalLen], data[totalLen:]
}

func decodeBet(msg []byte) *Bet {
	value := msg[TYPE_BYTES+SIZE_BYTES:]
	bet := &Bet{byteBet: msg}
	for len(value) > 0 {
		fieldType, fieldMsg, rest := splitMsg(value)
		fieldValue := fieldMsg[TYPE_BYTES+SIZE_BYTES:]
		if fieldType[0] == 0 && fieldType[1] == AGENCYID_BYTES {
			bet.agencyId = fieldValue
		} else if fieldType[0] == 0 && fieldType[1] == FIRSTNAME_BYTES {
			bet.firstName = fieldValue
		} else if fieldType[0] == 0 && fieldType[1] == LASTNAME_BYTES {
			bet.lastName = fieldValue
		} else if fieldType[0] == 0 && fieldType[1] == DOCUMENT_BYTES {
			bet.document = fieldValue
		} else if fieldType[0] == 0 && fieldType[1] == BIRTHDATE_BYTES {
			bet.birthdate = fieldValue
		} else if fieldType[0] == 0 && fieldType[1] == NUMBER_BYTES {
			bet.number = fieldValue
		}
		value = rest
	}
	return bet
}

func decodeBatch(value []byte) *Batch {
	var bets []*Bet
	for len(value) > 0 {
		_, msg, rest := splitMsg(value)
		bets = append(bets, decodeBet(msg))
		value = rest
	}
	return &Batch{bets: bets}
}

func parseHeader(header []byte) (msgType []byte, size int) {
	msgType = header[:TYPE_BYTES]
	size = int(binary.BigEndian.Uint16(header[TYPE_BYTES:]))
	return msgType, size
}

func encodeBatch(bets []*Bet) []byte {
	var value []byte
	for _, bet := range bets {
		value = append(value, bet.byteBet...)
	}
	return encodeField(BATCH_BYTES, value)
}

func parseMessage(header []byte, value []byte) (Message, error) {
	msgType := header[:TYPE_BYTES]

	if msgType[0] == 0 && msgType[1] == BET_BYTES {
		return &Bet{byteBet: append(header, value...)}, nil
	} else if msgType[0] == 0 && msgType[1] == ACK_BYTES {
		return Ack{}, nil
	} else if msgType[0] == 0 && msgType[1] == BATCH_BYTES {
		return decodeBatch(value), nil
	}

	return nil, nil
}
