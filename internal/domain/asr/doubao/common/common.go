package common

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

const DefaultSampleRate = 16000

type ProtocolVersion byte
type MessageType byte
type MessageTypeSpecificFlags byte
type SerializationType byte
type CompressionType byte

const (
	PROTOCOL_VERSION = ProtocolVersion(0b0001)

	// Message Type:
	CLIENT_FULL_REQUEST       = MessageType(0b0001)
	CLIENT_AUDIO_ONLY_REQUEST = MessageType(0b0010)
	SERVER_FULL_RESPONSE      = MessageType(0b1001)
	SERVER_ERROR_RESPONSE     = MessageType(0b1111)

	// Message Type Specific Flags
	NO_SEQUENCE       = MessageTypeSpecificFlags(0b0000) // no check sequence
	POS_SEQUENCE      = MessageTypeSpecificFlags(0b0001)
	NEG_SEQUENCE      = MessageTypeSpecificFlags(0b0010)
	NEG_WITH_SEQUENCE = MessageTypeSpecificFlags(0b0011)

	// Message Serialization
	NO_SERIALIZATION = SerializationType(0b0000)
	JSON             = SerializationType(0b0001)

	// Message Compression
	GZIP = CompressionType(0b0001)
)

func GzipCompress(input []byte) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(input)
	w.Close()
	return b.Bytes()
}

func GzipDecompress(input []byte) []byte {
	b := bytes.NewBuffer(input)
	r, _ := gzip.NewReader(b)
	out, _ := ioutil.ReadAll(r)
	r.Close()
	return out
}
