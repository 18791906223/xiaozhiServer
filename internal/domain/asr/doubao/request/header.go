package request

import (
	"bytes"
	"net/http"

	"xiaozhi-esp32-server-golang/internal/domain/asr/doubao/common"
	"xiaozhi-esp32-server-golang/internal/domain/doubaoapi"
)

type AsrRequestHeader struct {
	messageType              common.MessageType
	messageTypeSpecificFlags common.MessageTypeSpecificFlags
	serializationType        common.SerializationType
	compressionType          common.CompressionType
	reservedData             []byte
}

func (h *AsrRequestHeader) toBytes() []byte {
	header := bytes.NewBuffer([]byte{})
	header.WriteByte(byte(common.PROTOCOL_VERSION<<4 | 1))
	header.WriteByte(byte(h.messageType<<4) | byte(h.messageTypeSpecificFlags))
	header.WriteByte(byte(h.serializationType<<4) | byte(h.compressionType))
	header.Write(h.reservedData)
	return header.Bytes()
}

func (h *AsrRequestHeader) WithMessageType(messageType common.MessageType) *AsrRequestHeader {
	h.messageType = messageType
	return h
}

func (h *AsrRequestHeader) WithMessageTypeSpecificFlags(messageTypeSpecificFlags common.MessageTypeSpecificFlags) *AsrRequestHeader {
	h.messageTypeSpecificFlags = messageTypeSpecificFlags
	return h
}

func (h *AsrRequestHeader) WithSerializationType(serializationType common.SerializationType) *AsrRequestHeader {
	h.serializationType = serializationType
	return h
}

func (h *AsrRequestHeader) WithCompressionType(compressionType common.CompressionType) *AsrRequestHeader {
	h.compressionType = compressionType
	return h
}

func (h *AsrRequestHeader) WithReservedData(reservedData []byte) *AsrRequestHeader {
	h.reservedData = reservedData
	return h
}

func DefaultHeader() *AsrRequestHeader {
	return &AsrRequestHeader{
		messageType:              common.CLIENT_FULL_REQUEST,
		messageTypeSpecificFlags: common.POS_SEQUENCE,
		serializationType:        common.JSON,
		compressionType:          common.GZIP,
		reservedData:             []byte{0x00},
	}
}

func NewAuthHeader(appKey, accessKey, resourceID, connectID string) http.Header {
	return doubaoapi.NewASRHeaders(appKey, accessKey, resourceID, connectID)
}
