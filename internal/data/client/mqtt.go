package client

import (
	msg "xiaozhi-esp32-server-golang/internal/data/msg"
)

const (
	DeviceMockPubTopicPrefix = msg.MDeviceMockPubTopicPrefix
	DeviceMockSubTopicPrefix = msg.MDeviceMockSubTopicPrefix
	DeviceSubTopicPrefix     = msg.MDeviceSubTopicPrefix
	DevicePubTopicPrefix     = msg.MDevicePubTopicPrefix
	ServerSubTopicPrefix     = msg.MServerSubTopicPrefix
	ServerPubTopicPrefix     = msg.MServerPubTopicPrefix
)

const (
	ClientActiveTs = 120
)
