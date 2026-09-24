package mcp

import "github.com/mark3labs/mcp-go/mcp"

type Vision struct {
	Url   string `json:"url"`
	Token string `json:"token"`
}

type InitializeParams struct {
	// The latest version of the Model Context Protocol that the client supports.
	// The client MAY decide to support older versions as well.
	ProtocolVersion string                 `json:"protocolVersion"`
	Capabilities    map[string]interface{} `json:"capabilities"`
	ClientInfo      mcp.Implementation     `json:"clientInfo"`
}
