package mqtt_server

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	mqttServer "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/spf13/viper"

	msg "xiaozhi-esp32-server-golang/internal/data/msg"
	log "xiaozhi-esp32-server-golang/logger"
)

var (
	currentServer *mqttServer.Server
	serverMu      sync.Mutex
)

// StartMqttServer 启动 MQTT 服务器（可被 StopMqttServer 后再次调用以热更）
func StartMqttServer() error {
	serverMu.Lock()
	defer serverMu.Unlock()
	if currentServer != nil {
		return errors.New("mqtt_server 已在运行，请先 StopMqttServer")
	}
	srv := mqttServer.New(&mqttServer.Options{
		InlineClient: true,
	})

	if err := srv.AddHook(&AuthHook{}, nil); err != nil {
		log.Errorf("添加 AuthHook 失败: %v", err)
		return err
	}
	deviceHook := &DeviceHook{
		server: srv,
		publishLifecycle: func(event msg.MqttLifecycleEvent) error {
			payload, err := json.Marshal(event)
			if err != nil {
				return err
			}
			return srv.Publish(msg.MDeviceLifecycleTopic, payload, false, 0)
		},
	}
	if err := srv.AddHook(deviceHook, nil); err != nil {
		log.Errorf("添加 DeviceHook 失败: %v", err)
		return err
	}

	if viper.GetBool("mqtt_server.tls.enable") {
		pemFile := viper.GetString("mqtt_server.tls.pem")
		keyFile := viper.GetString("mqtt_server.tls.key")
		cert, err := tls.LoadX509KeyPair(pemFile, keyFile)
		if err != nil {
			log.Errorf("加载证书失败: %v", err)
			return err
		}
		tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
		ssltcp := listeners.NewTCP(listeners.Config{
			ID:        "ssl",
			Address:   fmt.Sprintf(":%d", viper.GetInt("mqtt_server.tls.port")),
			TLSConfig: tlsConfig,
		})
		if err := srv.AddListener(ssltcp); err != nil {
			return err
		}
	}

	host := viper.GetString("mqtt_server.listen_host")
	port := viper.GetInt("mqtt_server.listen_port")
	if port == 0 {
		return errors.New("mqtt_server.port 配置错误，请检查配置文件")
	}
	address := fmt.Sprintf("%s:%d", host, port)
	tcp := listeners.NewTCP(listeners.Config{Type: "tcp", ID: "t1", Address: address})
	if err := srv.AddListener(tcp); err != nil {
		return err
	}

	currentServer = srv
	log.Infof("MQTT 服务器启动，监听 %s 地址...", address)
	go func() {
		// Serve() 在库内启动 listener 协程后即返回，不会阻塞，故不在此处清 currentServer
		if err := srv.Serve(); err != nil {
			log.Warnf("MQTT Server Serve 退出: %v", err)
		}
	}()
	return nil
}

// StopMqttServer 停止当前 MQTT 服务器，便于热更后重新 StartMqttServer
func StopMqttServer() error {
	log.Infof("enter StopMqttServer ")
	defer log.Infof("exit StopMqttServer ")
	serverMu.Lock()
	defer serverMu.Unlock()
	srv := currentServer
	if srv == nil {
		return nil
	}
	// 将 Close 纳入同一临界区，避免并发 Stop 对同一实例重复调用 Close。
	if err := srv.Close(); err != nil {
		log.Warnf("StopMqttServer Close: %v", err)
		return err
	}
	currentServer = nil
	log.Info("MQTT 服务器已停止")
	return nil
}
