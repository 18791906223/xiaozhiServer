package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"xiaozhi-esp32-server-golang/internal/app/server"
	user_config "xiaozhi-esp32-server-golang/internal/domain/config"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/spf13/viper"
)

func main() {
	// 解析命令行参数
	configFile := flag.String("c", defaultConfigFilePath, "配置文件路径")
	managerEnable := flag.Bool("manager-enable", defaultManagerEnable, "是否启用内嵌 manager")
	managerConfig := flag.String("manager-config", "", "manager 配置文件路径，启用时可选，默认 manager/backend/config/config.json")
	asrEnable := flag.Bool("asr-enable", defaultAsrEnable, "是否启用内嵌 asr_server")
	asrConfig := flag.String("asr-config", "", "asr_server 配置文件路径，启用时可选，默认 asr_server/config.json")
	flag.Parse()
	// 配置文件路径为空，退出
	if *configFile == "" {
		fmt.Println("配置文件路径不能为空")
		return
	}

	// 先启动 manager，再 Init，否则 Init 里 updateConfigFromAPI 会一直连不上 manager 导致卡死
	// 是否启动后端程序
	if *managerEnable {
		// 启动后台程序
		StartManagerHTTP(*managerConfig)
	}
	// 启动声纹服务
	if *asrEnable {
		// 启动声纹服务
		StartAsrServerHTTP(*asrConfig)
	}
	// 初始化主服务
	err := Init(*configFile)
	if err != nil {
		return
	}

	// 根据配置启动 pprof 服务
	// 是否启动服务器的性能检测
	if viper.GetBool("server.pprof.enable") {
		// 端口
		pprofPort := viper.GetInt("server.pprof.port")
		// 通过go线程启动服务
		go func() {
			log.Infof("启动 pprof 服务，端口: %d", pprofPort)
			if err := http.ListenAndServe(fmt.Sprintf(":%d", pprofPort), nil); err != nil {
				log.Errorf("pprof 服务启动失败: %v", err)
			}
		}()
		log.Infof("pprof 地址: http://localhost:%d/debug/pprof/", pprofPort)
	} else {
		log.Info("pprof 服务已禁用")
	}

	// 创建服务器
	appInstance := server.NewApp()
	// 创建一个锁
	var lock sync.RWMutex
	// 注册 system_config 热更：用 viper 当前配置与推送配置对比，仅当内容变更时合并并触发热更
	user_config.RegisterManagerSystemConfigHandler(func(data map[string]interface{}) {
		lock.Lock()
		defer lock.Unlock()
		current := viper.AllSettings()
		oldMqttServer := current["mqtt_server"]
		oldMqtt := current["mqtt"]
		oldUdp := current["udp"]
		oldMcp := current["mcp"]
		oldLocalMcp := current["local_mcp"]

		var doMqttServer, doMqttReload, doUdpReload, doMcpReload bool
		if data["mqtt_server"] != nil {
			if !SystemConfigEqual(data["mqtt_server"], oldMqttServer) {
				doMqttServer = true
			}
		}
		if data["mqtt"] != nil {
			if !SystemConfigEqual(data["mqtt"], oldMqtt) {
				doMqttReload = true
			}
		}
		if data["udp"] != nil {
			if udpListenChanged(data["udp"], oldUdp) {
				doUdpReload = true
			}
		}
		if data["mcp"] != nil {
			if !SystemConfigEqual(data["mcp"], oldMcp) {
				doMcpReload = true
			}
		}
		if data["local_mcp"] != nil {
			if !SystemConfigEqual(data["local_mcp"], oldLocalMcp) {
				doMcpReload = true
			}
		}

		ApplySystemConfigToViper(data)

		var wg sync.WaitGroup
		if doMqttServer {
			wg.Add(1)
			go func() {
				defer wg.Done()
				appInstance.ReloadMqttServer()
			}()
		}
		if doMqttReload || doUdpReload {
			wg.Add(1)
			go func() {
				defer wg.Done()
				appInstance.ReloadMqttUdpWithFlags(doMqttReload, doUdpReload)
			}()
		}
		if doMcpReload {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := appInstance.ReloadMCP(); err != nil {
					log.Errorf("ReloadMCP failed: %v", err)
				}
			}()
		}
		wg.Wait()
	})
	appInstance.Run()

	// 阻塞监听退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Info("服务器已启动，按 Ctrl+C 退出")
	<-quit

	log.Info("正在关闭服务器...")

	// 停止周期性配置更新服务
	StopPeriodicConfigUpdate()
	if *managerEnable {
		StopManagerHTTP()
	}
	if *asrEnable {
		StopAsrServerHTTP()
	}

	log.Info("服务器已关闭")
}

func udpListenChanged(newUdpCfg interface{}, oldUdpCfg interface{}) bool {
	newListenHost, newListenPort := udpListenHostPort(newUdpCfg)
	oldListenHost, oldListenPort := udpListenHostPort(oldUdpCfg)
	if newListenHost == "" && newListenPort == 0 {
		return false
	}
	return newListenHost != oldListenHost || newListenPort != oldListenPort
}

func udpListenHostPort(cfg interface{}) (string, int) {
	if cfg == nil {
		return "", 0
	}
	type udpListen struct {
		ListenHost string `json:"listen_host"`
		ListenPort int    `json:"listen_port"`
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return "", 0
	}
	var parsed udpListen
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", 0
	}
	return parsed.ListenHost, parsed.ListenPort
}
