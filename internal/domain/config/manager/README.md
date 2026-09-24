# 配置管理器使用说明

## 概述

本包提供了两个主要的管理器：
1. **ConfigManager** - 配置管理器，提供高层级的配置管理功能
2. **AuthManager** - 认证管理器，专门处理设备激活和认证相关功能

## 核心特性

### ConfigManager 配置管理器
- ✅ 配置缓存机制，提高访问性能
- ✅ 配置验证功能
- ✅ 单例模式的全局管理
- ✅ 缓存清理和失效机制
- ✅ 线程安全的并发访问

### AuthManager 认证管理器
- ✅ 设备激活状态检查（通过HTTP接口）
- ✅ 实时获取激活信息（无缓存）
- ✅ 挑战码验证和HMAC安全验证
- ✅ 直接调用后端接口，确保数据实时性
- ✅ **HTTP接口集成** - 调用后端管理系统的激活接口

## HTTP接口集成

AuthManager 现在通过HTTP接口调用后端管理系统，支持以下接口：

### 1. 检查设备激活状态
```http
GET /api/internal/device/check-activation?device_id=xxx&client_id=xxx
```

### 2. 获取设备激活信息
```http
GET /api/internal/device/activation-info?device_id=xxx&client_id=xxx
```

### 3. 设备激活
```http
POST /api/internal/device/activate
Content-Type: application/json

{
  "device_id": "xxx",
  "client_id": "xxx",
  "code": "123456",
  "challenge": "uuid",
  "algorithm": "hmac-sha256",
  "serial_number": "ABC123",
  "hmac": "signature"
}
```

## 配置说明

在配置文件（config.yaml）中添加以下配置：

```yaml
manager:
  backend_url: "http://localhost:8080"  # 后端管理系统的基础URL
```

如果未配置，默认使用 `http://localhost:8080`。

## 使用示例

```go
package main

import (
    "context"
    "xiaozhi-esp32-server-golang/internal/domain/config/manager"
)

func main() {
    ctx := context.Background()
    
    // 初始化管理器
    err := manager.Init()
    if err != nil {
        panic(err)
    }
    
    err = manager.InitAuth()
    if err != nil {
        panic(err)
    }
    
    // 获取管理器实例
    configManager := manager.GetInstance()
    authManager := manager.GetAuthInstance()
    
    // 使用配置管理器
    config, err := configManager.GetUserConfig(ctx, "device_001")
    if err != nil {
        // 处理错误
    }
    
    // 使用认证管理器（通过HTTP接口）
    activated, err := authManager.IsDeviceActivated(ctx, "device_001", "client_001")
    if err != nil {
        // 处理错误
    }
    
    if !activated {
        // 获取激活信息
        code, challenge, message, timeout := authManager.GetActivationInfo(ctx, "device_001", "client_001")
        // 显示激活码给用户...
        
        // 用户输入激活码后进行验证
        activationPayload := types.ActivationPayload{
            Algorithm:    "hmac-sha256",
            SerialNumber: "ABC123",
            Challenge:    challenge,
            HMAC:         "calculated_hmac",
        }
        
        success, err := authManager.VerifyChallenge(ctx, "device_001", "client_001", fmt.Sprintf("%d", code), activationPayload)
        // 处理激活结果...
    }
}
```

## 架构优势

### 前端系统集成
- ESP32设备或其他前端系统直接调用 AuthManager
- AuthManager 内部通过HTTP调用后端管理系统
- 实现了前端系统与后端管理系统的解耦

### 实时数据
- 直接调用HTTP接口，获取最新状态
- 无缓存设计，确保数据实时性
- 简化架构，减少复杂性

### 错误处理
- 完善的错误处理和日志记录
- HTTP请求失败时的降级处理
- 详细的错误信息和调试日志

### 安全性
- 支持HMAC验证
- 安全的激活流程
- 实时状态验证

## 注意事项

1. 确保后端管理系统正在运行并且可访问
2. 正确配置 `manager.backend_url`
3. HTTP客户端默认超时为10秒
4. 无缓存模式，每次调用都会请求后端接口
5. 确保网络连接稳定，避免频繁的接口调用失败
