package webrtc_vad

import (
	"context"
	"sync"
	"testing"
	"time"

	"xiaozhi-esp32-server-golang/internal/util"
)

func TestWebRTCVADPool(t *testing.T) {
	// 创建VAD配置
	vadConfig := WebRTCVADConfig{
		SampleRate: 16000,
		Mode:       2,
	}

	// 创建池配置
	poolConfig := &util.PoolConfig{
		MaxSize:          3,
		MinSize:          1,
		MaxIdle:          2,
		AcquireTimeout:   5 * time.Second,
		IdleTimeout:      1 * time.Minute,
		ValidateOnBorrow: true,
		ValidateOnReturn: true,
	}

	// 创建VAD资源池
	pool, err := NewWebRTCVADPool(vadConfig, poolConfig)
	if err != nil {
		t.Fatalf("Failed to create WebRTC VAD pool: %v", err)
	}
	defer pool.Close()

	// 测试获取和释放VAD
	vad, err := pool.AcquireVAD()
	if err != nil {
		t.Fatalf("Failed to acquire VAD: %v", err)
	}

	// 测试VAD功能
	testData := make([]float32, 320) // 20ms的16kHz音频数据
	for i := range testData {
		testData[i] = 0.1 // 填充一些测试数据
	}

	active, err := vad.IsVAD(testData)
	if err != nil {
		t.Errorf("VAD detection failed: %v", err)
	}

	t.Logf("VAD result: %v", active)

	// 释放VAD
	err = pool.ReleaseVAD(vad)
	if err != nil {
		t.Errorf("Failed to release VAD: %v", err)
	}

	// 检查统计信息
	stats := pool.Stats()
	t.Logf("Pool stats: %+v", stats)
}

func TestWebRTCVADPoolConcurrency(t *testing.T) {
	vadConfig := WebRTCVADConfig{
		SampleRate: 16000,
		Mode:       2,
	}

	poolConfig := &util.PoolConfig{
		MaxSize:        5,
		MinSize:        2,
		MaxIdle:        3,
		AcquireTimeout: 10 * time.Second,
		IdleTimeout:    30 * time.Second,
	}

	pool, err := NewWebRTCVADPool(vadConfig, poolConfig)
	if err != nil {
		t.Fatalf("Failed to create WebRTC VAD pool: %v", err)
	}
	defer pool.Close()

	// 并发测试
	numWorkers := 10
	numIterations := 5
	var wg sync.WaitGroup

	testData := make([]float32, 320)
	for i := range testData {
		testData[i] = float32(i%100) / 100.0
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < numIterations; j++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// 获取VAD实例
				vad, err := pool.AcquireVAD()
				if err != nil {
					t.Errorf("Worker %d iteration %d: Failed to acquire VAD: %v", workerID, j, err)
					return
				}

				// 使用VAD
				_, err = vad.IsVAD(testData)
				if err != nil {
					t.Errorf("Worker %d iteration %d: VAD detection failed: %v", workerID, j, err)
				}

				// 模拟一些处理时间
				time.Sleep(10 * time.Millisecond)

				// 释放VAD
				err = pool.ReleaseVAD(vad)
				if err != nil {
					t.Errorf("Worker %d iteration %d: Failed to release VAD: %v", workerID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// 检查最终统计信息
	stats := pool.Stats()
	t.Logf("Final pool stats: %+v", stats)
}

func TestWebRTCVADFactory(t *testing.T) {
	config := WebRTCVADConfig{
		SampleRate: 16000,
		Mode:       2,
	}

	factory := NewWebRTCVADFactory(config)

	// 测试创建资源
	resource, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create resource: %v", err)
	}
	defer resource.Close()

	// 验证资源类型
	vad, ok := resource.(*WebRTCVAD)
	if !ok {
		t.Fatalf("Created resource is not WebRTCVAD type")
	}

	// 验证配置
	if vad.GetSampleRate() != config.SampleRate {
		t.Errorf("Expected sample rate %d, got %d", config.SampleRate, vad.GetSampleRate())
	}

	if vad.GetMode() != config.Mode {
		t.Errorf("Expected mode %d, got %d", config.Mode, vad.GetMode())
	}

	// 测试验证功能
	if !factory.Validate(resource) {
		t.Error("Factory validation failed for valid resource")
	}

	// 测试重置功能
	err = factory.Reset(resource)
	if err != nil {
		t.Errorf("Factory reset failed: %v", err)
	}

	// 测试资源有效性
	if !resource.IsValid() {
		t.Error("Resource should be valid after reset")
	}
}

func TestWebRTCVADPoolTimeout(t *testing.T) {
	vadConfig := WebRTCVADConfig{
		SampleRate: 16000,
		Mode:       2,
	}

	poolConfig := &util.PoolConfig{
		MaxSize:        1, // 只允许一个资源
		MinSize:        1,
		MaxIdle:        1,
		AcquireTimeout: 100 * time.Millisecond, // 短超时时间
		IdleTimeout:    1 * time.Minute,
	}

	pool, err := NewWebRTCVADPool(vadConfig, poolConfig)
	if err != nil {
		t.Fatalf("Failed to create WebRTC VAD pool: %v", err)
	}
	defer pool.Close()

	// 获取第一个VAD实例
	vad1, err := pool.AcquireVAD()
	if err != nil {
		t.Fatalf("Failed to acquire first VAD: %v", err)
	}

	// 尝试获取第二个VAD实例，应该超时
	start := time.Now()
	vad2, err := pool.AcquireVAD()
	elapsed := time.Since(start)

	if err == nil {
		pool.ReleaseVAD(vad2)
		t.Error("Expected timeout error, but got VAD instance")
	}

	if elapsed < 90*time.Millisecond {
		t.Errorf("Expected timeout around 100ms, but got %v", elapsed)
	}

	// 释放第一个VAD
	err = pool.ReleaseVAD(vad1)
	if err != nil {
		t.Errorf("Failed to release VAD: %v", err)
	}

	// 现在应该能够获取VAD
	vad3, err := pool.AcquireVAD()
	if err != nil {
		t.Errorf("Failed to acquire VAD after release: %v", err)
	}
	pool.ReleaseVAD(vad3)
}

// BenchmarkWebRTCVADPool 性能测试
func BenchmarkWebRTCVADPool(b *testing.B) {
	vadConfig := WebRTCVADConfig{
		SampleRate: 16000,
		Mode:       2,
	}

	poolConfig := &util.PoolConfig{
		MaxSize: 10,
		MinSize: 2,
		MaxIdle: 5,
	}

	pool, err := NewWebRTCVADPool(vadConfig, poolConfig)
	if err != nil {
		b.Fatalf("Failed to create WebRTC VAD pool: %v", err)
	}
	defer pool.Close()

	testData := make([]float32, 320)
	for i := range testData {
		testData[i] = float32(i%100) / 100.0
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			vad, err := pool.AcquireVAD()
			if err != nil {
				b.Errorf("Failed to acquire VAD: %v", err)
				continue
			}

			_, err = vad.IsVAD(testData)
			if err != nil {
				b.Errorf("VAD detection failed: %v", err)
			}

			pool.ReleaseVAD(vad)
		}
	})
}
