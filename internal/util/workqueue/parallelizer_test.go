package workqueue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewParalle(t *testing.T) {
	optimize(100000000, 16)
	notOptimize(100000000, 16)
}

func optimize(count int, concurrent int) {
	idList := []int{}
	for i := 0; i < count; i++ {
		idList = append(idList, i)
	}

	startTime := time.Now().UnixNano()
	ParallelizeUntilOptimize(context.Background(), concurrent, len(idList), func(piece int) {
		sum := idList[piece] + 10
		_ = sum
	})
	optimCost := time.Now().UnixNano() - startTime
	_ = optimCost
	fmt.Println(optimCost)
}

func notOptimize(count int, concurrent int) {
	idList := []int{}
	for i := 0; i <= count; i++ {
		idList = append(idList, i)
	}

	startTime := time.Now().UnixNano()
	ParallelizeUntil(context.Background(), concurrent, len(idList), func(piece int) {
		sum := idList[piece] + 10
		_ = sum
	})
	cost := time.Now().UnixNano() - startTime

	fmt.Println(cost)
}

/*
func TestNewParalle(t *testing.T) {
	idList := []int{}
	for i := 0; i <= 100000000; i++ {
		idList = append(idList, i)
	}

	startTime := time.Now().UnixNano()
	ParallelizeUntilOptimize(context.Background(), 8, len(idList), func(piece int) {
		sum := idList[piece] + 10
		_ = sum
	})
	optimCost := time.Now().UnixNano() - startTime

	fmt.Println(optimCost)

	startTime = time.Now().UnixNano()
	ParallelizeUntil(context.Background(), 8, len(idList), func(piece int) {
		sum := idList[piece] + 10
		_ = sum
	})
	cost := time.Now().UnixNano() - startTime

	fmt.Println(cost)

	fmt.Println(cost / optimCost)
}
*/
