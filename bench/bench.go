package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/yplusplus/beecask"
	"github.com/yplusplus/ylog"
)

var (
	removeAll = os.RemoveAll
)

const (
	benchDir    = "./bc_bench"
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

var (
	bc            *beecask.Beecask
	keys          []string
	vsize         int
	operationsNum int
)

func GenerateKeys() {
	keys = make([]string, operationsNum)
	for i := 0; i < operationsNum; i++ {
		keys[i] = fmt.Sprintf("%9d", i)
	}
}

func RandomSetBench(num int) {
	value := make([]byte, vsize)
	begin := time.Now()
	j := 0
	for i := 0; i < num; i++ {
		if err := bc.Set(keys[j], value); err != nil {
			ylog.Fatalf("Set Record[key:%s] failed, err=%s", keys[j], err)
		}
		j++
		if j == len(keys) {
			j = 0
		}
	}
	end := time.Now()
	d := end.Sub(begin)
	fmt.Printf("%d set operation[key: %dB, value: %dB] in %fs\n", num, len(keys[0]), vsize, d.Seconds())
	fmt.Printf("average %f qps\n", float64(num)/d.Seconds())
	writeMB := int64(num) * int64(vsize) / 1e6
	fmt.Printf("average %f MB/s\n", float64(writeMB)/d.Seconds())
	fmt.Printf("average %f micros/op\n", d.Seconds()*1e6/float64(num))
}

func RandomGetBench(num int) {
	begin := time.Now()
	for i := 0; i < num; i++ {
		r := rand.Intn(len(keys))
		if _, err := bc.Get(keys[r]); err != nil {
			ylog.Fatalf("Get Record[key:%s] failed", keys[r])
		}
	}
	end := time.Now()
	d := end.Sub(begin)
	fmt.Printf("%d get operation in %fs\n", num, d.Seconds())
	fmt.Printf("average %f qps\n", float64(num)/d.Seconds())
	fmt.Printf("average %f micros/op\n", d.Seconds()*1e6/float64(num))
}

func RandomSetBenchWhenMerge() {
	fmt.Println("RandomSetBenchWhenMerge:")
	done := make(chan bool, 1)
	go func() {
		bc.Merge()
		done <- true
	}()
	RandomSetBench(operationsNum)
	<-done
}

func RandomGetBenchWhenMerge() {
	fmt.Println("RandomGetBenchWhenMerge:")
	done := make(chan bool, 1)
	go func() {
		bc.Merge()
		done <- true
	}()
	RandomGetBench(operationsNum)
	<-done
}

func init() {
	flag.IntVar(&vsize, "value-size", 2048, "value size")
	flag.IntVar(&operationsNum, "op-num", 1e5, "operations number")
}

func main() {
	// init ylog
	flag.Parse()
	ylog.Init()
	defer ylog.Flush()

	// clear DB
	//os.RemoveAll(benchDir)

	options := beecask.NewOptions()
	options.MaxOpenFiles = 256
	var err error
	bc, err = beecask.NewBeecask(*options, benchDir)
	if err != nil {
		fmt.Print(err)
		ylog.Fatal(err)
	}
	defer bc.Close()

	GenerateKeys()

	fmt.Println("RandomSetBench:")
	RandomSetBench(operationsNum)

	fmt.Println("RandomGetBench:")
	RandomGetBench(operationsNum)

	RandomGetBenchWhenMerge()

	RandomSetBenchWhenMerge()

	mergeBegin := time.Now()
	bc.Merge()
	mergeEnd := time.Now()
	fmt.Printf("Merge in %fs\n", mergeEnd.Sub(mergeBegin).Seconds())
}
