package main

import (
	"bufio"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/hatlonely/go-kit/flag"
	"github.com/hatlonely/go-kit/refx"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup //定义一个同步等待的组

func producer(file string, ch chan string) {
	defer wg.Done()
	defer close(ch)
	// 创建句柄
	fi, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer func(fi *os.File) {
		err := fi.Close()
		if err != nil {
			panic(err)
		}
	}(fi)

	readNum := 0
	// 创建 Reader
	r := bufio.NewReader(fi)
	for {
		lineBytes, err := r.ReadBytes('\n')
		if err != nil && err != io.EOF {
			fmt.Println(err.Error())
			continue
		}
		line := strings.TrimSpace(string(lineBytes))
		readNum += 1
		if err == io.EOF {
			break
		}
		ch <- line
	}
	fmt.Printf("read groutine read %d records.\n", readNum)
}

func consumer(idx int, split string, cli *redis.Client, ch chan string) {
	defer wg.Done()
	total, errNum := 0, 0
	// ticker := time.Tick(time.Minute)
	var quit bool
	for {
		select {
		//case <- ticker:
		//	fmt.Printf("groutine[%d] deal %d record, %d errors\n", idx, total, errNum)
		case line := <-ch:
			arrays := strings.Split(line, split)
			if len(arrays) != 4 {
				continue
			}
			fmt.Printf("groutine[%d] hset %s, %s, %s\n", idx, arrays[1], arrays[2], arrays[3])
			res, err := cli.HSet(arrays[1], arrays[2], arrays[3]).Result()
			if err != nil {
				errNum += 1
				fmt.Println(res, err.Error())
			}
			total += 1
		default:
			fmt.Printf("groutine[%d] deal %d record, %d errors, exit...\n", idx, total, errNum)
			quit = true
			break
		}
		if quit {
			break
		}
	}
}

type Options struct {
	// 0 解密 1 加密 2 生成token
	Addr     string `flag:"--addr; usage: redis host:port; default: 127.0.0.1:6379"`
	Password string `flag:"--password; usage: redis password;"`
	Num      int    `flag:"--num; usage: 并发数; default: 1"`
	Split string `flag:"--split; usage: 分隔符;"`
	// FileType string `flag:"file_type; usage: 文件类型"`
	FileName string `flag:"file_name; usage: 文件名"`
}

func main() {
	if err := flag.Struct(&Options{}, refx.WithCamelName()); err != nil {
		panic(err)
	}
	if err := flag.Parse(); err != nil {
		fmt.Println(flag.Usage())
		return
	}

	addr := flag.GetString("addr")
	password := flag.GetString("password")
	num := flag.GetInt("num")
	split := flag.GetStringD("split", " ")
	fmt.Printf("分隔符split是 [%s]\n", split)

	// fileType := flag.GetString("file_type")
	fileName := flag.GetString("file_name")

	rds := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		MaxRetries:   1,
		MinIdleConns: 1,
		PoolSize: num,
	})
	if _, err := rds.Ping().Result(); err != nil {
		panic(err)
	}

	//用channel来传递"产品", 不再需要自己去加锁维护一个全局的阻塞队列
	ch := make(chan string, 10000)
	go producer(fileName, ch)
	wg.Add(1)
	time.Sleep(time.Second)
	for i := 0; i < num; i++ {
		fmt.Printf("run groutine[%d]\n", i)
		go consumer(i, split, rds, ch)
		wg.Add(1)
	}
	wg.Wait()
}
