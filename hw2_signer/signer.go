package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const multiHashIterCount = 6

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})

	for _, jobFunc := range jobs {
		out := make(chan interface{})
		wrapper := jobWrapper(jobFunc, wg)

		wg.Add(1)
		go wrapper(in, out)

		in = out
	}

	wg.Wait()
}

func jobWrapper(jobFunc job, wg *sync.WaitGroup) job {
	return func(in, out chan interface{}) {
		defer wg.Done()

		jobFunc(in, out)
		close(out)
	}
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for val := range in {
		data := fmt.Sprint(val)
		md5 := DataSignerMd5(data)

		wg.Add(1)
		go func() {
			defer wg.Done()

			ch1 := crc32Ch(data)
			ch2 := crc32Ch(md5)

			out <- fmt.Sprintf("%s~%s", <-ch1, <-ch2)
		}()
	}

	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for val := range in {
		data := fmt.Sprint(val)

		wg.Add(1)
		go func() {
			defer wg.Done()

			chs := make([]<-chan string, 0, multiHashIterCount)
			for th := 0; th < multiHashIterCount; th++ {
				ch := crc32Ch(strconv.Itoa(th) + data)
				chs = append(chs, ch)
			}

			parts := make([]string, 0, multiHashIterCount)
			for _, ch := range chs {
				parts = append(parts, <-ch)
			}

			out <- strings.Join(parts, "")
		}()
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	results := make([]string, 0)
	for val := range in {
		results = append(results, fmt.Sprint(val))
	}

	sort.Strings(results)
	out <- strings.Join(results, "_")
}

func crc32Ch(val string) <-chan string {
	out := make(chan string, 1)
	go func() {
		out <- DataSignerCrc32(val)
		close(out)
	}()
	return out
}
