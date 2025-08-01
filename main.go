package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
)

func abs32(x int32) int32 {
	if x < 0 {
		return -x
	}
	return x
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

type stationStats struct {
	min   int32
	max   int32
	sum   int64
	count int32
}

func parseTemperatureFromBytes(data []byte, start, end int) (int32, bool) {
	if start >= end {
		return 0, false
	}

	sign := int32(1)
	i := start
	if data[i] == '-' {
		sign = -1
		i++
	}

	intPart := int32(0)
	for ; i < end; i++ {
		c := data[i]
		if c == '.' {
			i++
			break
		}
		if c < '0' || c > '9' {
			return 0, false
		}
		intPart = intPart*10 + int32(c-'0')
	}

	fracPart := int32(0)
	if i < end {
		c := data[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		fracPart = int32(c - '0')
	}

	return sign * (intPart*10 + fracPart), true
}

type stationIntern struct {
	nameToID map[string]int
	idToName []string
	stats    []stationStats
}

func processChunk(data []byte, start, end int) *stationIntern {

	if start != 0 {
		for start < end && data[start-1] != '\n' {
			start++
		}
	}

	intern := &stationIntern{
		nameToID: make(map[string]int, 512),
		idToName: make([]string, 0, 512),
		stats:    make([]stationStats, 0, 512),
	}

	i := start
	for i < end {

		j := bytes.IndexByte(data[i:end], '\n')
		var line []byte
		if j == -1 {

			break
		}
		line = data[i : i+j]
		i += j + 1

		if len(line) == 0 {
			continue
		}

		semicolonIdx := -1
		for i := len(line) - 1; i >= 0; i-- {
			if line[i] == ';' {
				semicolonIdx = i
				break
			}
		}

		if semicolonIdx <= 0 || semicolonIdx >= len(line)-1 {
			continue
		}

		temp, ok := parseTemperatureFromBytes(line, semicolonIdx+1, len(line))
		if !ok {
			continue
		}

		stationBytes := line[:semicolonIdx]
		stationName := string(stationBytes)

		stationID, exists := intern.nameToID[stationName]
		if !exists {

			stationID = len(intern.idToName)
			intern.nameToID[stationName] = stationID
			intern.idToName = append(intern.idToName, stationName)
			intern.stats = append(intern.stats, stationStats{})
		}

		stats := &intern.stats[stationID]
		if stats.count == 0 {
			stats.min, stats.max = temp, temp
		} else {
			if temp < stats.min {
				stats.min = temp
			}
			if temp > stats.max {
				stats.max = temp
			}
		}
		stats.sum += int64(temp)
		stats.count++
	}

	return intern
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <measurements-file>\n", os.Args[0])
		os.Exit(1)
	}

	path := os.Args[1]
	file, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to stat file: %v\n", err)
		os.Exit(1)
	}
	fileSize := fi.Size()
	if fileSize == 0 {
		return
	}
	if fileSize > int64(^uint(0)>>1) {
		fmt.Fprintf(os.Stderr, "File too large to mmap on this platform\n")
		os.Exit(1)
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to mmap file: %v\n", err)
		os.Exit(1)
	}
	defer syscall.Munmap(data)

	workerCount := runtime.NumCPU()
	if workerCount < 1 {
		workerCount = 1
	}
	if int64(workerCount) > fileSize {
		workerCount = int(fileSize)
	}
	if workerCount == 0 {
		workerCount = 1
	}

	chunkSize := int(fileSize) / workerCount

	var wg sync.WaitGroup
	resultsCh := make(chan *stationIntern, workerCount)

	for i := 0; i < workerCount; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == workerCount-1 {
			end = int(fileSize)
		}

		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			resultsCh <- processChunk(data, s, e)
		}(start, end)
	}

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	globalStats := make(map[string]stationStats)
	for workerData := range resultsCh {

		for stationID, stats := range workerData.stats {
			stationName := workerData.idToName[stationID]
			g := globalStats[stationName]
			if g.count == 0 {
				g.min, g.max = stats.min, stats.max
			} else {
				if stats.min < g.min {
					g.min = stats.min
				}
				if stats.max > g.max {
					g.max = stats.max
				}
			}
			g.sum += stats.sum
			g.count += stats.count
			globalStats[stationName] = g
		}
	}

	stations := make([]string, 0, len(globalStats))
	for station := range globalStats {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	for _, station := range stations {
		s := globalStats[station]

		meanFixedPoint := (s.sum + int64(s.count)/2) / int64(s.count)

		minInt, minFrac := s.min/10, abs32(s.min%10)
		meanInt, meanFrac := meanFixedPoint/10, abs64(meanFixedPoint%10)
		maxInt, maxFrac := s.max/10, abs32(s.max%10)

		fmt.Printf("%s;%d.%d/%d.%d/%d.%d\n", station,
			minInt, minFrac, meanInt, meanFrac, maxInt, maxFrac)
	}
}
