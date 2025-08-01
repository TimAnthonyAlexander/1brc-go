package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
)

type stationStats struct {
	min   float64
	max   float64
	sum   float64
	count int
}

func parseTemperature(buf []byte) (float64, bool) {
	if len(buf) == 0 {
		return 0, false
	}

	sign := 1.0
	i := 0
	if buf[0] == '-' {
		sign = -1.0
		i++
	}

	intPart := 0.0
	for ; i < len(buf); i++ {
		c := buf[i]
		if c == '.' {
			i++
			break
		}
		if c < '0' || c > '9' {
			return 0, false
		}
		intPart = intPart*10 + float64(c-'0')
	}

	fracPart := 0.0
	if i < len(buf) {
		c := buf[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		fracPart = float64(c-'0') / 10.0
	}

	return sign * (intPart + fracPart), true
}

func processChunk(file *os.File, offset, length int64) map[string]stationStats {
	section := io.NewSectionReader(file, offset, length)
	reader := bufio.NewReaderSize(section, 1<<20)

	if offset != 0 {
		_, _ = reader.ReadBytes('\n')
	}

	local := make(map[string]stationStats)

	for {
		line, err := reader.ReadBytes('\n')

		if len(line) > 0 {

			if line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
			}
			if len(line) == 0 {
				goto checkErr
			}

			idx := bytes.IndexByte(line, ';')
			if idx <= 0 || idx >= len(line)-1 {
				goto checkErr
			}

			station := string(line[:idx])
			tempBuf := line[idx+1:]

			temp, ok := parseTemperature(tempBuf)
			if !ok {
				goto checkErr
			}

			stats := local[station]
			if stats.count == 0 {
				stats.min = temp
				stats.max = temp
			} else {
				if temp < stats.min {
					stats.min = temp
				}
				if temp > stats.max {
					stats.max = temp
				}
			}
			stats.sum += temp
			stats.count++
			local[station] = stats
		}

	checkErr:
		if err != nil {
			break
		}
	}

	return local
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

	chunkSize := fileSize / int64(workerCount)

	var wg sync.WaitGroup
	resultsCh := make(chan map[string]stationStats, workerCount)

	for i := 0; i < workerCount; i++ {
		offset := int64(i) * chunkSize
		length := chunkSize
		if i == workerCount-1 {
			length = fileSize - offset
		}

		wg.Add(1)
		go func(off, size int64) {
			defer wg.Done()
			resultsCh <- processChunk(file, off, size)
		}(offset, length)
	}

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	globalStats := make(map[string]stationStats)
	for local := range resultsCh {
		for station, stats := range local {
			g := globalStats[station]
			if g.count == 0 {
				g.min = stats.min
				g.max = stats.max
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
			globalStats[station] = g
		}
	}

	stations := make([]string, 0, len(globalStats))
	for station := range globalStats {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	for _, station := range stations {
		s := globalStats[station]
		mean := s.sum / float64(s.count)

		minRounded := math.Round(s.min*10) / 10
		meanRounded := math.Round(mean*10) / 10
		maxRounded := math.Round(s.max*10) / 10

		fmt.Printf("%s;%.1f/%.1f/%.1f\n", station, minRounded, meanRounded, maxRounded)
	}
}
