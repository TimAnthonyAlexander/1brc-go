package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
)

// stationStats keeps running aggregates for one weather station.
type stationStats struct {
	min   float64
	max   float64
	sum   float64
	count int
}

// parseTemperature parses a temperature value known to contain at most one
// decimal place. A custom parser avoids the overhead of strconv.ParseFloat in
// the hot path.
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

	// integral part
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

	// optional single decimal digit
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

// processChunk walks over data[start:end) and returns local aggregates.
func processChunk(data []byte, start, end int) map[string]stationStats {
	// ensure we start at a line boundary (caller guarantees start==0 for the
	// very first chunk)
	if start != 0 {
		for start < end && data[start-1] != '\n' {
			start++
		}
	}

	local := make(map[string]stationStats)

	i := start
	for i < end {
		// find newline separating the current line
		j := bytes.IndexByte(data[i:end], '\n')
		var line []byte
		if j == -1 {
			// no complete line in the remaining slice – break; the next chunk
			// (or EOF if last) will handle it
			break
		}
		line = data[i : i+j]
		i += j + 1 // move past "line + \n"

		if len(line) == 0 {
			continue // skip empty lines
		}

		idx := bytes.IndexByte(line, ';')
		if idx <= 0 || idx >= len(line)-1 {
			continue // malformed – ignore
		}

		station := string(line[:idx])
		tempBuf := line[idx+1:]

		temp, ok := parseTemperature(tempBuf)
		if !ok {
			continue // skip invalid values
		}

		stats := local[station]
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
		stats.sum += temp
		stats.count++
		local[station] = stats
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
	if fileSize == 0 {
		return // nothing to do
	}
	if fileSize > int64(^uint(0)>>1) {
		fmt.Fprintf(os.Stderr, "File too large to mmap on this platform\n")
		os.Exit(1)
	}

	// mmap the file read-only
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
	resultsCh := make(chan map[string]stationStats, workerCount)

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
	for local := range resultsCh {
		for station, stats := range local {
			g := globalStats[station]
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
