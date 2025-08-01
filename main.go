package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
)

type stationStats struct {
	min   float64
	max   float64
	sum   float64
	count int
}

type measurement struct {
	station string
	temp    float64
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

	// Channel to distribute parsed measurements to workers
	const chanBuffer = 4096
	measurementsCh := make(chan measurement, chanBuffer)

	// WaitGroup to wait for all workers to finish
	var wg sync.WaitGroup
	workerCount := runtime.NumCPU()

	// Channel to collect local maps from workers
	resultsCh := make(chan map[string]stationStats, workerCount)

	// Launch workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localStats := make(map[string]stationStats)
			for m := range measurementsCh {
				stats := localStats[m.station]
				if stats.count == 0 {
					// initialize
					stats.min = m.temp
					stats.max = m.temp
				}
				if m.temp < stats.min {
					stats.min = m.temp
				}
				if m.temp > stats.max {
					stats.max = m.temp
				}
				stats.sum += m.temp
				stats.count++
				localStats[m.station] = stats
			}
			resultsCh <- localStats
		}()
	}

	// Reader goroutine to parse the file and send to workers
	go func() {
		reader := bufio.NewReaderSize(file, 1<<20) // 1 MiB buffer
		for {
			line, err := reader.ReadBytes('\n')
			if len(line) > 0 {
				// Remove trailing newline/carriage return
				if line[len(line)-1] == '\n' {
					line = line[:len(line)-1]
				}
				if len(line) == 0 {
					continue // skip empty lines
				}
				idx := bytes.IndexByte(line, ';')
				if idx <= 0 || idx >= len(line)-1 {
					continue // malformed line, skip
				}
				station := string(line[:idx])
				tempBytes := line[idx+1:]
				temp, err := strconv.ParseFloat(string(tempBytes), 64)
				if err != nil {
					continue // skip invalid temperature
				}
				measurementsCh <- measurement{station: station, temp: temp}
			}
			if err != nil {
				break
			}
		}
		close(measurementsCh)
	}()

	// Wait for workers to finish
	wg.Wait()
	close(resultsCh)

	// Merge results
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

	// Output results sorted by station name
	stations := make([]string, 0, len(globalStats))
	for station := range globalStats {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	for _, station := range stations {
		s := globalStats[station]
		mean := s.sum / float64(s.count)

		// Round values to one decimal place using half-away-from-zero rounding
		minRounded := math.Round(s.min*10) / 10
		meanRounded := math.Round(mean*10) / 10
		maxRounded := math.Round(s.max*10) / 10

		fmt.Printf("%s;%.1f/%.1f/%.1f\n", station, minRounded, meanRounded, maxRounded)
	}
}
