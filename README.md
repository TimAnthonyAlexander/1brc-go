# 1 Billion-Row Challenge (1BRC) – Go Solution

Just trying to see whether or not I can implement the 1 Billion Row Challenge in Go, as I am currently learning it.

---

## Result

| Hardware | File | Real Time* |
|----------|------|-----------|
| **MacBook Pro 14″ (M1 Pro 10-core CPU, 32 GB RAM)** | 1 000 000 000 rows (≈ 13 GB) | **12.335 s** |

*measured with*

```
time ./1brc measurements-1000000000.txt
# 138.92s user 7.74s system 846% cpu 12.335 total
```

---

## How it works

   The file is split into *N* equal-sized byte ranges (`chunkSize = fileSize / N`, where *N* = logical CPU cores). Each range is processed by its own goroutine using `io.NewSectionReader`, so the kernel never copies data and workers stay NUMA-friendly.

   Workers stream their slice with a 1 MiB `bufio.Reader`. If a chunk does not start at byte 0, the first partial line is discarded so every record is seen exactly once.

   Data contains at most one decimal digit. A tiny custom `parseTemperature([]byte)` turns the bytes into a `float64` using a handful of arithmetic ops—≈ 30× faster than `strconv.ParseFloat` and zero allocations.

   Each goroutine keeps its own `map[string]stationStats` (min, max, sum, count). **No locks** are needed in the hot loop.

   Upon completion each worker sends its map on a buffered channel. The main goroutine merges the maps once all workers have finished.

   Station names are sorted alphabetically to satisfy the challenge’s deterministic output requirement.

---

## Build 

```bash
# Go 1.20+ is sufficient

go build -o 1brc .

# Run on any data file (path is the sole argument)
./1brc measurements-1000000000.txt
```

