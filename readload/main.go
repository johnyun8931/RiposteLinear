package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	Latency  time.Duration
	Status   int
	ServerID string
	EpochID  int64
	Error    string
}

type readResponse struct {
	EpochID  int64  `json:"epoch_id"`
	ServerID string `json:"server_id"`
}

func main() {
	var baseURL string
	var x int
	var y int
	var durationSeconds int
	var concurrency int
	var output string
	var timeoutMS int
	flag.StringVar(&baseURL, "url", "", "Read ALB base URL, for example http://example.elb.amazonaws.com")
	flag.IntVar(&x, "x", 0, "Read column")
	flag.IntVar(&y, "y", 0, "Read global row")
	flag.IntVar(&durationSeconds, "duration-seconds", 60, "Load duration in seconds")
	flag.IntVar(&concurrency, "concurrency", 64, "Concurrent read workers")
	flag.IntVar(&timeoutMS, "timeout-ms", 2000, "Per-request timeout in milliseconds")
	flag.StringVar(&output, "output", "", "Optional JSON summary output path")
	flag.Parse()

	if baseURL == "" {
		log.Fatal("-url is required")
	}
	if durationSeconds <= 0 || concurrency <= 0 || timeoutMS <= 0 {
		log.Fatal("-duration-seconds, -concurrency, and -timeout-ms must be positive")
	}
	readURL, err := buildReadURL(baseURL, x, y)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durationSeconds)*time.Second)
	defer cancel()
	client := &http.Client{Timeout: time.Duration(timeoutMS) * time.Millisecond}
	results := make(chan result, concurrency*4)
	var issued atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				issued.Add(1)
				results <- doRead(client, readURL)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	summary := summarize(results, start, &issued, concurrency, readURL)
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	data = append(data, '\n')
	if output != "" {
		if err := os.WriteFile(output, data, 0644); err != nil {
			log.Fatal(err)
		}
		return
	}
	fmt.Print(string(data))
}

func buildReadURL(base string, x int, y int) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/read"
	q := u.Query()
	q.Set("x", strconv.Itoa(x))
	q.Set("y", strconv.Itoa(y))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func doRead(client *http.Client, readURL string) result {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, readURL, nil)
	if err != nil {
		return result{Error: err.Error()}
	}
	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start)
	if err != nil {
		return result{Latency: latency, Error: err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	out := result{Latency: latency, Status: resp.StatusCode}
	if err != nil {
		out.Error = err.Error()
		return out
	}
	if resp.StatusCode != http.StatusOK {
		out.Error = strings.TrimSpace(string(body))
		return out
	}
	var parsed readResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		out.Error = err.Error()
		return out
	}
	out.ServerID = parsed.ServerID
	out.EpochID = parsed.EpochID
	return out
}

func summarize(results <-chan result, start time.Time, issued *atomic.Int64, concurrency int, readURL string) map[string]any {
	latencies := make([]time.Duration, 0)
	statuses := map[string]int{}
	errors := map[string]int{}
	servers := map[string]int{}
	epochs := map[string]int{}
	var ok int
	for r := range results {
		if r.Status != 0 {
			statuses[strconv.Itoa(r.Status)]++
		}
		if r.Error != "" {
			errors[r.Error]++
			continue
		}
		ok++
		latencies = append(latencies, r.Latency)
		if r.ServerID != "" {
			servers[r.ServerID]++
		}
		epochs[strconv.FormatInt(r.EpochID, 10)]++
	}
	elapsed := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	elapsedSeconds := elapsed.Seconds()
	if elapsedSeconds <= 0 {
		elapsedSeconds = 1
	}
	return map[string]any{
		"read_url":            readURL,
		"started_at":          start.UTC().Format(time.RFC3339),
		"elapsed_seconds":     elapsedSeconds,
		"concurrency":         concurrency,
		"requests_issued":     issued.Load(),
		"requests_completed":  ok + countMap(errors),
		"success_count":       ok,
		"error_count":         countMap(errors),
		"requests_per_second": float64(ok) / elapsedSeconds,
		"latency_ms": map[string]float64{
			"min": percentile(latencies, 0),
			"p50": percentile(latencies, 50),
			"p90": percentile(latencies, 90),
			"p95": percentile(latencies, 95),
			"p99": percentile(latencies, 99),
			"max": percentile(latencies, 100),
		},
		"statuses":   statuses,
		"errors":     errors,
		"server_ids": servers,
		"epoch_ids":  epochs,
	}
}

func percentile(values []time.Duration, pct int) float64 {
	if len(values) == 0 {
		return 0
	}
	if pct <= 0 {
		return durationMS(values[0])
	}
	if pct >= 100 {
		return durationMS(values[len(values)-1])
	}
	idx := (len(values)*pct + 99) / 100
	if idx <= 0 {
		idx = 1
	}
	return durationMS(values[idx-1])
}

func durationMS(value time.Duration) float64 {
	return float64(value.Microseconds()) / 1000
}

func countMap(values map[string]int) int {
	var total int
	for _, count := range values {
		total += count
	}
	return total
}
