// Command stampede simulates a flash-sale crowd against the gateway:
// every virtual user gets a token, joins the waiting room, polls until
// admitted, reserves (retrying 429/503 with the SAME idempotency key) and pays.
//
//	go run ./cmd/stampede -gateway http://localhost:8080 -users 5000 -event 1 -tier 1
//
// Afterwards, verify in Postgres that nothing was oversold:
//
//	SELECT t.total, sum(s.available) AS left FROM ticket_tiers t JOIN inventory_shards s ON s.tier_id=t.id GROUP BY t.id;
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"time"
)

type result struct {
	outcome string
	reserve time.Duration
	queued  time.Duration
}

func main() {
	gateway := flag.String("gateway", "http://localhost:8080", "gateway base URL")
	users := flag.Int("users", 2000, "virtual users")
	concurrency := flag.Int("concurrency", 1000, "max simultaneous users")
	eventID := flag.Int("event", 1, "event id")
	tierID := flag.Int("tier", 1, "tier id")
	qty := flag.Int("qty", 1, "tickets per user")
	pay := flag.Bool("pay", true, "confirm (pay) after reserving")
	flag.Parse()

	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{
		MaxIdleConns: *concurrency, MaxIdleConnsPerHost: *concurrency, IdleConnTimeout: 30 * time.Second,
	}}
	c := &caller{base: *gateway, http: client}

	results := make(chan result, *users)
	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *users; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			results <- c.user(fmt.Sprintf("load-%d-%d", start.Unix(), i), *eventID, *tierID, *qty, *pay)
		}(i)
	}
	wg.Wait()
	close(results)
	elapsed := time.Since(start)

	counts := map[string]int{}
	var lat, queue []time.Duration
	for r := range results {
		counts[r.outcome]++
		if r.reserve > 0 {
			lat = append(lat, r.reserve)
		}
		queue = append(queue, r.queued)
	}
	fmt.Printf("\n%d users in %s\n", *users, elapsed.Round(time.Millisecond))
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-22s %d\n", k, counts[k])
	}
	fmt.Printf("reserve latency  p50=%s p95=%s p99=%s\n", pct(lat, 50), pct(lat, 95), pct(lat, 99))
	fmt.Printf("time in queue    p50=%s p95=%s max=%s\n", pct(queue, 50), pct(queue, 95), pct(queue, 100))
}

type caller struct {
	base string
	http *http.Client
}

func (c *caller) user(userID string, eventID, tierID, qty int, pay bool) result {
	var tok struct{ Token string }
	if code := c.do("POST", "/auth/dev-token", "", nil, map[string]any{"user_id": userID}, &tok); code != 200 {
		return result{outcome: fmt.Sprintf("auth_%d", code)}
	}
	auth := "Bearer " + tok.Token

	queuedAt := time.Now()
	var st struct {
		Ticket         string `json:"ticket"`
		State          string `json:"state"`
		AdmissionToken string `json:"admission_token"`
		PollAfterSec   int    `json:"poll_after_sec"`
	}
	for try := 0; ; try++ {
		code := c.do("POST", fmt.Sprintf("/api/v1/queue/%d/join", eventID), auth, nil, nil, &st)
		if code == 200 {
			break
		}
		if try > 10 {
			return result{outcome: fmt.Sprintf("join_%d", code)}
		}
		backoff(try)
	}
	ticket := st.Ticket
	for st.State != "admitted" {
		// Real clients honour poll_after_sec; the demo polls faster.
		time.Sleep(time.Duration(300+rand.IntN(400)) * time.Millisecond)
		if code := c.do("GET", fmt.Sprintf("/api/v1/queue/%d/status", eventID), auth,
			map[string]string{"x-queue-ticket": ticket}, nil, &st); code != 200 {
			return result{outcome: fmt.Sprintf("status_%d", code)}
		}
	}
	queued := time.Since(queuedAt)

	idem := fmt.Sprintf("%s-attempt", userID)
	var res struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	t0 := time.Now()
	var code int
	for try := 0; try < 8; try++ {
		code = c.do("POST", "/api/v1/reservations", auth,
			map[string]string{"idempotency-key": idem, "x-admission-token": st.AdmissionToken},
			map[string]any{"event_id": eventID, "tier_id": tierID, "quantity": qty}, &res)
		if code != 429 && code != 503 && code != 504 && code != 0 {
			break
		}
		backoff(try)
	}
	lat := time.Since(t0)
	switch code {
	case 201, 200:
	case 409:
		return result{outcome: "sold_out", reserve: lat, queued: queued}
	default:
		return result{outcome: fmt.Sprintf("reserve_%d", code), reserve: lat, queued: queued}
	}
	if !pay {
		return result{outcome: "held", reserve: lat, queued: queued}
	}
	for try := 0; try < 5; try++ {
		code = c.do("POST", "/api/v1/reservations/"+res.ID+"/confirm", auth, nil,
			map[string]any{"payment_token": "tok_visa"}, &res)
		if code == 200 {
			return result{outcome: "confirmed", reserve: lat, queued: queued}
		}
		if code != 202 && code != 503 && code != 429 && code != 0 {
			break
		}
		backoff(try)
	}
	return result{outcome: fmt.Sprintf("confirm_%d", code), reserve: lat, queued: queued}
}

func (c *caller) do(method, path, auth string, headers map[string]string, body any, out any) int {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, c.base+path, rdr)
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	if auth != "" {
		req.Header.Set("authorization", auth)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if out != nil && resp.StatusCode < 300 {
		_ = json.Unmarshal(data, out)
	}
	return resp.StatusCode
}

func backoff(try int) {
	d := time.Duration(100<<min(try, 5)) * time.Millisecond
	time.Sleep(d/2 + time.Duration(rand.Int64N(int64(d/2))))
}

func pct(d []time.Duration, p int) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	i := (len(d)*p)/100 - 1
	if i < 0 {
		i = 0
	}
	if i >= len(d) {
		i = len(d) - 1
	}
	return d[i].Round(time.Millisecond)
}
