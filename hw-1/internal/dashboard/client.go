package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	HTTP *http.Client
}

// FetchHandlers читает GET /handlers приложения.
func (c *Client) FetchHandlers(ctx context.Context, appURL string) ([]Handler, error) {
	var body struct {
		Handlers []Handler `json:"handlers"`
	}
	if err := c.do(ctx, http.MethodGet, appURL+"/handlers", nil, "", "", &body); err != nil {
		return nil, err
	}
	return body.Handlers, nil
}

// Grafana - доступ к API Grafana с учёткой администратора.
type Grafana struct {
	Client
	URL, User, Password string
}

func (g *Grafana) Health(ctx context.Context) error {
	return g.do(ctx, http.MethodGet, g.URL+"/api/health", nil, "", "", nil)
}

// Upsert создаёт или перезаписывает дашборд с тем же uid.
func (g *Grafana) Upsert(ctx context.Context, d Dashboard) error {
	payload := map[string]any{"dashboard": d, "overwrite": true, "message": "generated"}
	return g.do(ctx, http.MethodPost, g.URL+"/api/dashboards/db", payload, g.User, g.Password, nil)
}

func (c *Client) do(ctx context.Context, method, url string, in any, user, pass string, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s %s: %s: %s", method, url, resp.Status, bytes.TrimSpace(msg))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Retry повторяет f, пока не выйдет timeout.
func Retry(ctx context.Context, timeout, interval time.Duration, f func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		err := f(ctx)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ctx.Err(), err)
		case <-time.After(interval):
		}
	}
}
