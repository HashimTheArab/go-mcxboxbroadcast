package broadcaster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

type SlackNotifier struct {
	WebhookURL string
	Client     *http.Client
}

func (s SlackNotifier) Notify(ctx context.Context, message string) error {
	if s.WebhookURL == "" {
		return errors.New("webhook URL is empty")
	}
	body, err := json.Marshal(map[string]string{
		"text":    message,
		"content": message,
	})
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("make webhook request: %w", sanitizeWebhookError(err))
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client().Do(req)
	if err != nil {
		return fmt.Errorf("POST webhook: %w", sanitizeWebhookError(err))
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s webhook: %s", req.Method, resp.Status)
	}
	return nil
}

func (s SlackNotifier) client() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	return http.DefaultClient
}

func sanitizeWebhookError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Errorf("%s failed", urlErr.Op)
	}
	return errors.New("request failed")
}
