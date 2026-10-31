package broadcaster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

type Notifier interface {
	Notify(ctx context.Context, message string) error
}

// notify sends message to the configured notifier, if any. A spent ctx, such as an expired
// upload deadline or a stopped broadcaster, must not drop the message.
func (b *Broadcaster) notify(ctx context.Context, message string) {
	if b.conf.Notifier == nil {
		return
	}
	if ctx.Err() != nil {
		ctx = context.WithoutCancel(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := b.conf.Notifier.Notify(ctx, message); err != nil {
		b.log.Error("send notification", "err", err)
	}
}

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
