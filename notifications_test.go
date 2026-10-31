package broadcaster

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestSlackNotifierPostsConfiguredMessage(t *testing.T) {
	var body string
	n := SlackNotifier{
		WebhookURL: "https://example.test/slack",
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() != "https://example.test/slack" {
				t.Fatalf("unexpected URL %s", req.URL)
			}
			data, _ := io.ReadAll(req.Body)
			body = string(data)
			return response(http.StatusOK, ""), nil
		})},
	}
	if err := n.Notify(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, "hello") {
		t.Fatalf("message missing from body %q", body)
	}
	var payload map[string]string
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["text"] != "hello" || payload["content"] != "hello" {
		t.Fatalf("unexpected webhook payload %#v", payload)
	}
}

func TestSlackNotifierErrorDoesNotLeakWebhookURL(t *testing.T) {
	secretURL := "https://hooks.slack.com/services/TOKEN/SECRET"
	n := SlackNotifier{
		WebhookURL: secretURL,
		Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return response(http.StatusForbidden, ""), nil
		})},
	}
	err := n.Notify(context.Background(), "hello")
	if err == nil {
		t.Fatal("expected webhook error")
	}
	if strings.Contains(err.Error(), secretURL) || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("webhook URL leaked in error: %v", err)
	}

	n.Client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("network down")
	})}
	err = n.Notify(context.Background(), "hello")
	if err == nil {
		t.Fatal("expected transport error")
	}
	if strings.Contains(err.Error(), secretURL) || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("webhook URL leaked in transport error: %v", err)
	}

	n.WebhookURL = "https://hooks.slack.com/services/TOKEN/%zzSECRET"
	err = n.Notify(context.Background(), "hello")
	if err == nil {
		t.Fatal("expected malformed URL error")
	}
	if strings.Contains(err.Error(), "TOKEN") || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("webhook URL leaked in malformed URL error: %v", err)
	}
}
