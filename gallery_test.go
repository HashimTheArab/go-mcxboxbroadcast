package broadcaster

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/png"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/df-mc/go-xsapi/v2"
	"github.com/sandertv/gophertunnel/minecraft/service"
)

func TestBroadcasterUploadsGalleryForEnabledSubAccounts(t *testing.T) {
	imagePath := testGalleryImageFile(t)
	seen := map[string]string{}
	client := &http.Client{Transport: galleryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/xuid/") {
			xuid := req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]
			seen[xuid] = req.Header.Get("Authorization")
			if xuid == "bad" {
				return galleryHTTPResponse(http.StatusInternalServerError, ""), nil
			}
			return galleryHTTPResponse(http.StatusOK, `{"result":{"showcasedImages":[]}}`), nil
		}
		if req.Method == http.MethodPost && req.URL.Path == "/api/v1.0/gallery" {
			return galleryHTTPResponse(http.StatusAccepted, `{"result":{"id":"new"}}`), nil
		}
		t.Fatalf("unexpected request %s %s", req.Method, req.URL)
		return nil, nil
	})}
	b := &Broadcaster{
		ctx: context.Background(),
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		conf: Config{
			XUID: "primary",
			Gallery: &GalleryConfig{
				Enabled:     true,
				ImagePath:   imagePath,
				TokenSource: galleryTokenSource{authorization: "Bearer primary"},
				Client:      client,
			},
			SubAccounts: []SubAccountConfig{
				{
					ID:                   "good",
					Enabled:              true,
					XBLClient:            &xsapi.Client{},
					XUID:                 "good",
					MinecraftTokenSource: galleryTokenSource{authorization: "Bearer good"},
				},
				{
					ID:                   "bad",
					Enabled:              true,
					XBLClient:            &xsapi.Client{},
					XUID:                 "bad",
					MinecraftTokenSource: galleryTokenSource{authorization: "Bearer bad"},
				},
				{
					ID:                   "after-failure",
					Enabled:              true,
					XBLClient:            &xsapi.Client{},
					XUID:                 "after-failure",
					MinecraftTokenSource: galleryTokenSource{authorization: "Bearer after-failure"},
				},
				{
					ID:                   "disabled",
					Enabled:              false,
					XBLClient:            &xsapi.Client{},
					XUID:                 "disabled",
					MinecraftTokenSource: galleryTokenSource{authorization: "Bearer disabled"},
				},
			},
		},
	}

	b.uploadGallery(context.Background())

	for xuid, authorization := range map[string]string{
		"primary":       "Bearer primary",
		"good":          "Bearer good",
		"bad":           "Bearer bad",
		"after-failure": "Bearer after-failure",
	} {
		if got := seen[xuid]; got != authorization {
			t.Fatalf("gallery authorization for %s = %q, want %q", xuid, got, authorization)
		}
	}
	if _, ok := seen["disabled"]; ok {
		t.Fatal("disabled sub-account gallery was uploaded")
	}
}

func TestGalleryClientCleanupWaitsForActiveUpload(t *testing.T) {
	imagePath := testGalleryImageFile(t)
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	client := &xsapi.Client{}
	b := &Broadcaster{
		ctx: context.Background(),
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		conf: Config{
			XUID: "primary",
			Gallery: &GalleryConfig{
				Enabled:     true,
				ImagePath:   imagePath,
				TokenSource: galleryTokenSource{authorization: "Bearer primary"},
				Client: &http.Client{Transport: galleryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
					if strings.HasSuffix(req.URL.Path, "/xuid/primary") {
						close(requestStarted)
						<-releaseRequest
					}
					return galleryHTTPResponse(http.StatusInternalServerError, ""), nil
				})},
			},
			SubAccounts: []SubAccountConfig{{
				ID:                   "sub",
				Enabled:              true,
				XBLClient:            client,
				XBLTokenSource:       staticTokenSource{},
				XUID:                 "sub",
				MinecraftTokenSource: galleryTokenSource{authorization: "Bearer sub"},
			}},
		},
	}

	uploadDone := make(chan struct{})
	go func() {
		defer close(uploadDone)
		b.uploadGallery(context.Background())
	}()
	<-requestStarted

	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		b.clearCreatedXBLClientReferences(map[*xsapi.Client]struct{}{client: {}})
	}()

	select {
	case <-cleanupDone:
		t.Fatal("client cleanup completed while gallery upload was active")
	case <-time.After(20 * time.Millisecond):
	}
	if b.conf.SubAccounts[0].XBLClient != client {
		t.Fatal("client reference was cleared before the active gallery upload completed")
	}

	close(releaseRequest)
	<-uploadDone
	<-cleanupDone
	if b.conf.SubAccounts[0].XBLClient != nil {
		t.Fatal("client reference was not cleared after gallery upload completed")
	}
}

func TestGalleryTimeoutIsIndependentPerAccount(t *testing.T) {
	imagePath := testGalleryImageFile(t)
	seenSub := false
	client := &http.Client{Transport: galleryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/xuid/primary"):
			<-req.Context().Done()
			return nil, req.Context().Err()
		case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/xuid/sub"):
			if err := req.Context().Err(); err != nil {
				t.Fatalf("sub-account received expired context: %v", err)
			}
			seenSub = true
			return galleryHTTPResponse(http.StatusOK, `{"result":{"showcasedImages":[]}}`), nil
		case req.Method == http.MethodPost && req.URL.Path == "/api/v1.0/gallery":
			return galleryHTTPResponse(http.StatusAccepted, `{"result":{"id":"new"}}`), nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			return nil, nil
		}
	})}
	b := &Broadcaster{
		ctx:                  context.Background(),
		log:                  slog.New(slog.NewTextHandler(io.Discard, nil)),
		galleryUploadTimeout: 20 * time.Millisecond,
		conf: Config{
			XUID: "primary",
			Gallery: &GalleryConfig{
				Enabled:     true,
				ImagePath:   imagePath,
				TokenSource: galleryTokenSource{authorization: "Bearer primary"},
				Client:      client,
			},
			SubAccounts: []SubAccountConfig{{
				ID:                   "sub",
				Enabled:              true,
				XBLClient:            &xsapi.Client{},
				XUID:                 "sub",
				MinecraftTokenSource: galleryTokenSource{authorization: "Bearer sub"},
			}},
		},
	}

	b.uploadGalleryWithTimeout()

	if !seenSub {
		t.Fatal("sub-account gallery upload did not run after primary timeout")
	}
}

func TestGalleryClientReusesReencodedEquivalentImage(t *testing.T) {
	localImage := testPNG(t, png.BestCompression)
	remoteImage := testPNG(t, png.NoCompression)
	if bytes.Equal(localImage, remoteImage) {
		t.Fatal("test images unexpectedly have identical encoded bytes")
	}
	imagePath := filepath.Join(t.TempDir(), "image.png")
	if err := os.WriteFile(imagePath, localImage, 0o600); err != nil {
		t.Fatal(err)
	}

	var downloaded bool
	g := GalleryClient{
		TokenSource: galleryMinecraftTokenSource{},
		Client: &http.Client{Transport: galleryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/xuid/1"):
				return galleryHTTPResponse(http.StatusOK, `{"result":{"showcasedImages":[{"id":"img","url":"https://cdn.example.test/image.png"}]}}`), nil
			case req.Method == http.MethodGet && req.URL.Host == "cdn.example.test":
				downloaded = true
				return responseBytes(http.StatusOK, remoteImage), nil
			case req.Method == http.MethodPost && req.URL.Path == "/api/v1.0/gallery":
				t.Fatal("image should have been reused instead of uploaded")
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})},
	}

	if err := g.SetShowcase(context.Background(), "1", imagePath, true); err != nil {
		t.Fatal(err)
	}
	if !downloaded {
		t.Fatal("gallery image was not downloaded for dedupe comparison")
	}
}

func TestGalleryClientAuthenticatesRemoteImageFetch(t *testing.T) {
	remoteImage := testPNG(t, png.NoCompression)
	imagePath := filepath.Join(t.TempDir(), "image.png")
	if err := os.WriteFile(imagePath, remoteImage, 0o600); err != nil {
		t.Fatal(err)
	}

	g := GalleryClient{
		TokenSource: galleryMinecraftTokenSource{},
		Client: &http.Client{Transport: galleryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/xuid/1"):
				return galleryHTTPResponse(http.StatusOK, `{"result":{"showcasedImages":[{"id":"img","url":"https://cdn.example.test/image.png"}]}}`), nil
			case req.Method == http.MethodGet && req.URL.Host == "cdn.example.test":
				// The image endpoint rejects unauthenticated requests with 401,
				// which used to force a needless re-upload on every start.
				if req.Header.Get("Authorization") != "Bearer minecraft" {
					return galleryHTTPResponse(http.StatusUnauthorized, ""), nil
				}
				return responseBytes(http.StatusOK, remoteImage), nil
			case req.Method == http.MethodPost && req.URL.Path == "/api/v1.0/gallery":
				t.Fatal("image should have been reused instead of uploaded")
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})},
	}

	result, err := g.SetShowcaseResult(context.Background(), "1", imagePath, false)
	if err != nil {
		t.Fatal(err)
	}
	if !result.AlreadySet {
		t.Fatal("expected authenticated image fetch to recognise the existing showcase image")
	}
}

func TestGalleryClientSetShowcaseResultReportsExistingImage(t *testing.T) {
	localImage := testPNG(t, png.BestCompression)
	remoteImage := testPNG(t, png.NoCompression)
	imagePath := filepath.Join(t.TempDir(), "image.png")
	if err := os.WriteFile(imagePath, localImage, 0o600); err != nil {
		t.Fatal(err)
	}

	g := GalleryClient{
		TokenSource: galleryMinecraftTokenSource{},
		Client: &http.Client{Transport: galleryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/xuid/1"):
				return galleryHTTPResponse(http.StatusOK, `{"result":{"showcasedImages":[{"id":"img","url":"https://cdn.example.test/image.png"}]}}`), nil
			case req.Method == http.MethodGet && req.URL.Host == "cdn.example.test":
				return responseBytes(http.StatusOK, remoteImage), nil
			case req.Method == http.MethodPost && req.URL.Path == "/api/v1.0/gallery":
				t.Fatal("image should have been reused instead of uploaded")
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})},
	}

	result, err := g.SetShowcaseResult(context.Background(), "1", imagePath, true)
	if err != nil {
		t.Fatal(err)
	}
	if result.ImageID != "img" {
		t.Fatalf("image id = %q, want img", result.ImageID)
	}
	if !result.AlreadySet {
		t.Fatal("expected existing showcase image to be reported")
	}
	if result.Uploaded {
		t.Fatal("existing showcase image should not be reported as uploaded")
	}
}

func TestGalleryClientReportsInvalidLocalImage(t *testing.T) {
	imagePath := filepath.Join(t.TempDir(), "image.txt")
	if err := os.WriteFile(imagePath, []byte("not an image"), 0o600); err != nil {
		t.Fatal(err)
	}

	g := GalleryClient{
		TokenSource: galleryMinecraftTokenSource{},
		Client: &http.Client{Transport: galleryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/xuid/1"):
				return galleryHTTPResponse(http.StatusOK, `{"result":{"showcasedImages":[]}}`), nil
			case req.Method == http.MethodPost && req.URL.Path == "/api/v1.0/gallery":
				t.Fatal("invalid local image should not be uploaded")
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})},
	}

	err := g.SetShowcase(context.Background(), "1", imagePath, true)
	if err == nil {
		t.Fatal("expected invalid image error")
	}
	if !strings.Contains(err.Error(), "hash image pixels") || !strings.Contains(err.Error(), "decode image") {
		t.Fatalf("expected clear decode error, got %v", err)
	}
}

func testPNG(t *testing.T, level png.CompressionLevel) []byte {
	t.Helper()

	img := image.NewRGBA(image.Rect(0, 0, 3, 2))
	colors := []color.RGBA{
		{R: 0x10, G: 0x20, B: 0x30, A: 0xff},
		{R: 0x40, G: 0x50, B: 0x60, A: 0xff},
		{R: 0x70, G: 0x80, B: 0x90, A: 0xff},
		{R: 0xa0, G: 0xb0, B: 0xc0, A: 0xff},
		{R: 0xd0, G: 0xe0, B: 0xf0, A: 0xff},
		{R: 0x00, G: 0x11, B: 0x22, A: 0xff},
	}
	i := 0
	for y := img.Bounds().Min.Y; y < img.Bounds().Max.Y; y++ {
		for x := img.Bounds().Min.X; x < img.Bounds().Max.X; x++ {
			img.SetRGBA(x, y, colors[i])
			i++
		}
	}

	var buf bytes.Buffer
	enc := png.Encoder{CompressionLevel: level}
	if err := enc.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func testGalleryImageFile(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "image.png")
	if err := os.WriteFile(path, testGalleryImageBytes(t), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testGalleryImageBytes(t *testing.T) []byte {
	t.Helper()

	return testPNG(t, png.DefaultCompression)
}

func responseBytes(code int, body []byte) *http.Response {
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Body:       io.NopCloser(bytes.NewReader(body)),
		Header:     make(http.Header),
	}
}

type galleryRoundTripFunc func(*http.Request) (*http.Response, error)

func (f galleryRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func galleryHTTPResponse(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

type galleryMinecraftTokenSource struct{}

func (galleryMinecraftTokenSource) ServiceToken(context.Context) (*service.Token, error) {
	return &service.Token{AuthorizationHeader: "Bearer minecraft", ValidUntil: time.Now().Add(time.Hour)}, nil
}

type galleryTokenSource struct {
	authorization string
}

func (s galleryTokenSource) ServiceToken(context.Context) (*service.Token, error) {
	return &service.Token{AuthorizationHeader: s.authorization, ValidUntil: time.Now().Add(time.Hour)}, nil
}
