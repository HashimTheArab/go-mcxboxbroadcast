package broadcaster

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandertv/gophertunnel/minecraft/service"
)

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
