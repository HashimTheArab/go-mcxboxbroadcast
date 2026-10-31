package broadcaster

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"image"
	"image/color"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/sandertv/gophertunnel/minecraft/service"
)

const (
	galleryURL           = "https://persona.franchise.minecraft-services.net/api/v1.0/gallery"
	galleryUploadTimeout = 30 * time.Second
)

// uploadGallery sets the configured showcase image of the account's gallery.
func (b *Broadcaster) uploadGallery() {
	b.galleryMu.Lock()
	defer b.galleryMu.Unlock()

	cfg := b.conf.Gallery
	if cfg == nil {
		return
	}
	if _, err := os.Stat(cfg.ImagePath); errors.Is(err, os.ErrNotExist) {
		return
	}
	ctx, cancel := context.WithTimeout(b.ctx, galleryUploadTimeout)
	defer cancel()
	tokens, err := b.minecraftTokenSource(ctx)
	if err != nil {
		b.log.Warn("minecraft services token source unavailable", "err", err)
		b.notify(ctx, "Showcase image upload skipped: Minecraft services token source is unavailable.")
		return
	}
	client := galleryClient{tokens: tokens, http: httpClient(b.conf.HTTPClient), log: b.log}
	args := []any{"path", cfg.ImagePath, "delete_other", cfg.DeleteOtherImages, "xuid", b.xuid()}
	b.log.Info("setting showcase image", args...)
	result, err := client.setShowcase(ctx, b.xuid(), cfg.ImagePath, cfg.DeleteOtherImages)
	if err != nil {
		b.log.Error("set showcase image", append(args, "err", err)...)
		b.notify(ctx, "Showcase image upload failed: "+err.Error())
		return
	}
	b.log.Info("set showcase image", append(args, "image_id", result.ImageID, "uploaded", result.Uploaded, "already_set", result.AlreadySet)...)
}

// galleryClient manages the account's Minecraft profile showcase images.
type galleryClient struct {
	tokens service.TokenSource
	http   *http.Client
	log    *slog.Logger
}

type galleryImage struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type galleryResponse struct {
	Result struct {
		ShowcasedImages []galleryImage `json:"showcasedImages"`
	} `json:"result"`
}

type galleryUploadResponse struct {
	Result galleryImage `json:"result"`
}

type showcaseResult struct {
	ImageID    string
	Uploaded   bool
	AlreadySet bool
}

// setShowcase shows the image at imagePath, reusing a showcased copy with the same pixels.
func (g galleryClient) setShowcase(ctx context.Context, xuid, imagePath string, deleteOther bool) (showcaseResult, error) {
	if imagePath == "" {
		return showcaseResult{}, errors.New("image path is empty")
	}
	if _, err := os.Stat(imagePath); err != nil {
		return showcaseResult{}, err
	}
	images, err := g.images(ctx, xuid)
	if err != nil {
		return showcaseResult{}, err
	}
	g.log.DebugContext(ctx, "loaded gallery showcase images", "xuid", xuid, "count", len(images))
	newHash, err := fileHash(imagePath)
	if err != nil {
		return showcaseResult{}, err
	}
	var result showcaseResult
	for _, img := range images {
		if img.URL == "" {
			continue
		}
		hash, err := g.remoteImageHash(ctx, img.URL)
		if err != nil {
			g.log.DebugContext(ctx, "failed to compare gallery image", "image_id", img.ID, "err", err)
			continue
		}
		if hash == newHash {
			result.ImageID = img.ID
			result.AlreadySet = true
			g.log.DebugContext(ctx, "gallery showcase image already set", "image_id", result.ImageID)
			break
		}
	}
	if result.ImageID == "" {
		g.log.DebugContext(ctx, "uploading gallery showcase image", "path", imagePath)
		img, err := g.upload(ctx, imagePath)
		if err != nil {
			return showcaseResult{}, err
		}
		result.ImageID = img.ID
		result.Uploaded = true
		g.log.DebugContext(ctx, "uploaded gallery showcase image", "image_id", result.ImageID)
	}
	if deleteOther {
		var deleteErr error
		deleted := 0
		for _, img := range images {
			if img.ID != "" && img.ID != result.ImageID {
				g.log.DebugContext(ctx, "deleting old gallery image", "image_id", img.ID)
				deleteErr = errors.Join(deleteErr, g.delete(ctx, img.ID))
				deleted++
			}
		}
		if deleteErr != nil {
			return showcaseResult{}, fmt.Errorf("delete old gallery images: %w", deleteErr)
		}
		g.log.DebugContext(ctx, "deleted old gallery images", "count", deleted)
	}
	return result, nil
}

func (g galleryClient) images(ctx context.Context, xuid string) ([]galleryImage, error) {
	req, err := g.request(ctx, http.MethodGet, galleryURL+"/xuid/"+xuid, nil)
	if err != nil {
		return nil, err
	}
	resp, err := g.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s %s: %s", req.Method, req.URL, resp.Status)
	}
	var data galleryResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	return data.Result.ShowcasedImages, nil
}

// upload adds the image as the featured showcase image.
func (g galleryClient) upload(ctx context.Context, imagePath string) (galleryImage, error) {
	f, err := os.Open(imagePath)
	if err != nil {
		return galleryImage{}, err
	}
	defer f.Close()
	req, err := g.request(ctx, http.MethodPost, galleryURL, f)
	if err != nil {
		return galleryImage{}, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Ms-Showcased-Featured", "true")
	if stat, err := f.Stat(); err == nil {
		// UTC with milliseconds, matching Java's Instant.toString().
		req.Header.Set("X-Ms-Showcased-Timetaken", stat.ModTime().UTC().Format("2006-01-02T15:04:05.000Z"))
	}
	resp, err := g.http.Do(req)
	if err != nil {
		return galleryImage{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return galleryImage{}, fmt.Errorf("%s %s: %s", req.Method, req.URL, resp.Status)
	}
	var data galleryUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return galleryImage{}, err
	}
	return data.Result, nil
}

func (g galleryClient) delete(ctx context.Context, imageID string) error {
	req, err := g.request(ctx, http.MethodDelete, galleryURL+"/"+imageID, nil)
	if err != nil {
		return err
	}
	resp, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s", req.Method, req.URL, resp.Status)
	}
	return nil
}

func (g galleryClient) remoteImageHash(ctx context.Context, url string) (uint32, error) {
	// The gallery image endpoint requires the Minecraft service token even for
	// reads; an unauthenticated fetch returns 401 and forces a re-upload.
	req, err := g.request(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := g.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%s %s: %s", req.Method, req.URL, resp.Status)
	}
	hash, err := imageHash(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("hash gallery image pixels %s: %w", url, err)
	}
	return hash, nil
}

func (g galleryClient) request(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	tok, err := g.tokens.ServiceToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("request minecraft token: %w", err)
	}
	tok.SetAuthHeader(req)
	return req, nil
}

func fileHash(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	hash, err := imageHash(f)
	if err != nil {
		return 0, fmt.Errorf("hash image pixels %q: %w", path, err)
	}
	return hash, nil
}

func imageHash(r io.Reader) (uint32, error) {
	img, _, err := image.Decode(r)
	if err != nil {
		return 0, fmt.Errorf("decode image: %w", err)
	}

	hash := crc32.NewIEEE()
	bounds := img.Bounds()
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[:4], uint32(bounds.Dx()))
	binary.BigEndian.PutUint32(buf[4:], uint32(bounds.Dy()))
	_, _ = hash.Write(buf[:])

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			c := color.NRGBA64Model.Convert(img.At(x, y)).(color.NRGBA64)
			binary.BigEndian.PutUint16(buf[0:2], c.R)
			binary.BigEndian.PutUint16(buf[2:4], c.G)
			binary.BigEndian.PutUint16(buf[4:6], c.B)
			binary.BigEndian.PutUint16(buf[6:8], c.A)
			_, _ = hash.Write(buf[:])
		}
	}
	return hash.Sum32(), nil
}
