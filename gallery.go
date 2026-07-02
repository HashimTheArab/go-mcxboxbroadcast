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

	"github.com/sandertv/gophertunnel/minecraft/service"
)

const galleryURL = "https://persona.franchise.minecraft-services.net/api/v1.0/gallery"

type GalleryClient struct {
	TokenSource service.TokenSource
	Client      *http.Client
	Log         *slog.Logger
}

type GalleryImage struct {
	ID           string `json:"id"`
	IsFeatured   bool   `json:"isFeatured"`
	LastModified string `json:"lastModified"`
	TakenTime    string `json:"takenTime"`
	URL          string `json:"url"`
}

type galleryResponse struct {
	Result struct {
		ShowcasedImages []GalleryImage `json:"showcasedImages"`
	} `json:"result"`
}

type galleryUploadResponse struct {
	Result GalleryImage `json:"result"`
}

type SetShowcaseResult struct {
	ImageID    string
	Uploaded   bool
	AlreadySet bool
}

func (g GalleryClient) SetShowcase(ctx context.Context, xuid, imagePath string, deleteOther bool) error {
	_, err := g.SetShowcaseResult(ctx, xuid, imagePath, deleteOther)
	return err
}

func (g GalleryClient) SetShowcaseResult(ctx context.Context, xuid, imagePath string, deleteOther bool) (SetShowcaseResult, error) {
	if imagePath == "" {
		return SetShowcaseResult{}, errors.New("image path is empty")
	}
	if _, err := os.Stat(imagePath); err != nil {
		return SetShowcaseResult{}, err
	}
	images, err := g.Images(ctx, xuid)
	if err != nil {
		return SetShowcaseResult{}, err
	}
	g.debug(ctx, "loaded gallery showcase images", "xuid", xuid, "count", len(images))
	newHash, err := fileHash(imagePath)
	if err != nil {
		return SetShowcaseResult{}, err
	}
	var result SetShowcaseResult
	for _, img := range images {
		if img.URL == "" {
			continue
		}
		hash, err := g.remoteImageHash(ctx, img.URL)
		if err != nil {
			g.debug(ctx, "failed to compare gallery image", "image_id", img.ID, "err", err)
			continue
		}
		if hash == newHash {
			result.ImageID = img.ID
			result.AlreadySet = true
			g.debug(ctx, "gallery showcase image already set", "image_id", result.ImageID)
			break
		}
	}
	if result.ImageID == "" {
		g.debug(ctx, "uploading gallery showcase image", "path", imagePath)
		img, err := g.Upload(ctx, imagePath, true)
		if err != nil {
			return SetShowcaseResult{}, err
		}
		result.ImageID = img.ID
		result.Uploaded = true
		g.debug(ctx, "uploaded gallery showcase image", "image_id", result.ImageID)
	}
	if deleteOther {
		var deleteErr error
		deleted := 0
		for _, img := range images {
			if img.ID != "" && img.ID != result.ImageID {
				g.debug(ctx, "deleting old gallery image", "image_id", img.ID)
				deleteErr = errors.Join(deleteErr, g.Delete(ctx, img.ID))
				deleted++
			}
		}
		if deleteErr != nil {
			return SetShowcaseResult{}, fmt.Errorf("delete old gallery images: %w", deleteErr)
		}
		g.debug(ctx, "deleted old gallery images", "count", deleted)
	}
	return result, nil
}

func (g GalleryClient) Images(ctx context.Context, xuid string) ([]GalleryImage, error) {
	req, err := g.request(ctx, http.MethodGet, galleryURL+"/xuid/"+xuid, nil)
	if err != nil {
		return nil, err
	}
	resp, err := g.client().Do(req)
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

func (g GalleryClient) Upload(ctx context.Context, imagePath string, featured bool) (GalleryImage, error) {
	f, err := os.Open(imagePath)
	if err != nil {
		return GalleryImage{}, err
	}
	defer f.Close()
	req, err := g.request(ctx, http.MethodPost, galleryURL, f)
	if err != nil {
		return GalleryImage{}, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Ms-Showcased-Featured", fmt.Sprint(featured))
	if stat, err := f.Stat(); err == nil {
		// UTC with milliseconds, matching Java's Instant.toString().
		req.Header.Set("X-Ms-Showcased-Timetaken", stat.ModTime().UTC().Format("2006-01-02T15:04:05.000Z"))
	}
	resp, err := g.client().Do(req)
	if err != nil {
		return GalleryImage{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return GalleryImage{}, fmt.Errorf("%s %s: %s", req.Method, req.URL, resp.Status)
	}
	var data galleryUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return GalleryImage{}, err
	}
	return data.Result, nil
}

func (g GalleryClient) Delete(ctx context.Context, imageID string) error {
	req, err := g.request(ctx, http.MethodDelete, galleryURL+"/"+imageID, nil)
	if err != nil {
		return err
	}
	resp, err := g.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s", req.Method, req.URL, resp.Status)
	}
	return nil
}

func (g GalleryClient) remoteImageHash(ctx context.Context, url string) (uint32, error) {
	// The gallery image endpoint requires the Minecraft service token even for
	// reads; an unauthenticated fetch returns 401 and forces a re-upload.
	req, err := g.request(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := g.client().Do(req)
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

func (g GalleryClient) request(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if g.TokenSource == nil {
		return nil, errors.New("minecraft token source is nil")
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	tok, err := g.TokenSource.ServiceToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("request minecraft token: %w", err)
	}
	tok.SetAuthHeader(req)
	return req, nil
}

func (g GalleryClient) client() *http.Client {
	if g.Client != nil {
		return g.Client
	}
	return http.DefaultClient
}

func (g GalleryClient) debug(ctx context.Context, msg string, args ...any) {
	if g.Log != nil {
		g.Log.DebugContext(ctx, msg, args...)
	}
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
