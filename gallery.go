package broadcaster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/sandertv/gophertunnel/minecraft/service"
)

const galleryURL = "https://persona.franchise.minecraft-services.net/api/v1.0/gallery"

type GalleryClient struct {
	TokenSource service.TokenSource
	Client      *http.Client
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

func (g GalleryClient) SetShowcase(ctx context.Context, xuid, imagePath string, deleteOther bool) error {
	if imagePath == "" {
		return errors.New("image path is empty")
	}
	if _, err := os.Stat(imagePath); err != nil {
		return err
	}
	images, err := g.Images(ctx, xuid)
	if err != nil {
		return err
	}
	newHash, err := fileHash(imagePath)
	if err != nil {
		return err
	}
	var imageID string
	for _, img := range images {
		if img.URL == "" {
			continue
		}
		hash, err := g.remoteImageHash(ctx, img.URL)
		if err == nil && hash == newHash {
			imageID = img.ID
			break
		}
	}
	if imageID == "" {
		img, err := g.Upload(ctx, imagePath, true)
		if err != nil {
			return err
		}
		imageID = img.ID
	}
	if deleteOther {
		var deleteErr error
		for _, img := range images {
			if img.ID != "" && img.ID != imageID {
				deleteErr = errors.Join(deleteErr, g.Delete(ctx, img.ID))
			}
		}
		if deleteErr != nil {
			return fmt.Errorf("delete old gallery images: %w", deleteErr)
		}
	}
	return nil
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
		req.Header.Set("X-Ms-Showcased-Timetaken", stat.ModTime().Format(time.RFC3339))
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
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(data), nil
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
	tok, err := g.TokenSource.Token()
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

func fileHash(path string) (uint32, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(data), nil
}
