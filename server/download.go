package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"

	"github.com/jmorganca/ollama/api"
	"golang.org/x/sync/errgroup"
)

type BlobDownload struct {
	Total     int64
	Completed int64
	Parts     []BlobDownloadPart
}

type BlobDownloadPart struct {
	Offset    int64
	Size      int64
	Completed int64
}

var inProgress sync.Map // map of digests currently being downloaded to their current download progress

type downloadOpts struct {
	mp      ModelPath
	digest  string
	regOpts *RegistryOptions
	fn      func(api.ProgressResponse)
	retry   int // track the number of retries on this download
}

const maxRetry = 3

// downloadBlob downloads a blob from the registry and stores it in the blobs directory
func downloadBlob(ctx context.Context, opts downloadOpts) error {
	fp, err := GetBlobsPath(opts.digest)
	if err != nil {
		return err
	}

	fi, err := os.Stat(fp)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	var metadata BlobDownload
	metadataFile, err := os.Open(fp + ".json")
	switch {
	case errors.Is(err, os.ErrNotExist) && fi != nil:
		// no download metadata so the download is complete
		opts.fn(api.ProgressResponse{
			Digest:    opts.digest,
			Completed: int(fi.Size()),
			Total:     int(fi.Size()),
		})

		return nil
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return err
	default:
		defer metadataFile.Close()

		if err := json.NewDecoder(metadataFile).Decode(&metadata); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	requestURL := opts.mp.BaseURL()
	requestURL = requestURL.JoinPath("v2", opts.mp.GetNamespaceRepository(), "blobs", opts.digest)

	if len(metadata.Parts) == 0 {
		resp, err := makeRequest(ctx, "HEAD", requestURL, nil, nil, opts.regOpts)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		metadata.Total, _ = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)

		// reserve the file
		f.Truncate(metadata.Total)

		var offset int64
		size := int64(256 * 1024 * 1024)

		for offset < metadata.Total {
			if offset+size > metadata.Total {
				size = metadata.Total - offset
			}

			metadata.Parts = append(metadata.Parts, BlobDownloadPart{
				Offset: offset,
				Size:   size,
			})

			offset += size
		}
	}

	pw := &ProgressWriter{
		status: fmt.Sprintf("downloading %s", opts.digest),
		digest: opts.digest,
		total:  int(metadata.Total),
		fn:     opts.fn,
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(10)
	for i := range metadata.Parts {
		part := metadata.Parts[i]
		if part.Completed == part.Size {
			continue
		}

		g.Go(func() error {
			err := downloadBlobChunk(ctx, f, requestURL, part, pw, opts)
			return err
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func downloadBlobChunk(ctx context.Context, w io.WriterAt, requestURL *url.URL, part BlobDownloadPart, pw *ProgressWriter, opts downloadOpts) error {
	offset := part.Offset + part.Completed
	ws := io.NewOffsetWriter(w, offset)

	headers := make(http.Header)
	headers.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, part.Offset+part.Size-1))
	resp, err := makeRequest(ctx, "GET", requestURL, headers, nil, opts.regOpts)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(ws, io.TeeReader(resp.Body, pw))
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return nil
}
