package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AppDependencies struct {
	Postgres    *pgxpool.Pool
	Qdrant      *qdrant.Client
	EmbedderURL string
}

func mustGetenv(key string, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func initPostgres(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	ctxPing, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(ctxPing); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func initQdrant(ctx context.Context, host string, grpcPort int, useTLS bool) (*qdrant.Client, error) {
	qClient, err := qdrant.NewClient(&qdrant.Config{
		Host:   host,
		Port:   grpcPort,
		UseTLS: useTLS,
		APIKey: "",
	})
	if err != nil {
		return nil, err
	}
	return qClient, nil
}

func ensureCollection(ctx context.Context, q *qdrant.Client, name string, size uint64) error {
	err := q.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: name,
		VectorsConfig: &qdrant.VectorsConfig{
			Config: &qdrant.VectorsConfig_Params{Params: &qdrant.VectorParams{
				Size:     size,
				Distance: qdrant.Distance_Cosine,
			}},
		},
	})
	if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.AlreadyExists {
			return nil
		}
		return err
	}
	return nil
}

type embedRequest struct {
	Texts []string `json:"texts"`
}

type embedResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
}

type searchRequest struct {
	Query string `json:"query"`
	Limit int    `json:"limit"`
}

type searchResult struct {
	ID      string         `json:"id"`
	Score   float32        `json:"score"`
	Payload map[string]any `json:"payload"`
}

func callEmbedder(ctx context.Context, baseURL string, texts []string) ([][]float32, error) {
	body, _ := json.Marshal(embedRequest{Texts: texts})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedder status %d", resp.StatusCode)
	}
	var er embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
		return nil, err
	}
	return er.Embeddings, nil
}

func createTables(ctx context.Context, db *pgxpool.Pool) error {
	sql := `
CREATE TABLE IF NOT EXISTS hadith_collections (
  id SERIAL PRIMARY KEY,
  code TEXT UNIQUE NOT NULL,
  title TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hadiths (
  id SERIAL PRIMARY KEY,
  collection_id INT NOT NULL REFERENCES hadith_collections(id) ON DELETE CASCADE,
  number TEXT NOT NULL,
  text_ar TEXT,
  text_ru TEXT,
  text_en TEXT,
  grade TEXT,
  topics TEXT[]
);
`
	_, err := db.Exec(ctx, sql)
	return err
}

type HadithUploadRequest struct {
	Collection struct {
		Code  string `json:"code"`
		Title string `json:"title"`
	} `json:"collection"`
	Hadiths []struct {
		Number string   `json:"number"`
		TextAr string   `json:"text_ar"`
		TextRu string   `json:"text_ru"`
		TextEn string   `json:"text_en"`
		Grade  string   `json:"grade"`
		Topics []string `json:"topics"`
	} `json:"hadiths"`
}

func toPreferredText(h map[string]string) (text string, lang string) {
	if v := h["ru"]; v != "" {
		return v, "ru"
	}
	if v := h["en"]; v != "" {
		return v, "en"
	}
	if v := h["ar"]; v != "" {
		return v, "ar"
	}
	return "", ""
}

func main() {
	ctx := context.Background()

	port := mustGetenv("PORT", "8080")
	dsn := mustGetenv("POSTGRES_DSN", "postgres://app:app@localhost:5433/islamdb?sslmode=disable")
	qHost := mustGetenv("QDRANT_HOST", "localhost")
	qPortStr := mustGetenv("QDRANT_GRPC_PORT", "6334")
	embedderURL := mustGetenv("EMBEDDER_URL", "http://localhost:8000")

	qPort, err := strconv.Atoi(qPortStr)
	if err != nil {
		log.Fatalf("invalid QDRANT_GRPC_PORT: %v", err)
	}

	pg, err := initPostgres(ctx, dsn)
	if err != nil {
		log.Fatalf("postgres init: %v", err)
	}
	defer pg.Close()

	if err := createTables(ctx, pg); err != nil {
		log.Fatalf("create tables: %v", err)
	}

	qClient, err := initQdrant(ctx, qHost, qPort, false)
	if err != nil {
		log.Fatalf("qdrant init: %v", err)
	}
	if err := ensureCollection(ctx, qClient, "documents", 768); err != nil {
		log.Fatalf("ensure collection: %v", err)
	}

	deps := &AppDependencies{
		Postgres:    pg,
		Qdrant:      qClient,
		EmbedderURL: embedderURL,
	}

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	e.GET("/healthz", func(c echo.Context) error { return c.String(http.StatusOK, "ok") })

	e.POST("/v1/search", func(c echo.Context) error {
		var req searchRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "bad request"})
		}
		if req.Query == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "empty query"})
		}
		if req.Limit <= 0 || req.Limit > 50 {
			req.Limit = 10
		}

		ctx, cancel := context.WithTimeout(c.Request().Context(), 30*time.Second)
		defer cancel()

		embeds, err := callEmbedder(ctx, deps.EmbedderURL, []string{req.Query})
		if err != nil {
			return c.JSON(http.StatusBadGateway, map[string]string{"error": "embedder failed"})
		}
		if len(embeds) == 0 {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "no embedding returned"})
		}

		vector := embeds[0]

		limit := uint64(req.Limit)
		sp, err := deps.Qdrant.GetPointsClient().Search(ctx, &qdrant.SearchPoints{
			CollectionName: "documents",
			Vector:         vector,
			Limit:          limit,
			WithPayload:    &qdrant.WithPayloadSelector{SelectorOptions: &qdrant.WithPayloadSelector_Enable{Enable: true}},
		})
		if err != nil {
			return c.JSON(http.StatusBadGateway, map[string]string{"error": "qdrant search failed"})
		}

		results := make([]searchResult, 0, len(sp.Result))
		for _, r := range sp.Result {
			id := ""
			switch p := r.Id.PointIdOptions.(type) {
			case *qdrant.PointId_Num:
				id = fmt.Sprintf("%d", p.Num)
			case *qdrant.PointId_Uuid:
				id = p.Uuid
			default:
				id = ""
			}

			resultPayload := map[string]any{}
			for k, v := range r.Payload {
				resultPayload[k] = v
			}

			results = append(results, searchResult{ID: id, Score: r.Score, Payload: resultPayload})
		}
		return c.JSON(http.StatusOK, map[string]any{"results": results})
	})

	e.POST("/v1/admin/hadiths/upload", func(c echo.Context) error {
		var req HadithUploadRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "bad request"})
		}
		if req.Collection.Code == "" || req.Collection.Title == "" || len(req.Hadiths) == 0 {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "missing fields"})
		}
		if len(req.Hadiths) > 2000 {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "too many hadiths in one upload"})
		}

		ctx, cancel := context.WithTimeout(c.Request().Context(), 5*time.Minute)
		defer cancel()

		var collectionID int64
		err := deps.Postgres.QueryRow(ctx, `
INSERT INTO hadith_collections(code, title)
VALUES ($1, $2)
ON CONFLICT (code) DO UPDATE SET title = EXCLUDED.title
RETURNING id
`, req.Collection.Code, req.Collection.Title).Scan(&collectionID)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "db upsert collection failed"})
		}

		type row struct {
			ID     int64
			Number string
			TextAr string
			TextRu string
			TextEn string
			Grade  string
			Topics []string
		}
		rows := make([]row, 0, len(req.Hadiths))
		for _, h := range req.Hadiths {
			var id int64
			err := deps.Postgres.QueryRow(ctx, `
INSERT INTO hadiths (collection_id, number, text_ar, text_ru, text_en, grade, topics)
VALUES ($1,$2,$3,$4,$5,$6,$7)
RETURNING id
`, collectionID, h.Number, nullStr(h.TextAr), nullStr(h.TextRu), nullStr(h.TextEn), nullStr(h.Grade), toTextArray(h.Topics)).Scan(&id)
			if err != nil {
				return c.JSON(http.StatusInternalServerError, map[string]string{"error": "db insert hadith failed"})
			}
			rows = append(rows, row{
				ID:     id,
				Number: h.Number,
				TextAr: h.TextAr,
				TextRu: h.TextRu,
				TextEn: h.TextEn,
				Grade:  h.Grade,
				Topics: h.Topics,
			})
		}

		type doc struct {
			ID     int64
			Text   string
			Lang   string
			Number string
		}
		docs := make([]doc, 0, len(rows))
		for _, r := range rows {
			text, lang := toPreferredText(map[string]string{
				"ru": r.TextRu,
				"en": r.TextEn,
				"ar": r.TextAr,
			})
			if text == "" {
				continue
			}
			docs = append(docs, doc{ID: r.ID, Text: text, Lang: lang, Number: r.Number})
		}
		if len(docs) == 0 {
			return c.JSON(http.StatusOK, map[string]any{"inserted": len(rows), "embedded": 0})
		}

		const batchSize = 64
		upserted := 0
		for i := 0; i < len(docs); i += batchSize {
			j := i + batchSize
			if j > len(docs) {
				j = len(docs)
			}
			batch := docs[i:j]
			texts := make([]string, 0, len(batch))
			for _, d := range batch {
				texts = append(texts, d.Text)
			}
			embeds, err := callEmbedder(ctx, deps.EmbedderURL, texts)
			if err != nil {
				return c.JSON(http.StatusBadGateway, map[string]string{"error": "embedder failed"})
			}
			points := make([]*qdrant.PointStruct, 0, len(embeds))
			for k, vec := range embeds {
				d := batch[k]

				payload := qdrant.NewValueMap(
					map[string]any{
						"origin_type":     "hadith",
						"origin_id":       d.ID,
						"collection_code": req.Collection.Code,
						"number":          d.Number,
						"lang":            d.Lang,
						"title":           fmt.Sprintf("Hadith %s (%s)", d.Number, req.Collection.Code),
						"snippet":         snippet(d.Text, 280),
					},
				)

				points = append(points, &qdrant.PointStruct{
					Id:      &qdrant.PointId{PointIdOptions: &qdrant.PointId_Uuid{Uuid: uuid.NewString()}},
					Vectors: &qdrant.Vectors{VectorsOptions: &qdrant.Vectors_Vector{Vector: qdrant.NewVector(vec...)}},
					Payload: payload,
				})
			}
			_, err = deps.Qdrant.Upsert(ctx, &qdrant.UpsertPoints{CollectionName: "documents", Points: points})
			if err != nil {
				return c.JSON(http.StatusBadGateway, map[string]string{"error": "qdrant upsert failed"})
			}
			upserted += len(points)
		}

		return c.JSON(http.StatusOK, map[string]any{"inserted": len(rows), "embedded": upserted})
	})

	if err := e.Start(":" + port); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func snippet(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func toTextArray(a []string) any {
	if len(a) == 0 {
		return nil
	}
	return a
}
