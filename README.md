# Islam App (Monorepo)

Services:
- backend: Go Echo, Postgres, Qdrant
- embedder: FastAPI + Sentence-Transformers (multilingual-e5-base)
- frontend: Next.js

Run:
- docker compose build
- docker compose up -d
- Check:
  - embedder: http://localhost:8000/healthz
  - backend: http://localhost:8080/healthz
  - frontend: http://localhost:3000

Admin (hadiths upload):
POST http://localhost:8080/v1/admin/hadiths/upload
Body:
{
  "collection": {"code":"bukhari","title":"Sahih al-Bukhari"},
  "hadiths":[{"number":"1","text_ar":"...", "text_ru":"...", "grade":"sahih", "topics":["intention"]}]
}
