from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import os

MODEL_NAME = os.getenv("MODEL_NAME", "intfloat/multilingual-e5-base")

embedder_app = FastAPI(title="Embedding Service", version="0.1.0")

model = SentenceTransformer(MODEL_NAME)

class EmbedRequest(BaseModel):
    texts: list[str]

class EmbedResponse(BaseModel):
    embeddings: list[list[float]]

@embedder_app.get("/healthz")
def healthz():
    return {"status": "ok", "model": MODEL_NAME}

@embedder_app.post("/embed", response_model=EmbedResponse)
def embed(req: EmbedRequest):
    if not req.texts:
        return {"embeddings": []}
    vectors = model.encode(req.texts, normalize_embeddings=True, convert_to_numpy=True)
    return {"embeddings": vectors.tolist()}
