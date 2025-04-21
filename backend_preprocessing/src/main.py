from fastapi import FastAPI
from a_common.database import engine, Base
from backend_preprocessing.src.api import router


app = FastAPI()
Base.metadata.create_all(bind=engine)


app.include_router(
    router.router,
    # prefix="/preprocessing",
    tags=["test"],
)
