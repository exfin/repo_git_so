from fastapi import FastAPI
from pydantic import BaseModel
import boto3
import csv
import io
import pandas as pd


AWS_ACCESS_KEY = "AKIA5XCWV5MBPSK7K3NK"
AWS_SECRET_KEY = "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"
AWS_S3_BUCKET_NAME = "juanm-final"
AWS_REGION = "us-east-1"
CSV_FILE_NAME = "personas.csv"

s3_client = boto3.client(
        service_name="s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

app = FastAPI()



class Persona(BaseModel):
    nombre: str
    edad: int
    altura: float


def subir_csv_a_s3(df: pd.DataFrame):
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=AWS_S3_BUCKET_NAME,
            Key=CSV_FILE_NAME,
            Body=csv_buffer.getvalue()
        )


def descargar_csv_desde_s3() -> pd.DataFrame:
    try:
        response = s3_client.get_object(Bucket=AWS_S3_BUCKET_NAME, Key=CSV_FILE_NAME)
        return pd.read_csv(io.BytesIO(response["Body"].read()))
    except s3_client.exceptions.NoSuchKey:
        return pd.DataFrame(columns=["nombre", "edad", "altura"])
    

@app.post("/personas")
async def agregar_persona(persona: Persona):
    df = descargar_csv_desde_s3()
    nueva_fila = pd.DataFrame([persona.dict()])
    df = pd.concat([df, nueva_fila], ignore_index=True)
    subir_csv_a_s3(df)
    return {"mensaje": "Persona agregada exitosamente"}

@app.get("/numeropersonas")
async def obtener_numero_personas():
    df = descargar_csv_desde_s3()
    numero_personas = len(df)
    return {"numero_personas": numero_personas}