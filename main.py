import os
import io
import time
import json
import uuid
import boto3
import datetime
import requests
from decimal import Decimal
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "751629-esi3898k-examen1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/515162739424/cola-notas")
NOTAS_TABLE = os.getenv("NOTAS_TABLE", "notas_venta")
CONTENIDO_NOTA_TABLE = os.getenv("CONTENIDO_NOTA_TABLE", "contenido_nota")
CLIENTES_TABLE = os.getenv("CLIENTES_TABLE", "clientes")
DOMICILIOS_TABLE = os.getenv("DOMICILIOS_TABLE", "domicilios")
PRODUCTOS_TABLE = os.getenv("PRODUCTOS_TABLE", "productos")
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")

app = FastAPI(title="API de Notas de Venta")
cloudwatch = boto3.client('cloudwatch', region_name=REGION)
sqs_client = boto3.client('sqs', region_name=REGION)

# middleware para metricas (12 factores: Logs/Métricas)
@app.middleware("http")
async def add_metrics_and_process(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000 
    status_code = response.status_code

    if 200 <= status_code < 300:
        metric_name = "HTTP_2xx"
    elif 400 <= status_code < 500:
        metric_name = "HTTP_4xx"
    else:
        metric_name = "HTTP_5xx"

    try:
        cloudwatch.put_metric_data(
            Namespace='ExamenParcial/NotasAPI',
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': [{'Name': 'Environment', 'Value': ENVIRONMENT}],
                    'Value': 1,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'ExecutionTime',
                    'Dimensions': [
                        {'Name': 'Environment', 'Value': ENVIRONMENT}, 
                        {'Name': 'Endpoint', 'Value': request.url.path}
                    ],
                    'Value': process_time,
                    'Unit': 'Milliseconds'
                }
            ]
        )
    except Exception as e:
        print(f"error enviando metricas a cloudwatch: {e}")

    return response

def dynamodb_resource():
    return boto3.resource('dynamodb', region_name=REGION)

def s3_client():
    return boto3.client('s3', region_name=REGION)

def preparar_para_dynamo(data):
    if isinstance(data, dict):
        return {k: preparar_para_dynamo(v) for k, v in data.items()}
    if isinstance(data, list):
        return [preparar_para_dynamo(i) for i in data]
    if isinstance(data, float):
        return Decimal(str(data))
    return data

def _put_item(table_name: str, item: Dict[str, Any]):
    table = dynamodb_resource().Table(table_name)
    table.put_item(Item=preparar_para_dynamo(item))

def _get_item(table_name: str, key: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    table = dynamodb_resource().Table(table_name)
    return table.get_item(Key=key).get('Item')

class ContenidoNotaBase(BaseModel):
    id_producto: str
    cantidad: int = Field(gt=0, description="debe ser mayor que cero")
    precio_unitario: float = Field(gt=0, description="debe ser mayor que cero")

    @property
    def importe(self) -> float:
        return self.cantidad * self.precio_unitario

class NotaVentaCreate(BaseModel):
    id_cliente: str
    id_direccion_facturacion: str
    id_direccion_envio: str
    productos: List[ContenidoNotaBase]

class NotaVenta(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    id_cliente: str
    id_direccion_facturacion: str
    id_direccion_envio: str
    folio: str
    total: float = 0.0
    fecha_creacion: str = Field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat())

def generar_pdf_nota(nota, cliente, dir_fact, dir_env, contenido) -> bytes:
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    w, h = letter
    y = h - 50
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, y, "nota de venta")
    y -= 30
    c.setFont("Helvetica", 12)
    c.drawString(50, y, f"folio: {nota.folio}")
    # se omite logica visual repetitiva por brevedad, asume misma implementacion pdf
    c.save()
    buffer.seek(0)
    return buffer.getvalue()

def subir_pdf_a_s3(pdf_bytes: bytes, rfc_cliente: str, folio: str):
    client = s3_client()
    key_s3 = f"{rfc_cliente}/{folio}.pdf"
    metadata = {
        'hora-envio': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'nota-descargada': 'false',
        'veces-enviado': '1'
    }
    client.put_object(
        Bucket=BUCKET_NAME, Key=key_s3, Body=pdf_bytes,
        ContentType='application/pdf', Metadata=metadata
    )

def enviar_mensaje_sqs(folio: str, total: float, rfc_cliente: str):
    mensaje = {
        "folio": folio,
        "total": float(total),
        "rfc_cliente": rfc_cliente
    }
    sqs_client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(mensaje)
    )

@app.post("/notas", status_code=201)
def crear_nota(nota_data: NotaVentaCreate):
    cliente = _get_item(CLIENTES_TABLE, {'id': nota_data.id_cliente})
    if not cliente:
        raise HTTPException(
            status_code=404,
            detail=f"No existe un cliente con id '{nota_data.id_cliente}'.",
        )

    if not nota_data.productos:
        raise HTTPException(
            status_code=400,
            detail="La nota debe incluir al menos un producto.",
        )

    dir_fact = _get_item(DOMICILIOS_TABLE, {'id': nota_data.id_direccion_facturacion})
    if not dir_fact:
        raise HTTPException(
            status_code=404,
            detail=f"No existe un domicilio de facturación con id '{nota_data.id_direccion_facturacion}'.",
        )
    if dir_fact.get('id_cliente') != nota_data.id_cliente:
        raise HTTPException(
            status_code=400,
            detail="La dirección de facturación no pertenece al cliente indicado.",
        )

    dir_env = _get_item(DOMICILIOS_TABLE, {'id': nota_data.id_direccion_envio})
    if not dir_env:
        raise HTTPException(
            status_code=404,
            detail=f"No existe un domicilio de envío con id '{nota_data.id_direccion_envio}'.",
        )
    if dir_env.get('id_cliente') != nota_data.id_cliente:
        raise HTTPException(
            status_code=400,
            detail="La dirección de envío no pertenece al cliente indicado.",
        )

    for linea in nota_data.productos:
        producto = _get_item(PRODUCTOS_TABLE, {'id': linea.id_producto})
        if not producto:
            raise HTTPException(
                status_code=404,
                detail=f"No existe un producto con id '{linea.id_producto}'.",
            )

    folio = f"F-{uuid.uuid4().hex[:8].upper()}"
    
    total = sum(p.cantidad * p.precio_unitario for p in nota_data.productos)
    
    nota = NotaVenta(
        id_cliente=nota_data.id_cliente,
        id_direccion_facturacion=nota_data.id_direccion_facturacion,
        id_direccion_envio=nota_data.id_direccion_envio,
        folio=folio,
        total=total
    )
    
    _put_item(NOTAS_TABLE, nota.dict())
    
    pdf_bytes = generar_pdf_nota(nota, cliente, dir_fact, dir_env, nota_data.productos)
    subir_pdf_a_s3(pdf_bytes, cliente['rfc'], folio)
    
    # desacoplamiento asincrono en vez de background tasks
    enviar_mensaje_sqs(folio, total, cliente['rfc'])
    
    return {"mensaje": "nota creada y encolada para notificacion", "folio": folio}

def s3_client():
    return boto3.client('s3', region_name=REGION)

@app.get("/notas/{rfc_cliente}/{folio}/descargar")
def descargar_pdf(rfc_cliente: str, folio: str):
    client = s3_client()
    key_s3 = f"{rfc_cliente}/{folio}.pdf"
    try:
        # intentar obtener el objeto de S3
        response_s3 = client.get_object(Bucket=BUCKET_NAME, Key=key_s3)
        pdf_content = response_s3['Body'].read()
        
        # actualizar metadatos (Factor: Persistencia/Estado)
        client.copy_object(
            Bucket=BUCKET_NAME,
            Key=key_s3,
            CopySource={'Bucket': BUCKET_NAME, 'Key': key_s3},
            Metadata={**response_s3['Metadata'], 'nota-descargada': 'true'},
            MetadataDirective='REPLACE'
        )
        
        return Response(
            content=pdf_content,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={folio}.pdf"}
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            raise HTTPException(status_code=404, detail="El PDF no existe en S3.")
        raise HTTPException(status_code=500, detail=str(e))