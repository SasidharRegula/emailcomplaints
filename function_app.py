import azure.functions as func
import logging
from openai import AzureOpenAI 
from azure.storage.blob import BlobServiceClient 
from azure.ai.documentintelligence import DocumentIntelligenceClient 
from azure.core.credentials import AzureKeyCredential
import os,json,random
from azure.cosmos import CosmosClient 
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
app=func.FunctionApp()
@app.route(route="emailcomplaintsgithub", auth_level=func.AuthLevel.ANONYMOUS)
def emailcomplaintsgithub(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    BLOB_CONN_STR=os.getenv("BLOB_CONN_STR") 
    BLOB_CONTAINER_NAME=os.getenv("BLOB_CONTAINER_NAME")
    DOC_INT_KEY=os.getenv("DOC_INT_KEY") 
    DOC_INT_ENDPOINT=os.getenv("DOC_INT_ENDPOINT")
    AZURE_API_ENDPOINT=os.getenv("AZURE_API_ENDPOINT")
    AZURE_API_KEY=os.getenv("AZURE_API_KEY")
    AZURE_API_VERSION=os.getenv("AZURE_API_VERSION") 
    COSMOS_CONN_STR=os.getenv("COSMOS_CONN_STR") 
    COSMOS_DB_NAME=os.getenv("COSMOS_DB_NAME") 
    COSMOS_CONTAINER_NAME=os.getenv("COSMOS_CONTAINER_NAME") 
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR) 
    container_client = blob_service.get_container_client(BLOB_CONTAINER_NAME) 
    doc_int_client=DocumentIntelligenceClient(
        endpoint=DOC_INT_ENDPOINT,
        credential=AzureKeyCredential(key=DOC_INT_KEY)
    )
    aoai_client=AzureOpenAI(
        azure_endpoint=AZURE_API_ENDPOINT,
        api_key=AZURE_API_KEY,
        api_version=AZURE_API_VERSION
    ) 
    cosmos_client=CosmosClient.from_connection_string(COSMOS_CONN_STR) 
    database=cosmos_client.get_database_client(COSMOS_DB_NAME) 
    container=database.get_container_client(COSMOS_CONTAINER_NAME)
    case_id=req.params.get('case_id')
    if not case_id:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            case_id = req_body.get('case_id') 
    if not case_id:
        return func.HttpResponse("case_id is required", status_code=400) 
    uploaded_files = []
    if req.files:
        uploaded_files = req.files.getlist("files")
    if uploaded_files:
        logging.info(f"[UPLOAD] Received {len(uploaded_files)} file(s)") 
        for file in uploaded_files: 
            filename = file.filename.replace("\\", "/")
            blob_path = f"{case_id}/{filename}"
            blob_client = container_client.get_blob_client(blob_path) 
            blob_client.upload_blob(
                file.stream.read(),
                overwrite=True
            ) 
            logging.info(f"[UPLOAD] Stored file -> {blob_path}") 
    attachments=[]
    logging.info(f"[BLOB SCAN] Scanning container '{BLOB_CONTAINER_NAME}' for prefix '{case_id}/'") 
    blob_list = list(container_client.list_blobs(name_starts_with=f"{case_id}/"))
    logging.info(f"[BLOB COUNT] Found {len(blob_list)} blob(s)") 
    for blob in container_client.list_blobs(name_starts_with=f"{case_id}/"): 
        logging.info(f"Processing file: {blob.name}") 
        blob_client=container_client.get_blob_client(blob.name) 
        file_bytes=bytes(blob_client.download_blob().readall())
        attachments.append({
            "fileName": blob.name.split("/")[-1],
            "fileBytes": file_bytes
        }) 
    if not attachments:
        return func.HttpResponse(f"No attachments found in Blob for case {case_id}",status_code=404)
    def analyze_files(file_bytes):
        poller=doc_int_client.begin_analyze_document(
                model_id="prebuilt-layout",
                body=file_bytes
            ) 
        result=poller.result() 
        lines=[]
        for page in result.pages:
            for line in page.lines:
                lines.append(line.content)
        return "\n".join(lines) 
    with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(
                analyze_files,
                [a["fileBytes"] for a in attachments]
            )) 
    ocr_text="\n".join(results) 
    api_payload = {
            "caseId": case_id,
            "caseType": "Application Fraud",
            "fraudCategory": "Amount Fraud",
            "priority": "High",
            "email": {
                "from": "alerts@bankcore.com",
                "subject": "Excess Loan Amount Credited",
                "description": (
                    "System controls detected that the loan amount credited "
                    "exceeds the sanctioned amount. Preliminary review indicates "
                    "possible amount manipulation during disbursement."
                ),
                "receivedOn": "2026-02-08T10:15:00Z"
            },
            "customer": {
                "name": "Anil Sharma",
                "customerId": "CUST-774512",
                "accountType": "Retail Loan"
            },
            "attachments": [{"fileName": a["fileName"]} for a in attachments]
            } 
     
    email_description=api_payload["email"]["description"] 
    prompt = f"""Extract ONLY the following entities.Return STRICT JSON.
                    Applicant Name
                    Customer ID
                    Branch Code
                    Requested Amount
                    Sanctioned Amount

                    EMAIL:
                    {email_description}

                    DOCUMENT TEXT:
                    {ocr_text}
                    """ 
    response = aoai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
            {"role": "system", "content": "You extract structured fraud investigation entities."},
            {"role": "user", "content": prompt}
            ],
            temperature=0,
         max_tokens=400
        ) 
    raw_output = response.choices[0].message.content 
    cleaned = raw_output.strip() 
    if cleaned.startswith("```"):
            cleaned= cleaned.replace("```json", "").replace("```", "").strip() 
    try:
        cleaned_entities = json.loads(cleaned) 
    except Exception:
        logging.error(f"Invalid JSON from GPT: {cleaned}")
        return func.HttpResponse("Invalid JSON returned by model", status_code=500) 
    final_prompt=f"""You are a senior bank fraud investigation officer.
            Using ONLY the data below, generate a clear investigation summary.
            CASE ID: {case_id} 
            EXTRACTED ENTITIES: {json.dumps(cleaned_entities,indent=2)}
            OCR DOCUMENT TEXT: {ocr_text} 
            Return STRICT JSON with:
            - case_id
            - summary (3-4 sentences)
            - key_findings (bullet list)
            - risk_level (LOW / MEDIUM / HIGH)
            - recommended_action (single sentence) 
            No markdown. No explanations.
            """ 
    final_response=aoai_client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
                {"role": "system", "content": "You produce fraud investigation summaries."},
                {"role": "user", "content": final_prompt}
            ],
            temperature=0.2,
            max_tokens=500
            ) 
    final_raw_output=final_response.choices[0].message.content.strip() 
    if final_raw_output.startswith("```"):
        final_raw_output = final_raw_output.replace("```json", "").replace("```", "").strip()
    summary_json = json.loads(final_raw_output) 
    doc_id = f"{case_id}-{random.randint(10,99)}"
    document = {
            "id": doc_id,                      
            "case_id": case_id,                 
            "timestamp": datetime.utcnow().isoformat(),
            "ocr_text": ocr_text,
            "extracted_entities": cleaned_entities,
            "summary_result": summary_json
        }
    container.upsert_item(document) 
    return func.HttpResponse(
        json.dumps(summary_json),
        mimetype="application/json")