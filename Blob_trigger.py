import azure.functions as func
import json
import requests
import logging
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Optional, Any, Tuple
from datetime import datetime
import re

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = func.FunctionApp()

# Database configuration
PG_HOST = "145.190.8.4"
PG_PORT = "5432"
PG_USER = "spectra"
PG_PASSWORD = "SpectraParabola9"
PG_DATABASE = "ap_warehouse"

def get_db_connection():
    """Create and return a PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DATABASE
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def log_structured_message(event_type: str, camera_id: str = None, status: str = "info", details: Dict = None):
    """Log structured messages for monitoring"""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": event_type,
        "camera_id": camera_id or "unknown",
        "status": status,
        "service": "azure-pipeline-processor",
        "environment": "azure-function",
        "details": details or {}
    }
    logger.info(json.dumps(log_entry))

def parse_blob_url(blob_url: str) -> Optional[Tuple[str, str, str, str]]:
    """
    Parse blob URL with new structure: pipeline/Date/WH001/CAM006/CHUNK_uuid/CHUNK_uuid.mp4
    Returns: (warehouse_id, cam_id, chunk_id, date) or None
    """
    try:
        # Pattern: pipeline/YYYY-MM-DD/WH###/CAM###/CHUNK_uuid/CHUNK_uuid.mp4
        # Updated to handle uppercase letters in UUID (e.g., 0426047D)
        pattern = r'/pipeline/(\d{4}-\d{2}-\d{2})/([A-Z0-9]+)/([A-Z0-9]+)/CHUNK_([a-fA-F0-9\-]+)/CHUNK_[a-fA-F0-9\-]+\.mp4'
        match = re.search(pattern, blob_url)
        
        if match:
            date = match.group(1)
            warehouse_id = match.group(2)
            cam_id = match.group(3)
            chunk_id = match.group(4)
            
            log_structured_message("blob_url_parsed", cam_id, "success", {
                "warehouse_id": warehouse_id,
                "cam_id": cam_id,
                "chunk_id": chunk_id,
                "date": date,
                "blob_url": blob_url
            })
            return warehouse_id, cam_id, chunk_id, date
        else:
            # Fallback: Try splitting by '/' to debug
            parts = blob_url.split('/')
            logger.error(f"Regex failed. URL parts: {parts}")
            log_structured_message("blob_url_parse_failed", None, "error", {
                "blob_url": blob_url,
                "reason": "Pattern did not match",
                "url_parts": parts
            })
            return None
    except Exception as e:
        log_structured_message("blob_url_parse_exception", None, "error", {
            "error": str(e),
            "blob_url": blob_url
        })
        return None

def check_chunk_exists(chunk_id: str) -> bool:
    """Check if chunk already exists in database to prevent reprocessing"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        check_query = "SELECT chunk_id FROM public.wh_chunks WHERE chunk_id = %s"
        cursor.execute(check_query, (chunk_id,))
        result = cursor.fetchone()
        
        exists = result is not None
        
        if exists:
            log_structured_message("chunk_already_exists", None, "info", {
                "chunk_id": chunk_id,
                "status": "Chunk already processed, skipping"
            })
        
        return exists
        
    except Exception as e:
        log_structured_message("chunk_existence_check_failed", None, "warning", {
            "error": str(e),
            "chunk_id": chunk_id,
            "action": "Assuming chunk does not exist, proceeding with processing"
        })
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()

def insert_chunk_metadata(warehouse_id: str, cam_id: str, chunk_id: str, blob_url: str, date: str) -> bool:
    """Insert chunk metadata into wh_chunks table"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert chunk data - DO NOT update if already exists
        insert_query = """
            INSERT INTO public.wh_chunks (chunk_id, warehouse_id, cam_id, chunk_blob_url, transcripts_url, date, time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (chunk_id) DO NOTHING
        """
        
        cursor.execute(insert_query, (
            chunk_id,
            warehouse_id,
            cam_id,
            blob_url,
            None,  # transcripts_url is NULL initially
            date,
            datetime.utcnow()
        ))
        
        rows_affected = cursor.rowcount
        conn.commit()
        
        if rows_affected > 0:
            log_structured_message("chunk_inserted", cam_id, "success", {
                "chunk_id": chunk_id,
                "warehouse_id": warehouse_id,
                "cam_id": cam_id
            })
            return True
        else:
            log_structured_message("chunk_already_existed", cam_id, "info", {
                "chunk_id": chunk_id,
                "warehouse_id": warehouse_id,
                "cam_id": cam_id,
                "action": "Chunk was not inserted (already exists)"
            })
            return False
        
    except Exception as e:
        if conn:
            conn.rollback()
        log_structured_message("chunk_insert_failed", cam_id, "error", {
            "error": str(e),
            "chunk_id": chunk_id
        })
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()

def fetch_camera_config(warehouse_id: str, cam_id: str) -> Optional[Dict]:
    """Fetch camera configuration from database"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query camera configuration
        query = """
            SELECT cam_id, cam_direction, services, warehouse_id, camera_status
            FROM public.cameras
            WHERE cam_id = %s AND warehouse_id = %s
        """
        
        cursor.execute(query, (cam_id, warehouse_id))
        result = cursor.fetchone()
        
        if result:
            # Parse services JSON string to list
            try:
                services = json.loads(result['services']) if result['services'] else []
            except json.JSONDecodeError:
                log_structured_message("services_parse_error", cam_id, "warning", {
                    "services_raw": result['services']
                })
                services = []
            
            # Map cam_direction to facing_direction integer
            facing_direction = 1 if result['cam_direction'] == "Right" else -1
            
            config = {
                "cam_id": result['cam_id'],
                "warehouse_id": result['warehouse_id'],
                "facing_direction": facing_direction,
                "service_ids": services,
                "camera_status": result['camera_status']
            }
            
            log_structured_message("camera_config_fetched", cam_id, "success", {
                "config": config
            })
            return config
        else:
            log_structured_message("camera_not_found", cam_id, "error", {
                "warehouse_id": warehouse_id,
                "cam_id": cam_id
            })
            return None
            
    except Exception as e:
        log_structured_message("camera_config_fetch_failed", cam_id, "error", {
            "error": str(e)
        })
        return None
    finally:
        if conn:
            cursor.close()
            conn.close()

def construct_payload(blob_url: str, camera_config: Dict, chunk_id: str) -> Dict:
    """Construct FastAPI payload from camera configuration"""
    payload = {
        "blob_url": blob_url,
        "chunk_id": chunk_id,
        "service_ids": camera_config["service_ids"],
        "facing_direction": camera_config["facing_direction"],
        "camera_id": camera_config["cam_id"],
        "warehouse_id": camera_config["warehouse_id"]
    }
    
    log_structured_message("payload_constructed", camera_config["cam_id"], "success", {
        "payload": payload
    })
    return payload

def call_fastapi_service(payload: Dict) -> Dict[str, Any]:
    """Call FastAPI service with constructed payload"""
    fastapi_url = os.environ.get('FASTAPI_SERVICE_URL', 'https://warehousebackend.p9sphere.com')
    
    # --- FIX 1: Added '/api' to the endpoint ---
    endpoint = f"{fastapi_url}/api/process"
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'Azure-Pipeline-Processor/1.0'
    }
    
    camera_id = payload.get('camera_id', 'unknown')
    
    log_structured_message("fastapi_call_start", camera_id, "started", {
        "endpoint": endpoint,
        "payload": payload
    })
    
    try:
        # --- FIX 2: Increased timeout to 800 seconds ---
        response = requests.post(
            endpoint,
            json=payload,
            headers=headers,
            timeout=800
        )
        
        response_data = None
        try:
            response_data = response.json()
        except json.JSONDecodeError:
            response_data = {"raw_response": response.text}
        
        if response.status_code in [200, 201, 202]:
            success_log = {
                "status": response_data.get("status", "processing_complete"),
                "camera_id": camera_id,
                "service_results": response_data.get("service_results", ["processing:success"])
            }
            
            log_structured_message("fastapi_call_success", camera_id, "success", {
                "response_code": response.status_code,
                "response_data": response_data,
                "success_summary": success_log
            })
            
            logger.info(f"SUCCESS RESULT: {json.dumps(success_log)}")
            
            return {
                "success": True,
                "status_code": response.status_code,
                "response": response_data,
                "message": "FastAPI call successful"
            }
        else:
            log_structured_message("fastapi_call_error", camera_id, "error", {
                "response_code": response.status_code,
                "response_data": response_data,
                "error": f"HTTP {response.status_code}"
            })
            
            return {
                "success": False,
                "status_code": response.status_code,
                "response": response_data,
                "message": f"FastAPI call failed with status {response.status_code}"
            }
            
    except requests.exceptions.Timeout:
        log_structured_message("fastapi_call_timeout", camera_id, "error", {
            "error": "Request timeout after 800 seconds"
        })
        return {
            "success": False,
            "status_code": 408,
            "message": "FastAPI service call timed out"
        }
    except requests.exceptions.ConnectionError:
        log_structured_message("fastapi_connection_error", camera_id, "error", {
            "error": "Failed to connect to FastAPI service"
        })
        return {
            "success": False,
            "status_code": 503,
            "message": "Failed to connect to FastAPI service"
        }
    except Exception as e:
        log_structured_message("fastapi_call_exception", camera_id, "error", {
            "error": str(e),
            "error_type": type(e).__name__
        })
        return {
            "success": False,
            "status_code": 500,
            "message": f"Unexpected error: {str(e)}"
        }

@app.event_grid_trigger(arg_name="azeventgrid")
def PipelineBlobProcessor(azeventgrid: func.EventGridEvent):
    """
    Process Event Grid events for blob storage uploads in pipeline folder
    Flow: Parse URL → Insert wh_chunks → Fetch camera config → Call FastAPI
    """
    try:
        log_structured_message("event_grid_trigger_start", None, "started", {
            "event_type": azeventgrid.event_type,
            "subject": azeventgrid.subject
        })
        
        # Validate event type
        if azeventgrid.event_type != "Microsoft.Storage.BlobCreated":
            log_structured_message("event_type_ignored", None, "info", {
                "ignored_event_type": azeventgrid.event_type
            })
            return
        
        # Check event timestamp - only process recent events (within last 3 minutes)
        try:
            # Try to get event time from multiple possible sources
            event_time_str = None
            
            # Try getting from event object attribute first
            if hasattr(azeventgrid, 'event_time') and azeventgrid.event_time:
                # Convert datetime to string if needed
                if isinstance(azeventgrid.event_time, datetime):
                    event_time_str = azeventgrid.event_time.isoformat()
                else:
                    event_time_str = str(azeventgrid.event_time)
            
            if not event_time_str:
                # Try getting from event data JSON
                try:
                    event_data_temp = azeventgrid.get_json()
                    raw_event_time = event_data_temp.get('eventTime') or event_data_temp.get('data', {}).get('eventTime')
                    if raw_event_time:
                        if isinstance(raw_event_time, datetime):
                            event_time_str = raw_event_time.isoformat()
                        else:
                            event_time_str = str(raw_event_time)
                except:
                    pass
            
            if event_time_str:
                # Parse event time - handle both 'Z' and timezone formats
                event_time_str_clean = event_time_str.replace('Z', '+00:00')
                event_time = datetime.fromisoformat(event_time_str_clean)
                current_time = datetime.now(event_time.tzinfo) if event_time.tzinfo else datetime.utcnow()
                time_difference = (current_time - event_time).total_seconds()
                
                # Allow 3 minutes (180 seconds) for event processing delay
                MAX_EVENT_AGE_SECONDS = 180
                
                if time_difference > MAX_EVENT_AGE_SECONDS:
                    log_structured_message("event_too_old", None, "info", {
                        "event_time": event_time_str,
                        "time_difference_seconds": time_difference,
                        "max_allowed_seconds": MAX_EVENT_AGE_SECONDS,
                        "reason": "Event is too old, skipping to prevent reprocessing old chunks"
                    })
                    return
                
                log_structured_message("event_timestamp_validated", None, "success", {
                    "event_time": event_time_str,
                    "time_difference_seconds": time_difference,
                    "status": "Event is recent, proceeding with processing"
                })
            else:
                log_structured_message("event_time_not_found", None, "warning", {
                    "reason": "Event time not found, proceeding with processing anyway"
                })
        except Exception as e:
            log_structured_message("event_timestamp_validation_error", None, "warning", {
                "error": str(e),
                "error_type": type(e).__name__,
                "action": "Proceeding with processing despite timestamp validation failure"
            })
        
        # Get blob URL from event
        event_data = azeventgrid.get_json()
        blob_url = event_data.get('url')
        
        if not blob_url:
            log_structured_message("missing_blob_url", None, "error", {
                "event_data": event_data
            })
            return
        
        # Validate blob is in pipeline folder and is .mp4
        if '/pipeline/' not in blob_url or not blob_url.lower().endswith('.mp4'):
            log_structured_message("blob_not_valid", None, "info", {
                "blob_url": blob_url,
                "reason": "Not in pipeline folder or not mp4"
            })
            return
        
        log_structured_message("processing_blob_start", None, "started", {
            "blob_url": blob_url
        })
        
        # Step 1: Parse blob URL to extract metadata
        parsed_data = parse_blob_url(blob_url)
        if not parsed_data:
            log_structured_message("blob_parsing_failed", None, "error", {
                "blob_url": blob_url
            })
            return
        
        warehouse_id, cam_id, chunk_id, date = parsed_data
        
        # Step 1.5: Check if chunk already exists in database
        if check_chunk_exists(chunk_id):
            log_structured_message("chunk_already_processed", cam_id, "info", {
                "chunk_id": chunk_id,
                "warehouse_id": warehouse_id,
                "cam_id": cam_id,
                "blob_url": blob_url,
                "action": "Skipping processing - chunk already exists in database"
            })
            return
        
        # Step 2: Insert chunk metadata into database
        insert_success = insert_chunk_metadata(warehouse_id, cam_id, chunk_id, blob_url, date)
        if not insert_success:
            log_structured_message("chunk_insert_skipped", cam_id, "warning", {
                "chunk_id": chunk_id,
                "reason": "Insert failed or chunk already existed, skipping processing"
            })
            return
        
        # Step 3: Fetch camera configuration from database
        # This is the step that uses both warehouse_id and cam_id
        camera_config = fetch_camera_config(warehouse_id, cam_id)
        if not camera_config:
            log_structured_message("camera_config_missing", cam_id, "error", {
                "warehouse_id": warehouse_id,
                "cam_id": cam_id,
                "action": "Skipping processing"
            })
            return
        
        # Step 4: Construct payload with camera config
        payload = construct_payload(blob_url, camera_config, chunk_id)
        
        # Step 5: Call FastAPI service
        result = call_fastapi_service(payload)
        
        if result["success"]:
            log_structured_message("pipeline_processing_complete", cam_id, "success", {
                "chunk_id": chunk_id,
                "blob_url": blob_url,
                "fastapi_response": result["response"]
            })
        else:
            log_structured_message("pipeline_processing_failed", cam_id, "error", {
                "chunk_id": chunk_id,
                "blob_url": blob_url,
                "error": result["message"],
                "status_code": result["status_code"]
            })
            
    except Exception as e:
        log_structured_message("pipeline_processor_exception", None, "error", {
            "error": str(e),
            "error_type": type(e).__name__
        })
        logger.error(f"Critical error in PipelineBlobProcessor: {e}")
        raise
