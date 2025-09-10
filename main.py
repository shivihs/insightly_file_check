from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import json
import io

app = FastAPI()

class FileValidator:
    MAX_FILE_SIZE = 20 * 1024 * 1024  # 20 MB
    
    @classmethod
    def validate_file_size(cls, file: UploadFile) -> dict:
        """Walidacja rozmiaru pliku"""
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)
        
        if file_size > cls.MAX_FILE_SIZE:
            return {
                "valid": False,
                "message": f"Plik za duży. Max: {cls.MAX_FILE_SIZE / (1024*1024):.1f} MB"
            }
        
        return {"valid": True, "message": "Rozmiar OK"}
    
    @classmethod
    def validate_csv_format(cls, file: UploadFile) -> dict:
        """Walidacja formatu CSV"""
        if not file.filename or not file.filename.lower().endswith('.csv'):
            return {"valid": False, "message": "Plik musi mieć rozszerzenie .csv"}
        
        return {"valid": True, "message": "Format CSV OK"}
    
    @classmethod
    def validate_json_format(cls, file: UploadFile) -> dict:
        """Walidacja formatu JSON"""
        if not file.filename or not file.filename.lower().endswith('.json'):
            return {"valid": False, "message": "Plik musi mieć rozszerzenie .json"}
        
        return {"valid": True, "message": "Format JSON OK"}
    
    @classmethod
    def validate_csv_content(cls, file: UploadFile) -> dict:
        """Walidacja zawartości CSV"""
        try:
            file.file.seek(0)
            content = file.file.read()
            file.file.seek(0)
            
            # Próba dekodowania
            try:
                decoded_content = content.decode('utf-8')
            except UnicodeDecodeError:
                decoded_content = content.decode('latin-1')
            
            # Próba wczytania jako DataFrame
            df = pd.read_csv(io.StringIO(decoded_content))
            
            if df.empty:
                return {"valid": False, "message": "Plik CSV jest pusty"}
            
            return {
                "valid": True,
                "message": "Zawartość CSV OK",
                "rows": len(df),
                "columns": len(df.columns)
            }
            
        except Exception as e:
            return {"valid": False, "message": f"Błąd parsowania CSV: {str(e)}"}
    
    @classmethod
    def validate_json_content(cls, file: UploadFile) -> dict:
        """Walidacja zawartości JSON"""
        try:
            file.file.seek(0)
            content = file.file.read()
            file.file.seek(0)
            
            # Próba dekodowania
            try:
                decoded_content = content.decode('utf-8')
            except UnicodeDecodeError:
                decoded_content = content.decode('latin-1')
            
            # Próba parsowania JSON
            json_data = json.loads(decoded_content)
            
            # Sprawdzenie czy JSON nie jest pusty
            if not json_data:
                return {"valid": False, "message": "Plik JSON jest pusty"}
            
            # Podstawowe informacje o strukturze JSON
            data_type = type(json_data).__name__
            
            if isinstance(json_data, dict):
                keys_count = len(json_data.keys())
                return {
                    "valid": True,
                    "message": "Zawartość JSON OK",
                    "type": data_type,
                    "keys": keys_count
                }
            elif isinstance(json_data, list):
                items_count = len(json_data)
                return {
                    "valid": True,
                    "message": "Zawartość JSON OK",
                    "type": data_type,
                    "items": items_count
                }
            else:
                return {
                    "valid": True,
                    "message": "Zawartość JSON OK",
                    "type": data_type
                }
            
        except json.JSONDecodeError as e:
            return {"valid": False, "message": f"Błąd parsowania JSON: {str(e)}"}
        except Exception as e:
            return {"valid": False, "message": f"Błąd walidacji JSON: {str(e)}"}

@app.post("/validate/csv")
async def validate_csv(file: UploadFile = File(...)):
    """Endpoint walidacji CSV"""
    
    # Walidacja rozmiaru
    size_result = FileValidator.validate_file_size(file)
    if not size_result["valid"]:
        raise HTTPException(status_code=400, detail=size_result["message"])
    
    # Walidacja formatu CSV
    format_result = FileValidator.validate_csv_format(file)
    if not format_result["valid"]:
        raise HTTPException(status_code=400, detail=format_result["message"])
    
    # Walidacja zawartości CSV
    content_result = FileValidator.validate_csv_content(file)
    if not content_result["valid"]:
        raise HTTPException(status_code=400, detail=content_result["message"])
    
    return JSONResponse(content={
        "status": "success",
        "message": "Plik CSV jest poprawny",
        "file_type": "csv",
        "details": {
            "rows": content_result.get("rows"),
            "columns": content_result.get("columns")
        }
    })

@app.post("/validate/json")
async def validate_json(file: UploadFile = File(...)):
    """Endpoint walidacji JSON"""
    
    # Walidacja rozmiaru
    size_result = FileValidator.validate_file_size(file)
    if not size_result["valid"]:
        raise HTTPException(status_code=400, detail=size_result["message"])
    
    # Walidacja formatu JSON
    format_result = FileValidator.validate_json_format(file)
    if not format_result["valid"]:
        raise HTTPException(status_code=400, detail=format_result["message"])
    
    # Walidacja zawartości JSON
    content_result = FileValidator.validate_json_content(file)
    if not content_result["valid"]:
        raise HTTPException(status_code=400, detail=content_result["message"])
    
    return JSONResponse(content={
        "status": "success",
        "message": "Plik JSON jest poprawny",
        "file_type": "json",
        "details": {
            "type": content_result.get("type"),
            "keys": content_result.get("keys"),
            "items": content_result.get("items")
        }
    })

@app.post("/validate/auto")
async def validate_auto(file: UploadFile = File(...)):
    """Automatyczna walidacja na podstawie rozszerzenia pliku"""
    
    if not file.filename:
        raise HTTPException(status_code=400, detail="Brak nazwy pliku")
    
    filename_lower = file.filename.lower()
    
    if filename_lower.endswith('.csv'):
        return await validate_csv(file)
    elif filename_lower.endswith('.json'):
        return await validate_json(file)
    else:
        raise HTTPException(
            status_code=400, 
            detail="Nieobsługiwany format pliku. Obsługiwane: .csv, .json"
        )

@app.get("/health")
async def health_check():
    """Health check"""
    return {"status": "OK"}