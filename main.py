from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import json
import io

app = FastAPI()

class FileLoader:
    @staticmethod
    def _convert_to_serializable(value: Any) -> Any:
        """Konwertuje wartości na format możliwy do serializacji JSON"""
        if isinstance(value, (pd.Timestamp, pd.Timedelta)):
            return str(value)
        elif isinstance(value, (np.int64, np.float64)):
            return int(value) if isinstance(value, np.int64) else float(value)
        elif isinstance(value, dict):
            return {k: FileLoader._convert_to_serializable(v) for k, v in value.items()}
        elif isinstance(value, (list, tuple)):
            return [FileLoader._convert_to_serializable(v) for v in value]
        elif pd.isna(value):
            return None
        return value

    @classmethod
    async def load_file(cls, file: UploadFile) -> Dict[str, Any]:
        """Wczytuje plik i zwraca jego zawartość w ujednoliconym formacie"""
        try:
            content = await file.read()
            await file.seek(0)  # Reset pozycji pliku
            
            # Próba dekodowania
            try:
                decoded_content = content.decode('utf-8')
            except UnicodeDecodeError:
                decoded_content = content.decode('latin-1')
            
            file_info = {
                "filename": file.filename,
                "content_type": file.content_type,
                "size": len(content)
            }
            
            if file.filename.lower().endswith('.csv'):
                df = pd.read_csv(io.StringIO(decoded_content))
                data = df.head(100).to_dict('records')  # Limit do 100 wierszy dla podglądu
                data = [
                    {k: cls._convert_to_serializable(v) for k, v in row.items()}
                    for row in data
                ]
                
                return {
                    "file_info": file_info,
                    "data": data,
                    "metadata": {
                        "total_rows": len(df),
                        "total_columns": len(df.columns),
                        "columns": list(df.columns),
                        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                        "memory_usage": df.memory_usage(deep=True).sum() / 1024  # KB
                    }
                }
                
            elif file.filename.lower().endswith('.json'):
                json_data = json.loads(decoded_content)
                
                if isinstance(json_data, list):
                    # Konwersja listy obiektów
                    preview_data = json_data[:100]  # Limit do 100 elementów
                    df = pd.DataFrame.from_records(preview_data)
                else:
                    # Konwersja pojedynczego obiektu
                    df = pd.DataFrame([json_data])
                    preview_data = json_data
                
                return {
                    "file_info": file_info,
                    "data": cls._convert_to_serializable(preview_data),
                    "metadata": {
                        "total_rows": len(df),
                        "total_columns": len(df.columns),
                        "columns": list(df.columns),
                        "structure_type": "list" if isinstance(json_data, list) else "dict",
                        "memory_usage": df.memory_usage(deep=True).sum() / 1024  # KB
                    }
                }
            
            else:
                raise HTTPException(
                    status_code=400,
                    detail="Nieobsługiwany format pliku. Obsługiwane: .csv, .json"
                )
                
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Błąd wczytywania pliku: {str(e)}"
            )

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
        """Walidacja zawartości JSON - akceptuje tylko struktury konwertowalne do DataFrame"""
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
            
            # Sprawdzenie struktury JSON
            if isinstance(json_data, list):
                # Lista musi zawierać słowniki
                if not json_data or not all(isinstance(item, dict) for item in json_data):
                    return {
                        "valid": False,
                        "message": "JSON musi być listą obiektów (słowników)"
                    }
                return {
                    "valid": True,
                    "message": "Zawartość JSON OK",
                    "type": "list",
                    "items": len(json_data)
                }
            elif isinstance(json_data, dict):
                # Słownik musi mieć proste wartości lub listy prostych wartości
                for value in json_data.values():
                    if isinstance(value, dict):
                        return {
                            "valid": False,
                            "message": "JSON nie może zawierać zagnieżdżonych obiektów"
                        }
                    if isinstance(value, list) and any(isinstance(item, (dict, list)) for item in value):
                        return {
                            "valid": False,
                            "message": "Listy w JSON nie mogą zawierać złożonych struktur"
                        }
                return {
                    "valid": True,
                    "message": "Zawartość JSON OK",
                    "type": "dict",
                    "keys": len(json_data.keys())
                }
            else:
                return {
                    "valid": False,
                    "message": "JSON musi być obiektem lub listą obiektów"
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

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Wczytuje i przetwarza plik CSV lub JSON.
    Zwraca ujednolicony format danych zawierający:
    - Informacje o pliku
    - Podgląd danych (do 100 wierszy)
    - Metadane (liczba wierszy, kolumn, typy danych itp.)
    """
    # Walidacja rozmiaru
    size_result = FileValidator.validate_file_size(file)
    if not size_result["valid"]:
        raise HTTPException(status_code=400, detail=size_result["message"])
    
    # Wczytanie i przetworzenie pliku
    result = await FileLoader.load_file(file)
    
    return JSONResponse(content={
        "status": "success",
        "message": "Plik wczytany pomyślnie",
        **result
    })

@app.get("/health")
async def health_check():
    """Health check"""
    return {"status": "OK"}