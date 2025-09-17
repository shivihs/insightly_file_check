from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import json
import io
import os
import uuid
import time

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    """Wykonaj czyszczenie przy starcie aplikacji"""
    cleanup_old_files()

# Ścieżka do katalogu storage względem lokalizacji main.py
STORAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage")
os.makedirs(STORAGE_DIR, exist_ok=True)

# Czyszczenie starych plików przy starcie (opcjonalne)
def cleanup_old_files(max_age_hours=24):
    """Usuwa pliki starsze niż max_age_hours"""
    try:
        current_time = time.time()
        for fname in os.listdir(STORAGE_DIR):
            fpath = os.path.join(STORAGE_DIR, fname)
            if os.path.isfile(fpath):
                # Usuń jeśli plik jest starszy niż max_age_hours
                if current_time - os.path.getmtime(fpath) > max_age_hours * 3600:
                    try:
                        os.remove(fpath)
                    except:
                        pass  # Ignoruj błędy usuwania
    except:
        pass  # Ignoruj błędy podczas czyszczenia

class FileUtils:
    COMMON_SEPARATORS = [',', ';', '\t', '|']

    @staticmethod
    def decode_content(content: bytes) -> str:
        """Bezpieczne dekodowanie bajtów na string (UTF-8 z fallbackiem na Latin-1)"""
        try:
            return content.decode('utf-8')
        except UnicodeDecodeError:
            return content.decode('latin-1')
    
    @classmethod
    def detect_csv_separator(cls, content: str) -> str:
        """Wykrywa separator w pliku CSV"""
        # Weź pierwsze kilka linii do analizy
        sample_lines = content.split('\n')[:5]
        if not sample_lines:
            return ','
        
        # Zlicz wystąpienia każdego separatora w każdej linii
        separator_counts = {sep: [] for sep in cls.COMMON_SEPARATORS}
        
        for line in sample_lines:
            if not line.strip():  # Pomijamy puste linie
                continue
            for sep in cls.COMMON_SEPARATORS:
                count = line.count(sep)
                if count > 0:  # Zliczamy tylko jeśli separator występuje
                    separator_counts[sep].append(count)
        
        # Znajdź separator z najbardziej spójną liczbą wystąpień
        best_separator = ','  # domyślny separator
        best_consistency = 0
        
        for sep, counts in separator_counts.items():
            if not counts:  # Pomijamy separatory, które nie wystąpiły
                continue
            
            # Sprawdź czy liczba kolumn jest spójna
            if len(set(counts)) == 1 and counts[0] > 0:
                # Jeśli znaleźliśmy separator z jednakową liczbą wystąpień > 0
                return sep
            
            # Alternatywnie, weź separator z największą średnią wystąpień
            avg_count = sum(counts) / len(counts) if counts else 0
            if avg_count > best_consistency:
                best_consistency = avg_count
                best_separator = sep
        
        return best_separator

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
            decoded_content = FileUtils.decode_content(content)
            
            file_info = {
                "filename": file.filename,
                "content_type": file.content_type,
                "size": len(content)
            }
            
            if file.filename.lower().endswith('.csv'):
                # Wykryj separator i wczytaj plik
                separator = FileUtils.detect_csv_separator(decoded_content)
                df = pd.read_csv(io.StringIO(decoded_content), sep=separator)
                data = df.head(10).to_dict('records')  # Limit do 100 wierszy dla podglądu
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
            decoded_content = FileUtils.decode_content(content)
            
            # Próba wczytania jako DataFrame
            # Wykryj separator i wczytaj plik
            separator = FileUtils.detect_csv_separator(decoded_content)
            df = pd.read_csv(io.StringIO(decoded_content), sep=separator)
            
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
            decoded_content = FileUtils.decode_content(content)
            
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
    
    # Wygeneruj unikalny ID
    file_id = str(uuid.uuid4())
    storage_path = os.path.join(STORAGE_DIR, f"{file_id}_{file.filename}")

    # Zapis pliku do storage
    content = await file.read()
    with open(storage_path, "wb") as f:
        f.write(content)
    await file.seek(0)  # reset dla dalszego przetwarzania

    return JSONResponse(content={
        "status": "success",
        "message": "Plik wczytany pomyślnie",
        "file_id": file_id,
        **result
    })

@app.get("/health")
async def health_check():
    """Health check"""
    return {"status": "OK"}

@app.get("/download/{file_id}")
async def download_file(file_id: str):
    """Zwraca plik z storage na podstawie file_id"""
    for fname in os.listdir(STORAGE_DIR):
        if fname.startswith(file_id + "_"):
            file_path = os.path.join(STORAGE_DIR, fname)
            return FileResponse(file_path, filename=fname.split("_", 1)[1])
    raise HTTPException(status_code=404, detail="Plik nie znaleziony")

