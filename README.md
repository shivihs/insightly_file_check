# Walidator CSV/JSON – FastAPI + Streamlit

Kompletny przepływ **upload → walidacja/preview → storage (file_id) → download → dalsze endpointy (EDA/ML)**.

> Backend: FastAPI (`main.py`) · Frontend: Streamlit (`app.py`)

---

## Architektura i przepływ

1) **Upload** (`POST /upload`)
- Front wysyła plik jako `multipart/form-data`.
- Backend waliduje rozmiar i format, bezpiecznie **dekoduje** (UTF‑8 → fallback Latin‑1), **wykrywa separator CSV**, wczytuje dane i przygotowuje **preview**.
- Backend generuje **`file_id` (UUID)** i zapisuje oryginalny plik do `./storage/` jako `file_id_nazwaPliku.ext`.
- Zwraca JSON: `status`, `message`, **`file_id`**, `file_info`, `data` (preview), `metadata`.

2) **Preview & decyzja** (front)
- Użytkownik ogląda podgląd; jeśli OK → przekazuje **`file_id`** do kolejnych endpointów (np. EDA/ML).

3) **Download** (`GET /download/{file_id}`)
- Zwraca oryginalny plik ze `storage/` po `file_id`.

4) **Walidacja on-demand**
- `/validate/csv`, `/validate/json`, `/validate/auto` – same sprawdzenia bez zapisu do storage.

> Uwaga: `UploadFile` z FastAPI żyje tylko w trakcie requestu. Dlatego zapis do `storage/` i zwrot `file_id` jest kluczowy do dalszych kroków.

---

## Wymagania

- Python **3.10+**
- Pakiety (w `requirements.txt`): `fastapi`, `uvicorn[standard]`, `python-multipart`, `pandas`, `numpy`, `streamlit`, `requests`

---

## Instalacja i uruchomienie

```bash
# 1) wirtualne środowisko
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 2) instalacja
pip install -r requirements.txt

# 3) uruchom backend (FastAPI)
uvicorn main:app --reload --port 8000
# API docs: http://localhost:8000/docs

# 4) uruchom frontend (Streamlit)
streamlit run app.py
# UI: http://localhost:8501
```

> Katalog `storage/` tworzony automatycznie, ale dodaj go do `.gitignore`.

---

## Struktura projektu

```
project/
├─ main.py           # FastAPI – upload/validate/preview/storage/download
├─ app.py            # Streamlit – UI: upload → validate/preview → upload → download
├─ storage/          # zapisywane pliki (file_id_nazwa.ext)
├─ requirements.txt
└─ README.md
```

---

## Endpointy API

### `GET /health`
- **200** → `{ "status": "OK" }`

### `POST /upload`
- Body: `multipart/form-data` z polem **file** (`UploadFile`).
- Operacje: walidacja rozmiaru i formatu, dekodowanie, wykrycie separatora CSV, przygotowanie **preview** (CSV: do **10** wierszy, JSON-list: do **100** elementów), zapis oryginalnego pliku do `storage/` z nadanym `file_id`.
- **Zwraca**: `status`, `message`, `file_id`, `file_info`, `data`, `metadata`.

### `POST /validate/csv`
- Waliduje **rozszerzenie**, **rozmiar**, **parsowalność** (z wykrytym separatorem), **pustość**.
- **Zwraca**: `rows`, `columns` (gdy poprawny).

### `POST /validate/json`
- Waliduje **rozszerzenie**, **rozmiar**, **parsowalność JSON**, prostą strukturę (lista słowników **lub** słownik bez zagnieżdżonych obiektów).
- **Zwraca**: `type` (`list`/`dict`) oraz `items` lub `keys`.

### `POST /validate/auto`
- Wybiera odpowiedni walidator na podstawie rozszerzenia (CSV/JSON).

### `GET /download/{file_id}`
- Zwraca **oryginalny** plik zapisany podczas `/upload` (po `file_id`).

---

## Format odpowiedzi (przykłady)

**`POST /upload` – CSV (skrót)**
```json
{
  "status": "success",
  "message": "Plik wczytany pomyślnie",
  "file_id": "123e4567-e89b-12d3-a456-426614174000",
  "file_info": {
    "filename": "dane.csv",
    "content_type": "text/csv",
    "size": 12345
  },
  "data": [ { "colA": "A1", "colB": "B1" }, ... up to 10 rows ... ],
  "metadata": {
    "total_rows": 3500,
    "total_columns": 12,
    "columns": ["colA", "colB", "..."],
    "dtypes": { "colA": "object", "colB": "int64" },
    "memory_usage": 42.5
  }
}
```

**`POST /upload` – JSON-list (skrót)**
```json
{
  "status": "success",
  "message": "Plik wczytany pomyślnie",
  "file_id": "123e4567-e89b-12d3-a456-426614174000",
  "file_info": { "filename": "dane.json", "content_type": "application/json", "size": 9876 },
  "data": [ { "a": 1 }, { "a": 2 } ... up to 100 items ... ],
  "metadata": {
    "total_rows": 100, "total_columns": 1, "columns": ["a"],
    "memory_usage": 1.2, "structure_type": "list"
  }
}
```

**`POST /validate/csv` – OK**
```json
{
  "status": "success",
  "message": "Plik CSV jest poprawny",
  "file_type": "csv",
  "details": { "rows": 3500, "columns": 12 }
}
```

**`POST /validate/json` – OK**
```json
{
  "status": "success",
  "message": "Plik JSON jest poprawny",
  "file_type": "json",
  "details": { "type": "list", "items": 100 }
}
```

---

## Detekcja separatora CSV

- Użyta jest **heurystyka własna** na podstawie pierwszych linii pliku (lista popularnych separatorów: `,`, `;`, `\t`, `|`), która szuka **spójnej liczby separatorów** między wierszami i ignoruje przypadki bez separatora.
- Następnie separator jest przekazywany do `pd.read_csv(..., sep=separator)`.
- Dla niektórych danych możesz dodatkowo rozważyć `engine="python"` lub `decimal=','` (gdy liczby mają przecinek).

> Podgląd CSV jest przycinany do **10 wierszy** (celowo, by response był lekki); JSON-list do **100 elementów**.

---

## Uwagi dot-JSON

- Walidacja sprawdza głównie **parsowalność** i **prostą strukturę**: lista słowników *lub* słownik z prymitywami / listami prymitywów. Zagnieżdżone obiekty są odrzucane (jeżeli wymagane – rozważ `jsonschema` w kolejnych iteracjach).
- Na czas responsu wartości nie-serializowalne (np. `np.int64`, `pd.Timestamp`) są konwertowane do typów akceptowanych przez JSON.

---

## Streamlit – przykładowy klient

- `app.py` wysyła plik do `POST /validate/auto` (szybki check) i do `POST /upload` (pełny flow + `file_id`), wyświetla **preview** i metadane.
- Przycisk „Pobierz plik” wywołuje `GET /download/{file_id}` i udostępnia `st.download_button(...)` z zawartością.

Uruchomienie:
```bash
streamlit run app.py
# UI: http://localhost:8501
```

---

## Przykłady użycia curl

**Upload (CSV):**
```bash
curl -X POST "http://localhost:8000/upload"   -F "file=@./dane.csv"
```

**Walidacja automatyczna (CSV/JSON):**
```bash
curl -X POST "http://localhost:8000/validate/auto"   -F "file=@./dane.json"
```

**Pobranie pliku po file_id:**
```bash
curl -L "http://localhost:8000/download/123e4567-e89b-12d3-a456-426614174000" -o out.csv
```

---

## Konfiguracja / parametry

- **Limit rozmiaru pliku:** `MAX_FILE_SIZE = 20 * 1024 * 1024` (20 MB) – w klasie `FileValidator`.
- **Storage:** `./storage/` (tworzony automatycznie). Nazwa pliku: `file_id_oryginalnaNazwa.ext`.
- **Preview:** CSV → 10 wierszy, JSON-list → 100 elementów (ustawione w `main.py`).

