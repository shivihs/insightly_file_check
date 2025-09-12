import streamlit as st
import requests
import pandas as pd
import json
import io

# Konfiguracja strony
st.set_page_config(
    page_title="Walidator CSV/JSON",
    page_icon="📊",
    layout="wide"
)

# URL FastAPI (zmień na właściwy adres)
API_URL = "http://localhost:8000"

def validate_file_via_api(uploaded_file):
    """Walidacja pliku przez FastAPI"""
    try:
        # Przygotowanie pliku do wysłania
        files = {"file": (uploaded_file.name, uploaded_file.getvalue(), uploaded_file.type)}
        
        # Wywołanie API
        response = requests.post(f"{API_URL}/validate/auto", files=files)
        
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.json().get("detail", "Nieznany błąd")}
            
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Nie można połączyć się z API. Sprawdź czy FastAPI działa na localhost:8000"}
    except Exception as e:
        return {"success": False, "error": f"Błąd: {str(e)}"}

def load_file_from_api(uploaded_file):
    """Wczytanie pliku przez API"""
    try:
        # Przygotowanie pliku do wysłania
        files = {"file": (uploaded_file.name, uploaded_file.getvalue(), uploaded_file.type)}
        
        # Wywołanie API
        response = requests.post(f"{API_URL}/upload", files=files)
        
        if response.status_code == 200:
            result = response.json()
            
            # Tworzenie DataFrame z otrzymanych danych
            df = pd.DataFrame(result["data"])
            file_type = "csv" if uploaded_file.name.lower().endswith('.csv') else "json"
            file_id = result["file_id"]
            return df, file_type, file_id, result["metadata"]
        else:
            st.error(f"Błąd API: {response.json().get('detail', 'Nieznany błąd')}")
            return None, None, None, None
            
    except requests.exceptions.ConnectionError:
        st.error("Nie można połączyć się z API. Sprawdź czy FastAPI działa na localhost:8000")
        return None, None, None, None
    except Exception as e:
        st.error(f"Błąd: {str(e)}")
        return None, None, None, None

def show_file_info(df, file_type, validation_data):
    """Prezentacja informacji o pliku"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Typ pliku", file_type.upper())
        st.metric("Wiersze", len(df))
    
    with col2:
        st.metric("Kolumny", len(df.columns))
        if file_type == "csv":
            st.metric("Rozmiar w pamięci", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")
    
    with col3:
        # Dodatkowe informacje z walidacji
        details = validation_data.get("details", {}) if isinstance(validation_data, dict) else {}
        if file_type == "json" and isinstance(details, dict):
            if isinstance(details.get("type"), (str, int)):
                st.metric("Typ JSON", str(details["type"]))
            if isinstance(details.get("keys"), (int, str)):
                st.metric("Klucze", str(details["keys"]))
            if isinstance(details.get("items"), (int, str)):
                st.metric("Elementy", str(details["items"]))

def main():
    st.title("📊 Walidator plików CSV/JSON")
    st.markdown("Prześlij plik CSV lub JSON do walidacji i podglądu")
    
    # Upload pliku
    uploaded_file = st.file_uploader(
        "Wybierz plik",
        type=['csv', 'json'],
        help="Obsługiwane formaty: CSV, JSON (max 20MB)"
    )
    
    if uploaded_file is not None:
        # Informacje o pliku
        st.subheader("📄 Informacje o pliku")
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Nazwa:** {uploaded_file.name}")
            st.write(f"**Rozmiar:** {uploaded_file.size / 1024:.1f} KB")
        with col2:
            st.write(f"**Typ:** {uploaded_file.type}")

        
        # Walidacja przez API
        st.subheader("🔍 Walidacja")
        
        with st.spinner("Walidowanie pliku..."):
            validation_result = validate_file_via_api(uploaded_file)
        
        if validation_result["success"]:
            # Walidacja przeszła pomyślnie
            st.success("✅ Plik jest poprawny!")
            
            validation_data = validation_result["data"]
            
            # Wczytanie przez API
            uploaded_file.seek(0)  # Reset pozycji pliku
            df, file_type, file_id, metadata = load_file_from_api(uploaded_file)
            
            if df is not None and metadata is not None:
                # Informacje o danych
                st.subheader("📊 Informacje o danych")
                
                # Wyświetlenie metadanych
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Typ pliku", file_type.upper())
                    st.metric("Całkowita liczba wierszy", metadata["total_rows"])
                    st.write(f"**ID:** {file_id}")
                with col2:
                    st.metric("Liczba kolumn", metadata["total_columns"])
                    st.metric("Rozmiar w pamięci", f"{metadata['memory_usage']:.1f} KB")
                
                with col3:
                    if file_type == "json":
                        st.metric("Typ struktury", metadata["structure_type"])
                    if "columns" in metadata:
                        st.metric("Liczba kolumn", len(metadata["columns"]))
                
                # Podgląd danych
                st.subheader("👀 Podgląd danych")
                
                # Opcje wyświetlania
                col1, col2 = st.columns(2)
                with col1:
                    show_rows = st.slider("Liczba wierszy do wyświetlenia", 5, min(50, len(df)), 10)
                with col2:
                    show_info = st.checkbox("Pokaż informacje o kolumnach", value=True)
                
                # Wyświetlenie DataFrame
                st.dataframe(df.head(show_rows), use_container_width=True)
                
                # Informacje o kolumnach
                if show_info and not df.empty:
                    st.subheader("📋 Informacje o kolumnach")
                    
                    info_data = []
                    for col in df.columns:
                        try:
                            n_unique = df[col].nunique() if df[col].dtype != 'object' or df[col].apply(lambda x: not isinstance(x, dict)).all() else 'N/D'
                            info_data.append({
                                "Kolumna": col,
                                "Typ": str(df[col].dtype),
                                "Brakujące": df[col].isnull().sum(),
                                "Unikalne": n_unique
                            })
                        except:
                            info_data.append({
                                "Kolumna": col,
                                "Typ": str(df[col].dtype),
                                "Brakujące": df[col].isnull().sum(),
                                "Unikalne": "N/D"
                            })
                    
                    info_df = pd.DataFrame(info_data)
                    st.dataframe(info_df, use_container_width=True)
                
                # Opcja pobrania
                st.subheader("💾 Export do CSV (tylko dla JSON)")
                if file_type == "json":
                    csv_data = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Pobierz jako CSV",
                        data=csv_data,
                        file_name=f"{uploaded_file.name.rsplit('.', 1)[0]}.csv",
                        mime="text/csv"
                    )
                st.subheader("💾 Pobierz plik z API")
                if st.button("Wczytaj plik ze storage"):
                    dl_response = requests.get(f"{API_URL}/download/{file_id}")
                    if dl_response.status_code == 200:
                        st.success("✅ Plik wczytany pomyślnie")
                        # Pobierz oryginalną nazwę pliku z response headers lub użyj domyślnej
                        original_filename = dl_response.headers.get('content-disposition', '')
                        if 'filename=' in original_filename:
                            filename = original_filename.split('filename=')[1].strip('"')
                        else:
                            filename = "no_name.csv"
                        
                        st.download_button(
                            "Zapisz plik lokalnie",
                            dl_response.content,
                            file_name=filename,
                            mime="text/csv" if filename.endswith('.csv') else "application/json" if filename.endswith('.json') else "application/octet-stream"
                        )
                    else:
                        st.error("❌ Nie udało się pobrać pliku")
        else:
            # Błąd walidacji
            st.error(f"❌ Błąd walidacji: {validation_result['error']}")
            
            # Pokaż szczegóły błędu
            with st.expander("Szczegóły błędu"):
                st.code(validation_result['error'])

    # Sidebar z informacjami
    with st.sidebar:
        st.header("ℹ️ Informacje")
        st.markdown("""
        **Obsługiwane formaty:**
        - CSV (Comma Separated Values)
        - JSON (JavaScript Object Notation)
        
        **Ograniczenia:**
        - Maksymalny rozmiar: 20MB
        - Kodowanie: UTF-8, Latin-1
        
        **Funkcje:**
        - Automatyczna walidacja
        - Podgląd danych
        - Informacje o strukturze
        - Export do CSV (dla JSON)
        """)
        
        st.header("🔧 Status API")
        try:
            health_response = requests.get(f"{API_URL}/health", timeout=2)
            if health_response.status_code == 200:
                st.success("API działa")
            else:
                st.error("Problem z API")
        except:
            st.error("API niedostępne")

if __name__ == "__main__":
    main()