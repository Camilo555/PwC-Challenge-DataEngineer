#!/usr/bin/env python
"""
Prepara el dataset Online Retail II para el pipeline ETL.
Convierte Excel a CSV con el formato esperado.
"""
import pandas as pd
from pathlib import Path
import sys

def prepare_retail_data():
    """Convierte el Excel de Online Retail II a CSV con formato correcto."""
    
    # Rutas
    excel_path = Path("data/raw/online_retail_II.xlsx")
    csv_path = Path("data/raw/retail_transactions.csv")
    
    if not excel_path.exists():
        print(f"❌ Error: No se encontró {excel_path}")
        print("   Descarga el archivo de: https://archive.ics.uci.edu/ml/datasets/Online+Retail+II")
        sys.exit(1)
    
    print("📊 Leyendo archivo Excel...")
    
    try:
        # Leer ambas hojas del Excel
        df_2009_2010 = pd.read_excel(excel_path, sheet_name='Year 2009-2010')
        df_2010_2011 = pd.read_excel(excel_path, sheet_name='Year 2010-2011')
        
        print(f"   Hoja 2009-2010: {len(df_2009_2010):,} registros")
        print(f"   Hoja 2010-2011: {len(df_2010_2011):,} registros")
        
        # Combinar datasets
        df = pd.concat([df_2009_2010, df_2010_2011], ignore_index=True)
        print(f"   Total combinado: {len(df):,} registros")
        
        # Verificar columnas esperadas
        print("\n📋 Columnas originales:", df.columns.tolist())
        
        # Renombrar columnas al formato esperado por el ETL
        df.columns = df.columns.str.strip()  # Quitar espacios
        
        column_mapping = {
            'Invoice': 'invoice_no',
            'InvoiceNo': 'invoice_no',
            'StockCode': 'stock_code',
            'Description': 'description',
            'Quantity': 'quantity',
            'InvoiceDate': 'invoice_timestamp',
            'Price': 'unit_price',
            'UnitPrice': 'unit_price',
            'Customer ID': 'customer_id',
            'CustomerID': 'customer_id',
            'Country': 'country'
        }
        
        df.rename(columns=column_mapping, inplace=True)
        
        # Verificar que tenemos todas las columnas necesarias
        required_cols = ['invoice_no', 'stock_code', 'description', 'quantity', 
                        'unit_price', 'invoice_timestamp', 'customer_id', 'country']
        
        missing_cols = set(required_cols) - set(df.columns)
        if missing_cols:
            print(f"⚠️  Columnas faltantes: {missing_cols}")
            print("   Columnas disponibles:", df.columns.tolist())
        
        # Limpiar datos básicos
        print("\n🧹 Limpiando datos...")
        
        # Eliminar filas completamente vacías
        df.dropna(how='all', inplace=True)
        
        # Convertir customer_id a string (puede ser float con NaN)
        df['customer_id'] = df['customer_id'].apply(
            lambda x: str(int(x)) if pd.notna(x) else None
        )
        
        # Asegurar formato de fecha correcto
        df['invoice_timestamp'] = pd.to_datetime(df['invoice_timestamp'])
        
        # Estadísticas básicas
        print(f"\n📈 Estadísticas del dataset limpio:")
        print(f"   Registros totales: {len(df):,}")
        print(f"   Registros con customer_id nulo: {df['customer_id'].isna().sum():,}")
        print(f"   Rango de fechas: {df['invoice_timestamp'].min()} a {df['invoice_timestamp'].max()}")
        print(f"   Países únicos: {df['country'].nunique()}")
        print(f"   Productos únicos: {df['stock_code'].nunique()}")
        
        # Guardar como CSV
        df.to_csv(csv_path, index=False)
        print(f"\n✅ Datos guardados en: {csv_path}")
        print(f"   Tamaño del archivo: {csv_path.stat().st_size / 1024 / 1024:.2f} MB")
        
        # Crear un sample pequeño para pruebas rápidas
        sample_path = Path("data/raw/retail_sample.csv")
        df.head(10000).to_csv(sample_path, index=False)
        print(f"   Sample de prueba (10k registros) en: {sample_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error procesando el archivo: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = prepare_retail_data()
    sys.exit(0 if success else 1)