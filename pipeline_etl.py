"""
PROYECTO INTEGRADOR: PIPELINE ETL ROBUSTO CON ZONA DE CUARENTENA
Ingeniería de Datos (ATD-2602) - Grupo 2 (LCNT) - Fintech y Transacciones Bancarias
"""

import re
import sqlite3
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
from pydantic import (BaseModel, ConfigDict, Field, ValidationError,
                      field_validator, model_validator)

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
DATASETS_DIR = BASE_DIR / "DATASETS"
REPORTES_DIR = BASE_DIR / "reportes"
REPORTES_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# EXTRACT: carga de las 3 fuentes crudas
# ---------------------------------------------------------------------------
def cargar_fuentes():
    """Lee las tres fuentes sin modificar nada (datos crudos)."""
    cuentas = pd.read_csv(DATASETS_DIR / "catalogo_cuentas.csv")
    atm = pd.read_csv(DATASETS_DIR / "transacciones_atm.csv")
    digitales = pd.read_json(
        DATASETS_DIR / "transferencias_digitales.json",
        dtype=False,           # evita que pandas adivine/convierta tipos
        convert_dates=False,   # las fechas se tratan después, en la Fase 4
    )
    return cuentas, atm, digitales


# ---------------------------------------------------------------------------
# FASE 1: DIAGNÓSTICO DE CALIDAD INICIAL
# ---------------------------------------------------------------------------
def perfilar(df, nombre, llave=None):
    """
    Perfilado cuantitativo de un DataFrame crudo:
      - Completitud: % de nulos por columna
      - Unicidad: duplicados exactos (y duplicados por llave, si se indica)
      - Tipos inferidos (dtypes)
    Devuelve (tabla_por_columna, resumen_dict).
    """
    total = len(df)

    tabla = pd.DataFrame({
        "dtype": df.dtypes.astype(str),
        "nulos": df.isna().sum(),
        "pct_nulos": (df.isna().mean() * 100).round(2),
        "completitud_pct": ((1 - df.isna().mean()) * 100).round(2),
        "valores_unicos": df.nunique(),
    })
    tabla.index.name = "columna"

    resumen = {
        "fuente": nombre,
        "filas": total,
        "columnas": df.shape[1],
        "duplicados_exactos": int(df.duplicated().sum()),
        "duplicados_por_llave": (
            int(df.duplicated(subset=llave).sum()) if llave else None
        ),
        "llave": llave,
    }

    # ---- Impresión en consola ----
    print("=" * 70)
    print(f"DIAGNÓSTICO INICIAL: {nombre}")
    print("=" * 70)
    print(f"Filas: {total} | Columnas: {df.shape[1]}")
    print(f"Duplicados exactos (fila completa repetida): {resumen['duplicados_exactos']}")
    if llave:
        print(f"Duplicados por llave '{llave}': {resumen['duplicados_por_llave']}")
    print("\nCompletitud y tipos por columna:")
    print(tabla.to_string())
    print()

    return tabla, resumen


def ejecutar_fase1():
    cuentas, atm, digitales = cargar_fuentes()

    resultados = [
        perfilar(cuentas, "catalogo_cuentas", llave="id_cuenta"),
        perfilar(atm, "transacciones_atm", llave="id_transaccion"),
        perfilar(digitales, "transferencias_digitales", llave="transfer_id"),
    ]

    # Guardar evidencia para el reporte comparativo antes/después
    tablas = []
    for tabla, resumen in resultados:
        t = tabla.reset_index()
        t.insert(0, "fuente", resumen["fuente"])
        tablas.append(t)
    pd.concat(tablas).to_csv(
        REPORTES_DIR / "diagnostico_inicial_columnas.csv",
        index=False, encoding="utf-8-sig",
    )
    pd.DataFrame([r for _, r in resultados]).to_csv(
        REPORTES_DIR / "diagnostico_inicial_resumen.csv",
        index=False, encoding="utf-8-sig",
    )
    print("Reportes guardados en la carpeta 'reportes/'.")

    return cuentas, atm, digitales


# ---------------------------------------------------------------------------
# FASE 2: MANEJO DE FALTANTES E IMPUTACIÓN
# ---------------------------------------------------------------------------
# Textos que en la práctica significan "sin dato" aunque no sean NaN
TOKENS_VACIOS = {"", "na", "n/a", "null", "none", "nan", "-", "--", "s/d", "sin dato"}

ETIQUETA_CAJERO = "CAJERO_DESCONOCIDO"
ETIQUETA_EMAIL = "ANONIMO"


def normalizar_vacios(df):
    """Convierte cadenas vacías / 'N/A' / 'null' en NaN real para poder contarlas
    e imputarlas. No altera ningún otro valor."""
    df = df.copy()
    for col in df.select_dtypes(exclude="number").columns:
        es_vacio = df[col].astype("string").str.strip().str.lower().isin(TOKENS_VACIOS)
        df[col] = df[col].mask(es_vacio, pd.NA)
    return df


def tasa_nulos_por_grupo(df, columna, por):
    """% de nulos de `columna` dentro de cada categoría de `por`.
    Sirve de evidencia para clasificar el faltante como MCAR o MAR."""
    t = (
        df.assign(_nulo=df[columna].isna())
        .groupby(por)["_nulo"]
        .agg(nulos="sum", total="count")
    )
    t["pct_nulos"] = (t["nulos"] / t["total"] * 100).round(2)
    return t


def eliminar_duplicados_atm(atm):
    """Elimina los reintentos de cajero (filas 100% idénticas), conservando la primera."""
    antes = len(atm)
    atm = atm.drop_duplicates(keep="first").reset_index(drop=True)
    print(f"[ATM] Duplicados exactos eliminados: {antes - len(atm)} ({antes} -> {len(atm)} filas)")
    return atm


def imputar_monto_atm(atm):
    """
    monto_mxn (numérica y MUY sesgada) -> MEDIANA por tipo de operación.
      1) Regla de recálculo: una CONSULTA_SALDO no mueve dinero, así que su monto es 0.
      2) El resto se imputa con la mediana de su mismo tipo_operacion.
      3) Respaldo: mediana global (por si algún grupo estuviera completamente vacío).
    Se deja una bandera `monto_imputado` para trazabilidad.
    """
    atm = atm.copy()
    atm["monto_imputado"] = atm["monto_mxn"].isna()

    # 1) Recálculo por regla de negocio
    es_consulta = (atm["tipo_operacion"].astype("string").str.upper() == "CONSULTA_SALDO")
    atm.loc[es_consulta & atm["monto_mxn"].isna(), "monto_mxn"] = 0.0

    # 2) Mediana por grupo
    mediana_grupo = atm.groupby("tipo_operacion")["monto_mxn"].transform("median")
    atm["monto_mxn"] = atm["monto_mxn"].fillna(mediana_grupo)

    # 3) Respaldo global
    atm["monto_mxn"] = atm["monto_mxn"].fillna(atm["monto_mxn"].median())
    return atm


def imputar_categoricas(atm, digitales):
    """Categóricas -> etiquetas controladas (no se inventan valores)."""
    atm = atm.copy()
    digitales = digitales.copy()

    atm["cajero_imputado"] = atm["id_cajero"].isna()
    atm["id_cajero"] = atm["id_cajero"].fillna(ETIQUETA_CAJERO)

    digitales["email_imputado"] = digitales["notification_email"].isna()
    digitales["notification_email"] = digitales["notification_email"].fillna(ETIQUETA_EMAIL)
    return atm, digitales


def ejecutar_fase2(cuentas, atm, digitales):
    print("=" * 70)
    print("FASE 2: MANEJO DE FALTANTES E IMPUTACIÓN")
    print("=" * 70)

    # 0) Hacer visibles los vacíos "disfrazados" (cadenas vacías, N/A, etc.)
    cuentas, atm, digitales = (normalizar_vacios(d) for d in (cuentas, atm, digitales))
    print("\nNulos REALES tras normalizar vacíos disfrazados:")
    print(f"  atm.monto_mxn: {atm['monto_mxn'].isna().sum()} | "
          f"atm.id_cajero: {atm['id_cajero'].isna().sum()} | "
          f"digitales.notification_email: {digitales['notification_email'].isna().sum()}")

    # 1) Duplicados (primero, para que no distorsionen las medianas)
    atm = eliminar_duplicados_atm(atm)

    # 2) Evidencia para clasificar MCAR / MAR
    print("\n--- Evidencia MCAR/MAR: % nulos de monto_mxn por tipo_operacion ---")
    print(tasa_nulos_por_grupo(atm, "monto_mxn", "tipo_operacion").to_string())
    print("\n--- % nulos de monto_mxn por resultado ---")
    print(tasa_nulos_por_grupo(atm, "monto_mxn", "resultado").to_string())
    print("\n--- % nulos de id_cajero por tipo_operacion ---")
    print(tasa_nulos_por_grupo(atm, "id_cajero", "tipo_operacion").to_string())
    print("\n--- % nulos de notification_email por platform ---")
    print(tasa_nulos_por_grupo(digitales, "notification_email", "platform").to_string())

    # 3) Justificación de la mediana vs. la media
    m = atm["monto_mxn"].dropna()
    print("\n--- Por qué mediana y no media (monto_mxn) ---")
    print(f"  Media   : {m.mean():,.2f}")
    print(f"  Mediana : {m.median():,.2f}")
    print(f"  Máximo  : {m.max():,.2f}")
    print(f"  Asimetría (skew): {m.skew():.2f}  (>1 = muy sesgada a la derecha)")

    # 4) Imputación
    nulos_antes = {
        "monto_mxn": int(atm["monto_mxn"].isna().sum()),
        "id_cajero": int(atm["id_cajero"].isna().sum()),
        "notification_email": int(digitales["notification_email"].isna().sum()),
    }
    atm = imputar_monto_atm(atm)
    atm, digitales = imputar_categoricas(atm, digitales)

    nulos_despues = {
        "monto_mxn": int(atm["monto_mxn"].isna().sum()),
        "id_cajero": int(atm["id_cajero"].isna().sum()),
        "notification_email": int(digitales["notification_email"].isna().sum()),
    }

    # 5) Comparativo antes / después
    comparativo = pd.DataFrame({
        "nulos_antes": nulos_antes,
        "nulos_despues": nulos_despues,
        "metodo": {
            "monto_mxn": "Mediana por tipo_operacion (CONSULTA_SALDO = 0)",
            "id_cajero": f"Etiqueta '{ETIQUETA_CAJERO}'",
            "notification_email": f"Etiqueta '{ETIQUETA_EMAIL}'",
        },
    })
    comparativo.index.name = "columna"
    print("\n--- Comparativo antes / después ---")
    print(comparativo.to_string())
    comparativo.to_csv(REPORTES_DIR / "imputacion_antes_despues.csv", encoding="utf-8-sig")
    print("\nReporte guardado: reportes/imputacion_antes_despues.csv")

    return cuentas, atm, digitales


# ---------------------------------------------------------------------------
# FASE 3: TRATAMIENTO DE OUTLIERS CON IQR (CAPPING / WINSORIZING)
# ---------------------------------------------------------------------------
FACTOR_IQR = 1.5


def limites_iqr(serie, factor=FACTOR_IQR):
    """Devuelve Q1, Q3, IQR y los umbrales inferior y superior (Tukey)."""
    q1, q3 = serie.quantile(0.25), serie.quantile(0.75)
    iqr = q3 - q1
    return q1, q3, iqr, q1 - factor * iqr, q3 + factor * iqr


def eliminar_imposibles(atm, digitales):
    """
    Único caso en que se ELIMINAN filas: valores imposibles (monto <= 0).
      - ATM: monto <= 0 en operaciones que mueven dinero (RETIRO, DEPOSITO,
        PAGO_SERVICIOS). La CONSULTA_SALDO sí tiene monto 0 de forma legítima.
      - Transferencias digitales: amount <= 0.
    """
    es_movimiento = atm["tipo_operacion"].astype("string").str.upper() != "CONSULTA_SALDO"
    imposibles_atm = es_movimiento & (atm["monto_mxn"] <= 0)
    imposibles_dig = digitales["amount"] <= 0

    print(f"[ATM] Registros con monto imposible (<= 0) eliminados: {int(imposibles_atm.sum())}")
    print(f"[DIGITALES] Registros con amount imposible (<= 0) eliminados: {int(imposibles_dig.sum())}")

    eliminados = pd.concat([
        atm[imposibles_atm].assign(fuente="transacciones_atm"),
        digitales[imposibles_dig].assign(fuente="transferencias_digitales"),
    ])
    if len(eliminados) > 0:
        eliminados.to_csv(REPORTES_DIR / "registros_imposibles_eliminados.csv",
                          index=False, encoding="utf-8-sig")

    atm = atm[~imposibles_atm].reset_index(drop=True)
    digitales = digitales[~imposibles_dig].reset_index(drop=True)
    return atm, digitales


def capear_iqr(df, columna, grupo, bandera, llave, aplicable=None):
    """
    Winsorizing con IQR (factor 1.5) calculado POR GRUPO.
    Los valores fuera de [inferior, superior] se recortan al umbral (no se borran filas).
      - columna  : variable numérica a tratar
      - grupo    : columna que define los grupos (tipo_operacion o currency)
      - bandera  : nombre de la columna booleana que marca los valores recortados
      - llave    : columna identificadora, para el detalle de auditoría
      - aplicable: máscara booleana de las filas que participan (opcional)
    Devuelve (df_tratado, tabla_de_limites, detalle_de_recortes).
    """
    df = df.copy()
    df[bandera] = False
    if aplicable is None:
        aplicable = pd.Series(True, index=df.index)

    limites, detalle = [], []
    for clave, idx in df[aplicable].groupby(grupo).groups.items():
        serie = df.loc[idx, columna]
        q1, q3, iqr, inf, sup = limites_iqr(serie)
        fuera = (serie < inf) | (serie > sup)

        limites.append({
            "columna": columna, "grupo": clave, "n": len(serie),
            "Q1": q1, "Q3": q3, "IQR": iqr,
            "umbral_inferior": inf, "umbral_superior": sup,
            "outliers": int(fuera.sum()),
        })
        if fuera.any():
            idx_fuera = fuera[fuera].index
            detalle.append(pd.DataFrame({
                "fuente_llave": df.loc[idx_fuera, llave].values,
                "columna": columna,
                "grupo": clave,
                "valor_original": serie[idx_fuera].values,
                "valor_capeado": serie[idx_fuera].clip(lower=inf, upper=sup).values,
            }))

        df.loc[idx, columna] = serie.clip(lower=inf, upper=sup)
        df.loc[fuera[fuera].index, bandera] = True

    tabla_limites = pd.DataFrame(limites)
    detalle = pd.concat(detalle) if detalle else pd.DataFrame(
        columns=["fuente_llave", "columna", "grupo", "valor_original", "valor_capeado"])
    return df, tabla_limites, detalle


def resumen_estadistico(serie):
    return {
        "media": serie.mean(), "mediana": serie.median(), "desv_est": serie.std(),
        "minimo": serie.min(), "maximo": serie.max(), "asimetria": serie.skew(),
    }


def ejecutar_fase3(cuentas, atm, digitales):
    print("=" * 70)
    print("FASE 3: TRATAMIENTO DE OUTLIERS CON IQR (CAPPING)")
    print("=" * 70)

    # 1) Eliminar únicamente valores imposibles
    atm, digitales = eliminar_imposibles(atm, digitales)

    # 2) Estadísticos ANTES
    antes_atm = resumen_estadistico(atm["monto_mxn"])
    antes_dig = resumen_estadistico(digitales["amount"])

    # 3) Capping con IQR
    #    ATM: por tipo_operacion, SIN incluir CONSULTA_SALDO (su monto es 0 por definición,
    #         incluirlo desplazaría artificialmente los cuartiles).
    es_movimiento = atm["tipo_operacion"].astype("string").str.upper() != "CONSULTA_SALDO"
    atm, lim_atm, det_atm = capear_iqr(
        atm, "monto_mxn", grupo="tipo_operacion", bandera="monto_capeado",
        llave="id_transaccion", aplicable=es_movimiento)

    #    Digitales: por moneda (MXN y USD tienen escalas distintas; la conversión es la Fase 4).
    digitales, lim_dig, det_dig = capear_iqr(
        digitales, "amount", grupo="currency", bandera="amount_capeado",
        llave="transfer_id")

    limites = pd.concat([lim_atm, lim_dig], ignore_index=True)
    detalle = pd.concat([det_atm, det_dig], ignore_index=True)

    print("\n--- Umbrales IQR (factor 1.5) por grupo ---")
    print(limites.round(2).to_string(index=False))
    print(f"\nTotal de valores recortados (winsorizing): {len(detalle)}")
    if len(detalle) > 0:
        print(detalle.round(2).to_string(index=False))

    # 4) Estadísticos DESPUÉS y comparativo
    despues_atm = resumen_estadistico(atm["monto_mxn"])
    despues_dig = resumen_estadistico(digitales["amount"])

    comparativo = pd.DataFrame({
        "atm_monto_antes": antes_atm, "atm_monto_despues": despues_atm,
        "dig_amount_antes": antes_dig, "dig_amount_despues": despues_dig,
    })
    print("\n--- Comparativo estadístico antes / después ---")
    print(comparativo.round(2).to_string())

    limites.to_csv(REPORTES_DIR / "outliers_limites_iqr.csv", index=False, encoding="utf-8-sig")
    detalle.to_csv(REPORTES_DIR / "outliers_detalle_recortes.csv", index=False, encoding="utf-8-sig")
    comparativo.to_csv(REPORTES_DIR / "outliers_antes_despues.csv", encoding="utf-8-sig")
    print("\nReportes guardados: outliers_limites_iqr.csv, outliers_detalle_recortes.csv, "
          "outliers_antes_despues.csv")
    print(f"Filas finales -> ATM: {len(atm)} | Digitales: {len(digitales)} (no se perdió ninguna por outliers)")

    return cuentas, atm, digitales


# ---------------------------------------------------------------------------
# FASE 4: INTEGRACIÓN, UNIFICACIÓN Y HUÉRFANOS
# ---------------------------------------------------------------------------
TIPO_CAMBIO_USD_MXN = 19.50              # paridad oficial indicada en la guía
OFFSET_LOCAL = pd.Timedelta(hours=6)     # Colima: UTC-6 (sin horario de verano)


def patron_formato(valor):
    """Reduce un valor a su 'molde' (cada dígito -> 9) para listar formatos distintos."""
    return re.sub(r"\d", "9", str(valor))


def parsear_fechas_atm(serie):
    """
    fecha_hora del ATM llega en 3 formatos mezclados:
        2026/09/15 8:45:00  |  2026-10-12 22:22:00  |  12/09/2026 13:26 (DD/MM/AAAA)
    Se prueba cada formato explícito (así no hay ambigüedad DD/MM vs MM/DD).
    Lo que no encaje en ninguno queda como NaT y se detectará después.
    """
    s = serie.astype("string").str.strip()
    resultado = None
    for formato in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M"):
        parcial = pd.to_datetime(s, format=formato, errors="coerce")
        resultado = parcial if resultado is None else resultado.fillna(parcial)
    return resultado


def parsear_timestamps_digitales(serie):
    """
    timestamp del JSON llega como texto ISO con zona ('2026-09-12T19:49:00-06:00')
    o como número epoch en segundos (1788816180). Ambos se llevan a hora local (UTC-6).
    """
    es_epoch = serie.map(lambda x: isinstance(x, (int, float)) and not isinstance(x, bool))
    epoch = pd.to_datetime(pd.to_numeric(serie.where(es_epoch), errors="coerce"),
                           unit="s", utc=True)
    iso = pd.to_datetime(serie.where(~es_epoch).astype("string"),
                         format="ISO8601", utc=True, errors="coerce")
    return epoch.fillna(iso).dt.tz_convert(None) - OFFSET_LOCAL


def construir_transacciones(atm, digitales):
    """
    Fusiona ATM (CSV) + transferencias digitales (JSON) en UNA sola tabla con
    esquema común: fechas ISO (YYYY-MM-DD), importes en MXN y textos normalizados.
    """
    f_atm = parsear_fechas_atm(atm["fecha_hora"])
    f_dig = parsear_timestamps_digitales(digitales["timestamp"])

    t_atm = pd.DataFrame({
        "id_transaccion": atm["id_transaccion"],
        "canal": "ATM",
        "fecha": f_atm.dt.strftime("%Y-%m-%d"),
        "hora": f_atm.dt.strftime("%H:%M:%S"),
        "id_cuenta": atm["id_cuenta"].astype("string").str.strip().str.upper(),
        "tipo_operacion": atm["tipo_operacion"].astype("string").str.strip().str.upper(),
        "concepto": pd.NA,
        "monto_original": atm["monto_mxn"],
        "moneda_original": "MXN",
        "monto_mxn": atm["monto_mxn"],
        "estado": atm["resultado"].astype("string").str.strip().str.upper(),
        "id_cajero": atm["id_cajero"],
        "comision_aplicada": atm["comision_aplicada"],
        "clabe_beneficiario": pd.NA,
        "email_notificacion": pd.NA,
        "plataforma": pd.NA,
        "monto_imputado": atm["monto_imputado"],
        "monto_capeado": atm["monto_capeado"],
        "cajero_imputado": atm["cajero_imputado"],
        "email_imputado": False,
    })

    moneda = digitales["currency"].astype("string").str.strip().str.upper()
    factor = moneda.map({"MXN": 1.0, "USD": TIPO_CAMBIO_USD_MXN}).astype("float64")
    t_dig = pd.DataFrame({
        "id_transaccion": digitales["transfer_id"],
        "canal": "DIGITAL",
        "fecha": f_dig.dt.strftime("%Y-%m-%d"),
        "hora": f_dig.dt.strftime("%H:%M:%S"),
        "id_cuenta": digitales["account_id"].astype("string").str.strip().str.upper(),
        "tipo_operacion": "TRANSFERENCIA_SPEI",
        "concepto": digitales["concept"],
        "monto_original": digitales["amount"],
        "moneda_original": moneda,
        "monto_mxn": (digitales["amount"] * factor).round(2),
        "estado": digitales["status"].astype("string").str.strip().str.upper(),
        "id_cajero": pd.NA,
        "comision_aplicada": pd.NA,
        "clabe_beneficiario": digitales["beneficiary_clabe"],
        "email_notificacion": digitales["notification_email"],
        "plataforma": digitales["platform"],
        "monto_imputado": False,
        "monto_capeado": digitales["amount_capeado"],
        "cajero_imputado": False,
        "email_imputado": digitales["email_imputado"],
    })

    return pd.concat([t_atm, t_dig], ignore_index=True)


def cruzar_con_catalogo(tx, cuentas):
    """
    Merge (left) contra el catálogo maestro de cuentas.
    Las transacciones cuya cuenta NO existe en el catálogo se marcan como huérfanas
    (cuenta_huerfana = True) para desviarlas a cuarentena en la Fase 6.
    """
    cat = cuentas[["id_cuenta", "tipo_cuenta", "estado_cuenta"]].copy()
    cat["id_cuenta"] = cat["id_cuenta"].astype("string").str.strip().str.upper()

    tx = tx.merge(cat, on="id_cuenta", how="left", validate="m:1", indicator=True)
    tx["cuenta_huerfana"] = tx["_merge"].astype(str).eq("left_only")
    return tx.drop(columns="_merge")


def ejecutar_fase4(cuentas, atm, digitales):
    print("=" * 70)
    print("FASE 4: INTEGRACIÓN, UNIFICACIÓN Y HUÉRFANOS")
    print("=" * 70)

    # 1) Formatos de fecha ANTES de homologar
    print("\n--- Formatos de fecha detectados (antes) ---")
    print("ATM.fecha_hora:")
    print(atm["fecha_hora"].map(patron_formato).value_counts().to_string())
    print("\nDIGITALES.timestamp:")
    print(digitales["timestamp"].map(
        lambda x: f"{type(x).__name__}: {patron_formato(x)}").value_counts().to_string())

    # 2) Catálogo: fecha de apertura a ISO
    cuentas = cuentas.copy()
    cuentas["fecha_apertura"] = pd.to_datetime(
        cuentas["fecha_apertura"], errors="coerce").dt.strftime("%Y-%m-%d")

    # 3) Fusión + homologación de fechas + conversión de moneda
    tx = construir_transacciones(atm, digitales)

    sin_fecha = int(tx["fecha"].isna().sum())
    print(f"\nFechas que no se pudieron interpretar: {sin_fecha}")
    print(f"Rango de fechas homologado: {tx['fecha'].min()} a {tx['fecha'].max()}")

    n_usd = int((tx["moneda_original"] == "USD").sum())
    print(f"Importes convertidos de USD a MXN (x {TIPO_CAMBIO_USD_MXN}): {n_usd}")
    ejemplo = tx[tx["moneda_original"] == "USD"].head(3)[
        ["id_transaccion", "monto_original", "moneda_original", "monto_mxn"]]
    print(ejemplo.to_string(index=False))

    # 4) Cruce contra catálogo maestro y detección de huérfanos
    tx = cruzar_con_catalogo(tx, cuentas)
    huerfanos = tx[tx["cuenta_huerfana"]]
    resumen_huerfanos = (
        tx.groupby("canal")["cuenta_huerfana"].agg(huerfanas="sum", total="count")
    )
    resumen_huerfanos["pct_huerfanas"] = (
        resumen_huerfanos["huerfanas"] / resumen_huerfanos["total"] * 100).round(2)
    print("\n--- Transacciones con cuenta huérfana (no existe en el catálogo) ---")
    print(resumen_huerfanos.to_string())
    print(f"Total huérfanas: {len(huerfanos)} de {len(tx)} "
          f"({len(huerfanos) / len(tx) * 100:.2f}%) -> se desviarán a cuarentena en la Fase 6")
    print("Ejemplos de cuentas huérfanas:", ", ".join(huerfanos["id_cuenta"].head(5)))

    # 5) Vista de la tabla integrada
    print(f"\nTabla integrada: {tx.shape[0]} filas x {tx.shape[1]} columnas")
    print(tx["canal"].value_counts().to_string())
    print("\nEstados normalizados:")
    print(tx["estado"].value_counts().to_string())
    print("\nMuestra de la tabla integrada:")
    print(tx[["id_transaccion", "canal", "fecha", "hora", "id_cuenta",
              "monto_mxn", "estado", "cuenta_huerfana"]].head(5).to_string(index=False))

    # 6) Reportes
    tx.to_csv(REPORTES_DIR / "transacciones_integradas.csv", index=False, encoding="utf-8-sig")
    huerfanos[["id_transaccion", "canal", "id_cuenta", "monto_mxn"]].to_csv(
        REPORTES_DIR / "huerfanos_detalle.csv", index=False, encoding="utf-8-sig")
    resumen_huerfanos.to_csv(REPORTES_DIR / "huerfanos_resumen.csv", encoding="utf-8-sig")
    print("\nReportes guardados: transacciones_integradas.csv, huerfanos_detalle.csv, "
          "huerfanos_resumen.csv")

    return cuentas, tx


# ---------------------------------------------------------------------------
# FASE 5: TRANSFORMACIÓN, DISCRETIZACIÓN Y ESCALAMIENTO
# ---------------------------------------------------------------------------
ETIQUETAS_SEGMENTO = ("Bajo", "Medio", "Alto")


def escalar_min_max(serie):
    """
    Normalización Min-Max al rango [0, 1]:  x' = (x - min) / (max - min)
    Devuelve (serie_escalada, minimo, maximo).
    """
    minimo, maximo = serie.min(), serie.max()
    rango = maximo - minimo
    if rango == 0:                      # protección: columna constante
        return pd.Series(0.0, index=serie.index), minimo, maximo
    return (serie - minimo) / rango, minimo, maximo


def discretizar_terciles(serie, etiquetas=ETIQUETAS_SEGMENTO):
    """
    Discretiza en 3 segmentos con cuantiles (terciles):
        Bajo  : valor <= percentil 33.3
        Medio : percentil 33.3 < valor <= percentil 66.7
        Alto  : valor  > percentil 66.7
    Devuelve (segmentos, corte_1, corte_2).
    """
    corte_1, corte_2 = serie.quantile(1 / 3), serie.quantile(2 / 3)
    segmentos = pd.cut(
        serie, bins=[float("-inf"), corte_1, corte_2, float("inf")],
        labels=list(etiquetas),
    )
    return segmentos.astype("string"), corte_1, corte_2


def ejecutar_fase5(cuentas, tx):
    print("=" * 70)
    print("FASE 5: TRANSFORMACIÓN, DISCRETIZACIÓN Y ESCALAMIENTO")
    print("=" * 70)

    tx = tx.copy()

    # 1) Escalamiento Min-Max de monto_mxn
    tx["monto_mxn_norm"], minimo, maximo = escalar_min_max(tx["monto_mxn"])
    tx["monto_mxn_norm"] = tx["monto_mxn_norm"].round(6)

    print("\n--- Escalamiento Min-Max de monto_mxn ---")
    print(f"  Mínimo : {minimo:,.2f}  -> 0.0")
    print(f"  Máximo : {maximo:,.2f}  -> 1.0")
    print(f"  Rango resultante: [{tx['monto_mxn_norm'].min()}, {tx['monto_mxn_norm'].max()}]")

    # 2) Discretización en 3 segmentos por terciles
    tx["segmento_monto"], corte_1, corte_2 = discretizar_terciles(tx["monto_mxn"])

    print("\n--- Discretización por terciles (Bajo / Medio / Alto) ---")
    print(f"  Bajo  : monto_mxn <= {corte_1:,.2f}")
    print(f"  Medio : {corte_1:,.2f} < monto_mxn <= {corte_2:,.2f}")
    print(f"  Alto  : monto_mxn >  {corte_2:,.2f}")

    resumen_segmentos = (
        tx.groupby("segmento_monto")["monto_mxn"]
        .agg(registros="count", monto_min="min", monto_max="max")
        .reindex(list(ETIQUETAS_SEGMENTO))
    )
    print("\nRegistros por segmento:")
    print(resumen_segmentos.round(2).to_string())

    cruce = pd.crosstab([tx["canal"], tx["moneda_original"]], tx["segmento_monto"])
    cruce = cruce.reindex(columns=list(ETIQUETAS_SEGMENTO), fill_value=0)
    print("\nSegmentos por canal y moneda original:")
    print(cruce.to_string())

    # 3) Muestra de la transformación
    print("\nMuestra (un registro de cada segmento):")
    muestra = pd.concat([
        tx[tx["segmento_monto"] == s].sort_values("monto_mxn").iloc[[len(tx[tx["segmento_monto"] == s]) // 2]]
        for s in ETIQUETAS_SEGMENTO
    ])
    print(muestra[["id_transaccion", "canal", "monto_mxn", "monto_mxn_norm",
                   "segmento_monto"]].to_string(index=False))

    # 4) Reportes
    parametros = pd.DataFrame({
        "parametro": ["minmax_minimo", "minmax_maximo", "tercil_1 (p33.3)", "tercil_2 (p66.7)"],
        "valor": [minimo, maximo, corte_1, corte_2],
    })
    parametros.to_csv(REPORTES_DIR / "transformacion_parametros.csv", index=False, encoding="utf-8-sig")
    cruce.to_csv(REPORTES_DIR / "segmentos_por_canal.csv", encoding="utf-8-sig")
    tx.to_csv(REPORTES_DIR / "transacciones_transformadas.csv", index=False, encoding="utf-8-sig")
    print("\nReportes guardados: transformacion_parametros.csv, segmentos_por_canal.csv, "
          "transacciones_transformadas.csv")
    print(f"Tabla final de la fase: {tx.shape[0]} filas x {tx.shape[1]} columnas")

    return cuentas, tx


# ---------------------------------------------------------------------------
# FASE 6: VALIDACIÓN CON PYDANTIC Y ZONA DE CUARENTENA
# ---------------------------------------------------------------------------
PATRON_ID = re.compile(r"^(ATM-TX|SPEI-\d{4})-\d{5}$")
PATRON_HORA = re.compile(r"^\d{2}:\d{2}:\d{2}$")
PATRON_CUENTA = re.compile(r"^CTA-\d{4}$")
PATRON_EMAIL = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


class TransaccionLimpia(BaseModel):
    """Esquema estricto de la tabla final `transacciones_limpias`."""
    model_config = ConfigDict(str_strip_whitespace=True)

    id_transaccion: str
    canal: Literal["ATM", "DIGITAL"]
    fecha: date
    hora: str
    id_cuenta: str
    tipo_operacion: Literal["RETIRO", "DEPOSITO", "PAGO_SERVICIOS",
                            "CONSULTA_SALDO", "TRANSFERENCIA_SPEI"]
    concepto: Optional[str] = None
    monto_original: float = Field(ge=0)
    moneda_original: Literal["MXN", "USD"]
    monto_mxn: float = Field(ge=0)
    estado: Literal["EXITOSA", "FALLO_FONDOS", "FALLO_CONEXION", "DEVUELTA",
                    "RECHAZADA", "LIQUIDADA", "PENDIENTE"]
    id_cajero: Optional[str] = None
    comision_aplicada: Optional[float] = Field(default=None, ge=0)
    clabe_beneficiario: Optional[str] = None
    email_notificacion: Optional[str] = None
    plataforma: Optional[str] = None
    monto_imputado: bool
    monto_capeado: bool
    cajero_imputado: bool
    email_imputado: bool
    tipo_cuenta: Optional[str] = None
    estado_cuenta: Optional[str] = None
    cuenta_huerfana: bool
    monto_mxn_norm: float = Field(ge=0, le=1)
    segmento_monto: Literal["Bajo", "Medio", "Alto"]

    # ---- Validadores por campo (todos se evalúan; los errores se acumulan) ----
    @field_validator("id_transaccion")
    @classmethod
    def validar_id(cls, v):
        if not PATRON_ID.match(v):
            raise ValueError("ID_INVALIDO: formato de id_transaccion no reconocido")
        return v

    @field_validator("hora")
    @classmethod
    def validar_hora(cls, v):
        if not PATRON_HORA.match(v):
            raise ValueError("HORA_INVALIDA: debe tener formato HH:MM:SS")
        return v

    @field_validator("id_cuenta")
    @classmethod
    def validar_id_cuenta(cls, v):
        if not PATRON_CUENTA.match(v):
            raise ValueError("CUENTA_FORMATO_INVALIDO: debe tener formato CTA-0000")
        return v

    @field_validator("clabe_beneficiario")
    @classmethod
    def validar_clabe(cls, v):
        if v is None:
            return v
        if not (v.isdigit() and len(v) == 18):
            raise ValueError(f"CLABE_INVALIDA: debe tener 18 dígitos numéricos (trae {len(v)})")
        return v

    @field_validator("email_notificacion")
    @classmethod
    def validar_email(cls, v):
        if v is None or v == ETIQUETA_EMAIL:
            return v
        if not PATRON_EMAIL.match(v):
            raise ValueError(f"EMAIL_MALFORMADO: '{v}' no es un correo válido")
        return v

    @field_validator("cuenta_huerfana")
    @classmethod
    def validar_huerfana(cls, v):
        if v:
            raise ValueError("CUENTA_HUERFANA: la cuenta no existe en el catálogo maestro")
        return v

    # ---- Reglas de negocio entre campos (solo corren si los campos ya son válidos) ----
    @model_validator(mode="after")
    def reglas_de_negocio(self):
        if self.tipo_operacion != "CONSULTA_SALDO" and self.monto_mxn <= 0:
            raise ValueError("MONTO_INVALIDO: una operación que mueve dinero debe tener monto > 0")

        factor = TIPO_CAMBIO_USD_MXN if self.moneda_original == "USD" else 1.0
        if abs(self.monto_mxn - self.monto_original * factor) > 0.01:
            raise ValueError("CONVERSION_INCONSISTENTE: monto_mxn no coincide con monto_original x tipo de cambio")

        if self.canal == "ATM" and not self.id_cajero:
            raise ValueError("ATM_SIN_CAJERO: toda transacción de ATM debe traer id_cajero")

        if self.canal == "DIGITAL" and not (self.clabe_beneficiario and self.email_notificacion):
            raise ValueError("DIGITAL_INCOMPLETO: toda transferencia digital debe traer CLABE y correo")
        return self


def limpiar_registro(registro):
    """Convierte NaN / pd.NA a None y tipos numpy a tipos nativos de Python."""
    limpio = {}
    for clave, valor in registro.items():
        if valor is None or (not isinstance(valor, (list, dict)) and pd.isna(valor)):
            limpio[clave] = None
        elif hasattr(valor, "item"):
            limpio[clave] = valor.item()
        else:
            limpio[clave] = valor
    return limpio


def resumir_errores(error):
    """
    Convierte un ValidationError en un texto con TODOS los motivos, p. ej.:
      'CLABE_INVALIDA: debe tener 18 dígitos (trae 5) | CUENTA_HUERFANA: ...'
    """
    motivos = []
    for e in error.errors():
        mensaje = e["msg"].removeprefix("Value error, ")
        if re.match(r"^[A-Z_]+: ", mensaje):
            motivos.append(mensaje)
        else:  # errores de tipo/valor generados por el propio Pydantic
            campo = ".".join(str(x) for x in e["loc"]) or "registro"
            motivos.append(f"VALOR_INVALIDO: {campo}: {mensaje}")
    return " | ".join(motivos)


def validar_lote(tx):
    """
    Valida registro por registro. Un registro inválido NO detiene el lote:
    se captura el ValidationError y se desvía a cuarentena con su motivo.
    """
    validos, cuarentena = [], []
    for registro in tx.to_dict("records"):
        registro = limpiar_registro(registro)
        try:
            modelo = TransaccionLimpia(**registro)
            validos.append(modelo.model_dump(mode="json"))
        except ValidationError as error:
            cuarentena.append({**registro, "motivo_rechazo_cuarentena": resumir_errores(error)})
        except Exception as error:  # defensa final: nada debe tumbar el pipeline
            cuarentena.append({**registro,
                               "motivo_rechazo_cuarentena": f"ERROR_INESPERADO: {type(error).__name__}: {error}"})
    return pd.DataFrame(validos), pd.DataFrame(cuarentena)


def ejecutar_fase6(cuentas, tx):
    print("=" * 70)
    print("FASE 6: VALIDACIÓN CON PYDANTIC Y ZONA DE CUARENTENA")
    print("=" * 70)

    validos, cuarentena = validar_lote(tx)

    total = len(tx)
    print(f"\nRegistros recibidos      : {total}")
    print(f"Registros válidos        : {len(validos)}")
    print(f"Registros en cuarentena  : {len(cuarentena)}")
    print(f"Control de integridad    : {len(validos)} + {len(cuarentena)} = "
          f"{len(validos) + len(cuarentena)} (debe ser {total})")

    if len(cuarentena) > 0:
        codigos = Counter()
        for motivo in cuarentena["motivo_rechazo_cuarentena"]:
            codigos.update(re.findall(r"(?:^|\| )([A-Z_]+):", motivo))
        resumen = pd.DataFrame(codigos.most_common(), columns=["motivo", "registros"])
        multiples = int((cuarentena["motivo_rechazo_cuarentena"].str.count(r"\|") > 0).sum())

        print("\n--- Motivos de rechazo (un registro puede tener varios) ---")
        print(resumen.to_string(index=False))
        print(f"Registros con más de un motivo: {multiples}")

        print("\n--- Cuarentena por canal ---")
        print(cuarentena["canal"].value_counts().to_string())

        print("\n--- Muestra de cuarentena ---")
        print(cuarentena[["id_transaccion", "canal", "id_cuenta", "motivo_rechazo_cuarentena"]]
              .head(6).to_string(index=False, max_colwidth=75))

        resumen.to_csv(REPORTES_DIR / "cuarentena_resumen.csv", index=False, encoding="utf-8-sig")
        cuarentena.to_csv(REPORTES_DIR / "cuarentena_errores.csv", index=False, encoding="utf-8-sig")

    validos.to_csv(REPORTES_DIR / "registros_validos.csv", index=False, encoding="utf-8-sig")
    print("\nReportes guardados: registros_validos.csv, cuarentena_errores.csv, cuarentena_resumen.csv")

    return cuentas, validos, cuarentena


# ---------------------------------------------------------------------------
# FASE 7: CARGA TRANSACCIONAL A BASE DE DATOS (SQLite)
# ---------------------------------------------------------------------------
DB_PATH = BASE_DIR / "pipeline_fintech.db"

COLUMNAS_TX = [
    "id_transaccion", "canal", "fecha", "hora", "id_cuenta", "tipo_operacion",
    "concepto", "monto_original", "moneda_original", "monto_mxn", "estado",
    "id_cajero", "comision_aplicada", "clabe_beneficiario", "email_notificacion",
    "plataforma", "monto_imputado", "monto_capeado", "cajero_imputado",
    "email_imputado", "tipo_cuenta", "estado_cuenta", "cuenta_huerfana",
    "monto_mxn_norm", "segmento_monto",
]

# Tabla final: restricciones estrictas (segunda barrera de defensa tras Pydantic)
DDL_TRANSACCIONES_LIMPIAS = """
CREATE TABLE IF NOT EXISTS transacciones_limpias (
    id_transaccion      TEXT PRIMARY KEY,
    canal               TEXT NOT NULL CHECK (canal IN ('ATM', 'DIGITAL')),
    fecha               TEXT NOT NULL,
    hora                TEXT NOT NULL,
    id_cuenta           TEXT NOT NULL,
    tipo_operacion      TEXT NOT NULL,
    concepto            TEXT,
    monto_original      REAL NOT NULL CHECK (monto_original >= 0),
    moneda_original     TEXT NOT NULL CHECK (moneda_original IN ('MXN', 'USD')),
    monto_mxn           REAL NOT NULL CHECK (monto_mxn >= 0),
    estado              TEXT NOT NULL,
    id_cajero           TEXT,
    comision_aplicada   REAL CHECK (comision_aplicada IS NULL OR comision_aplicada >= 0),
    clabe_beneficiario  TEXT,
    email_notificacion  TEXT,
    plataforma          TEXT,
    monto_imputado      INTEGER NOT NULL,
    monto_capeado       INTEGER NOT NULL,
    cajero_imputado     INTEGER NOT NULL,
    email_imputado      INTEGER NOT NULL,
    tipo_cuenta         TEXT,
    estado_cuenta       TEXT,
    cuenta_huerfana     INTEGER NOT NULL,
    monto_mxn_norm      REAL NOT NULL CHECK (monto_mxn_norm BETWEEN 0 AND 1),
    segmento_monto      TEXT NOT NULL CHECK (segmento_monto IN ('Bajo', 'Medio', 'Alto'))
)
"""

# Cuarentena: sin restricciones (guarda lo que llegó, aunque esté corrupto) + el motivo
DDL_CUARENTENA_ERRORES = """
CREATE TABLE IF NOT EXISTS cuarentena_errores (
    id_cuarentena              INTEGER PRIMARY KEY AUTOINCREMENT,
    id_transaccion             TEXT,
    canal                      TEXT,
    fecha                      TEXT,
    hora                       TEXT,
    id_cuenta                  TEXT,
    tipo_operacion             TEXT,
    concepto                   TEXT,
    monto_original             REAL,
    moneda_original            TEXT,
    monto_mxn                  REAL,
    estado                     TEXT,
    id_cajero                  TEXT,
    comision_aplicada          REAL,
    clabe_beneficiario         TEXT,
    email_notificacion         TEXT,
    plataforma                 TEXT,
    monto_imputado             INTEGER,
    monto_capeado              INTEGER,
    cajero_imputado            INTEGER,
    email_imputado             INTEGER,
    tipo_cuenta                TEXT,
    estado_cuenta              TEXT,
    cuenta_huerfana            INTEGER,
    monto_mxn_norm             REAL,
    segmento_monto             TEXT,
    motivo_rechazo_cuarentena  TEXT NOT NULL,
    fecha_carga                TEXT DEFAULT CURRENT_TIMESTAMP
)
"""


def crear_esquema(conn):
    conn.execute(DDL_TRANSACCIONES_LIMPIAS)
    conn.execute(DDL_CUARENTENA_ERRORES)
    conn.commit()


def filas_para_insertar(df, columnas):
    """Convierte un DataFrame en una lista de tuplas listas para sqlite3."""
    filas = []
    for registro in df.to_dict("records"):
        registro = limpiar_registro(registro)
        filas.append(tuple(registro.get(c) for c in columnas))
    return filas


def cargar_lote(conn, validos, cuarentena):
    """
    Carga ATÓMICA: o entran las dos tablas completas o no entra nada.
      - Dentro de UNA transacción se vacían las tablas y se insertan los registros
        (así, recargar el pipeline no duplica datos).
      - Si todo sale bien -> conn.commit()
      - Si algo falla      -> conn.rollback(): la base queda exactamente como estaba.
    Devuelve (exito, mensaje).
    """
    marcadores = ", ".join("?" * len(COLUMNAS_TX))
    sql_validos = (f"INSERT INTO transacciones_limpias ({', '.join(COLUMNAS_TX)}) "
                   f"VALUES ({marcadores})")

    columnas_q = COLUMNAS_TX + ["motivo_rechazo_cuarentena"]
    marcadores_q = ", ".join("?" * len(columnas_q))
    sql_cuarentena = (f"INSERT INTO cuarentena_errores ({', '.join(columnas_q)}) "
                      f"VALUES ({marcadores_q})")

    try:
        conn.execute("DELETE FROM transacciones_limpias")
        conn.execute("DELETE FROM cuarentena_errores")
        conn.executemany(sql_validos, filas_para_insertar(validos, COLUMNAS_TX))
        if len(cuarentena) > 0:
            conn.executemany(sql_cuarentena, filas_para_insertar(cuarentena, columnas_q))
        conn.commit()
        return True, "COMMIT"
    except Exception as error:
        conn.rollback()
        return False, f"ROLLBACK por {type(error).__name__}: {error}"


def contar(conn, tabla):
    return conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]


def verificar_bd(conn):
    print("\n--- Verificación de la base de datos ---")
    print(f"  transacciones_limpias : {contar(conn, 'transacciones_limpias')} filas")
    print(f"  cuarentena_errores    : {contar(conn, 'cuarentena_errores')} filas")
    print(f"  PRAGMA integrity_check: {conn.execute('PRAGMA integrity_check').fetchone()[0]}")

    print("\nTransacciones limpias por canal:")
    for canal, n in conn.execute(
            "SELECT canal, COUNT(*) FROM transacciones_limpias GROUP BY canal ORDER BY canal"):
        print(f"  {canal:<8} {n}")

    print("\nTransacciones limpias por segmento:")
    for seg, n in conn.execute(
            "SELECT segmento_monto, COUNT(*) FROM transacciones_limpias "
            "GROUP BY segmento_monto ORDER BY CASE segmento_monto "
            "WHEN 'Bajo' THEN 1 WHEN 'Medio' THEN 2 ELSE 3 END"):
        print(f"  {seg:<8} {n}")

    print("\nCuarentena por motivo (texto completo):")
    for motivo, n in conn.execute(
            "SELECT substr(motivo_rechazo_cuarentena, 1, 60), COUNT(*) FROM cuarentena_errores "
            "GROUP BY motivo_rechazo_cuarentena ORDER BY COUNT(*) DESC LIMIT 6"):
        print(f"  {n:>4}  {motivo}")

    print("\nMuestra de transacciones_limpias:")
    for fila in conn.execute(
            "SELECT id_transaccion, canal, fecha, id_cuenta, monto_mxn, estado "
            "FROM transacciones_limpias LIMIT 3"):
        print("  ", fila)


def ejecutar_fase7(cuentas, validos, cuarentena):
    print("=" * 70)
    print("FASE 7: CARGA TRANSACCIONAL A BASE DE DATOS (SQLite)")
    print("=" * 70)

    conn = sqlite3.connect(DB_PATH)
    try:
        crear_esquema(conn)
        exito, mensaje = cargar_lote(conn, validos, cuarentena)
        print(f"\nBase de datos: {DB_PATH.name}")
        print(f"Resultado de la carga: {mensaje}")
        if exito:
            verificar_bd(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    cuentas, atm, digitales = ejecutar_fase1()
    cuentas, atm, digitales = ejecutar_fase2(cuentas, atm, digitales)
    cuentas, atm, digitales = ejecutar_fase3(cuentas, atm, digitales)
    cuentas, transacciones = ejecutar_fase4(cuentas, atm, digitales)
    cuentas, transacciones = ejecutar_fase5(cuentas, transacciones)
    cuentas, validos, cuarentena = ejecutar_fase6(cuentas, transacciones)
    ejecutar_fase7(cuentas, validos, cuarentena)