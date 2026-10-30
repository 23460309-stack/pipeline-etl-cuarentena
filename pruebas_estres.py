"""
PRUEBAS DE ESTRÉS - PIPELINE ETL CON ZONA DE CUARENTENA
Ingeniería de Datos (ATD-2602) - Grupo 2 (LCNT)
Rol: Ingeniero/a 3 (Validación, Resiliencia y BD)

Inyecta datos corruptos a propósito para comprobar que el pipeline:
  1) NO se cae (no hay excepciones sin controlar),
  2) manda cada registro corrupto a cuarentena con el motivo correcto,
  3) no rechaza de más (los registros buenos siguen pasando),
  4) protege la base de datos con transacciones (commit / rollback).

Uso:  python pruebas_estres.py      (debe estar en la misma carpeta que pipeline_etl.py)
"""

import contextlib
import io
import re
import sqlite3

import pandas as pd

from pipeline_etl import (REPORTES_DIR, cargar_lote, contar, crear_esquema,
                          ejecutar_fase1, ejecutar_fase2, ejecutar_fase3,
                          ejecutar_fase4, ejecutar_fase5, ejecutar_fase6,
                          limpiar_registro, validar_lote)


def obtener_registros_validos():
    """Corre las Fases 1-6 en silencio y devuelve los registros válidos reales."""
    with contextlib.redirect_stdout(io.StringIO()):
        cuentas, atm, digitales = ejecutar_fase1()
        cuentas, atm, digitales = ejecutar_fase2(cuentas, atm, digitales)
        cuentas, atm, digitales = ejecutar_fase3(cuentas, atm, digitales)
        cuentas, transacciones = ejecutar_fase4(cuentas, atm, digitales)
        cuentas, transacciones = ejecutar_fase5(cuentas, transacciones)
        cuentas, validos, cuarentena = ejecutar_fase6(cuentas, transacciones)
    return validos


def tomar_base(validos, canal, tipo=None, moneda="MXN"):
    """Toma un registro válido real como punto de partida para corromperlo."""
    filtro = (validos["canal"] == canal) & (validos["moneda_original"] == moneda)
    if tipo:
        filtro &= validos["tipo_operacion"] == tipo
    return limpiar_registro(validos[filtro].iloc[0].to_dict())


def construir_casos(base_atm, base_dig):
    """(nombre, base, cambios, esperado). esperado = 'VALIDO' o tupla de códigos de error."""
    vacio_atm = {k: None for k in base_atm}
    return [
        ("Monto negativo en retiro", base_atm,
         {"monto_mxn": -500.0, "monto_original": -500.0}, ("VALOR_INVALIDO",)),
        ("Monto cero en un retiro", base_atm,
         {"monto_mxn": 0.0, "monto_original": 0.0, "monto_mxn_norm": 0.0}, ("MONTO_INVALIDO",)),
        ("Monto no numérico ('abc')", base_atm, {"monto_mxn": "abc"}, ("VALOR_INVALIDO",)),
        ("Monto nulo", base_atm, {"monto_mxn": None}, ("VALOR_INVALIDO",)),
        ("CLABE de 5 dígitos", base_dig, {"clabe_beneficiario": "12345"}, ("CLABE_INVALIDA",)),
        ("CLABE con letras", base_dig,
         {"clabe_beneficiario": "01221010061402384A"}, ("CLABE_INVALIDA",)),
        ("Correo sin arroba", base_dig,
         {"email_notificacion": "sinarroba.com"}, ("EMAIL_MALFORMADO",)),
        ("Correo con doble arroba", base_dig,
         {"email_notificacion": "usuario@@banco.com"}, ("EMAIL_MALFORMADO",)),
        ("Cuenta huérfana", base_atm,
         {"id_cuenta": "CTA-9999", "cuenta_huerfana": True}, ("CUENTA_HUERFANA",)),
        ("Formato de cuenta roto", base_atm, {"id_cuenta": "CUENTA-1"}, ("CUENTA_FORMATO_INVALIDO",)),
        ("Fecha imposible (mes 13)", base_atm, {"fecha": "2026-13-45"}, ("VALOR_INVALIDO",)),
        ("Fecha nula", base_atm, {"fecha": None}, ("VALOR_INVALIDO",)),
        ("Hora inválida", base_atm, {"hora": "25h"}, ("HORA_INVALIDA",)),
        ("Estado desconocido", base_atm, {"estado": "XYZ"}, ("VALOR_INVALIDO",)),
        ("Moneda no soportada (EUR)", base_dig, {"moneda_original": "EUR"}, ("VALOR_INVALIDO",)),
        ("Conversión de moneda inconsistente", base_dig,
         {"moneda_original": "USD"}, ("CONVERSION_INCONSISTENTE",)),
        ("ATM sin id_cajero", base_atm, {"id_cajero": None}, ("ATM_SIN_CAJERO",)),
        ("Transferencia sin CLABE", base_dig,
         {"clabe_beneficiario": None}, ("DIGITAL_INCOMPLETO",)),
        ("ID de transacción con formato raro", base_atm,
         {"id_transaccion": "XYZ-1"}, ("ID_INVALIDO",)),
        ("ID de transacción nulo", base_atm, {"id_transaccion": None}, ("VALOR_INVALIDO",)),
        ("Segmento inexistente", base_atm, {"segmento_monto": "Gigante"}, ("VALOR_INVALIDO",)),
        ("Monto normalizado fuera de [0,1]", base_atm, {"monto_mxn_norm": 1.5}, ("VALOR_INVALIDO",)),
        ("Varios fallos a la vez", base_dig,
         {"clabe_beneficiario": "1", "email_notificacion": "x",
          "id_cuenta": "CTA-9999", "cuenta_huerfana": True},
         ("CLABE_INVALIDA", "EMAIL_MALFORMADO", "CUENTA_HUERFANA")),
        ("Registro completamente vacío", base_atm, vacio_atm, ("VALOR_INVALIDO",)),
        # --- Casos que deben seguir siendo VÁLIDOS (evitan rechazar de más) ---
        ("Concepto con acentos y símbolos (válido)", base_dig,
         {"concepto": "Pago mañana – ñandú ✓"}, "VALIDO"),
        ("Cuenta con espacios alrededor (válido)", base_atm, {"id_cuenta": " CTA-0001 "}, "VALIDO"),
        ("Correo 'ANONIMO' (válido)", base_dig, {"email_notificacion": "ANONIMO"}, "VALIDO"),
    ]


def pruebas_de_validacion(validos):
    base_atm = tomar_base(validos, "ATM", tipo="RETIRO")
    base_dig = tomar_base(validos, "DIGITAL", moneda="MXN")
    casos = construir_casos(base_atm, base_dig)

    resultados = []
    registros_mixtos = []
    for nombre, base, cambios, esperado in casos:
        registro = {**base, **cambios}
        registros_mixtos.append(registro)
        try:
            v, c = validar_lote(pd.DataFrame([registro]))
            if esperado == "VALIDO":
                ok = len(v) == 1 and len(c) == 0
                obtenido = "VALIDO" if len(v) == 1 else "CUARENTENA"
            else:
                motivo = c["motivo_rechazo_cuarentena"].iloc[0] if len(c) > 0 else ""
                ok = len(c) == 1 and all(codigo in motivo for codigo in esperado)
                obtenido = " + ".join(re.findall(r"(?:^|\| )([A-Z_]+):", motivo)) or "VALIDO"
        except Exception as error:   # un crash aquí significa que el pipeline NO es robusto
            ok, obtenido = False, f"CRASH: {type(error).__name__}"
        resultados.append({
            "prueba": nombre,
            "esperado": esperado if esperado == "VALIDO" else " + ".join(esperado),
            "obtenido": obtenido,
            "resultado": "OK" if ok else "FALLO",
        })

    # Lote mixto: los registros corruptos NO deben detener a los buenos
    buenos = validos.head(20).to_dict("records")
    n_validos_casos = sum(1 for caso in casos if caso[3] == "VALIDO")
    try:
        v, c = validar_lote(pd.DataFrame(registros_mixtos + buenos))
        esperado_v = 20 + n_validos_casos
        esperado_c = len(casos) - n_validos_casos
        ok = (len(v) == esperado_v and len(c) == esperado_c)
        obtenido = f"{len(v)} válidos / {len(c)} cuarentena"
    except Exception as error:
        ok, obtenido = False, f"CRASH: {type(error).__name__}"
        esperado_v, esperado_c = 20 + n_validos_casos, len(casos) - n_validos_casos
    resultados.append({
        "prueba": "Lote mixto (corruptos + 20 buenos) sin detenerse",
        "esperado": f"{esperado_v} válidos / {esperado_c} cuarentena",
        "obtenido": obtenido,
        "resultado": "OK" if ok else "FALLO",
    })
    return resultados


def pruebas_de_base_de_datos(validos):
    """Se ejecutan sobre una BD en memoria: la base real no se toca."""
    resultados = []
    conn = sqlite3.connect(":memory:")
    crear_esquema(conn)
    vacia = pd.DataFrame()

    # 1) Carga normal
    exito, _ = cargar_lote(conn, validos.head(5), vacia)
    n = contar(conn, "transacciones_limpias")
    resultados.append({"prueba": "BD: carga normal de 5 registros", "esperado": "COMMIT, 5 filas",
                       "obtenido": f"{'COMMIT' if exito else 'ROLLBACK'}, {n} filas",
                       "resultado": "OK" if (exito and n == 5) else "FALLO"})

    # 2) Llave primaria duplicada a mitad de la carga -> rollback
    duplicado = pd.concat([validos.head(3), validos.head(3)], ignore_index=True)
    exito, mensaje = cargar_lote(conn, duplicado, vacia)
    n = contar(conn, "transacciones_limpias")
    resultados.append({"prueba": "BD: llave duplicada en medio de la carga",
                       "esperado": "ROLLBACK, 5 filas intactas",
                       "obtenido": f"{'COMMIT' if exito else 'ROLLBACK'}, {n} filas",
                       "resultado": "OK" if (not exito and n == 5) else "FALLO"})

    # 3) Dato corrupto que se cuela directo a la BD (salta Pydantic) -> CHECK + rollback
    corrupto = validos.head(3).copy()
    corrupto.loc[corrupto.index[1], "monto_mxn"] = -1.0
    exito, mensaje = cargar_lote(conn, corrupto, vacia)
    n = contar(conn, "transacciones_limpias")
    resultados.append({"prueba": "BD: monto negativo viola restricción CHECK",
                       "esperado": "ROLLBACK, 5 filas intactas",
                       "obtenido": f"{'COMMIT' if exito else 'ROLLBACK'}, {n} filas",
                       "resultado": "OK" if (not exito and n == 5) else "FALLO"})

    # 4) Recargar el mismo lote no duplica datos (idempotencia)
    exito, _ = cargar_lote(conn, validos.head(5), vacia)
    n = contar(conn, "transacciones_limpias")
    resultados.append({"prueba": "BD: recarga del mismo lote (idempotencia)",
                       "esperado": "COMMIT, 5 filas (sin duplicar)",
                       "obtenido": f"{'COMMIT' if exito else 'ROLLBACK'}, {n} filas",
                       "resultado": "OK" if (exito and n == 5) else "FALLO"})
    conn.close()
    return resultados


def ejecutar_pruebas_estres(validos):
    print("=" * 70)
    print("PRUEBAS DE ESTRÉS: INYECCIÓN DE DATOS CORRUPTOS")
    print("=" * 70)

    resultados = pruebas_de_validacion(validos) + pruebas_de_base_de_datos(validos)
    tabla = pd.DataFrame(resultados)
    print(tabla.to_string(index=False, max_colwidth=48))

    superadas = int((tabla["resultado"] == "OK").sum())
    print(f"\nPruebas superadas: {superadas} de {len(tabla)}")
    if superadas == len(tabla):
        print("El pipeline soporta datos corruptos sin excepciones no controladas.")
    tabla.to_csv(REPORTES_DIR / "pruebas_estres.csv", index=False, encoding="utf-8-sig")
    print("Reporte guardado: reportes/pruebas_estres.csv")


if __name__ == "__main__":
    ejecutar_pruebas_estres(obtener_registros_validos())