from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import pandas as pd

app = Flask(__name__)

IGNORE_ENTITIES = ['no-input', 'no-match']

def load_faqs_from_sheet():
    print("INFO: Intentando conectar con Google Sheets y cargar FAQs...")
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        cred_json_str = os.environ.get('GOOGLE_CREDENTIALS')
        if not cred_json_str:
            print("ERROR CRÍTICO: GOOGLE_CREDENTIALS no está definido.")
            return pd.DataFrame()

        cred_dict = json.loads(cred_json_str)
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scope)
        client = gspread.authorize(credentials)

        spreadsheet = client.open("FAQs_intent_entidades")
        sheet = spreadsheet.sheet1
        records = sheet.get_all_records()
        df = pd.DataFrame(records)
        df.replace('', pd.NA, inplace=True)
        print(f"INFO: Se cargaron {len(df)} filas de FAQs exitosamente.")
        return df
    except Exception as e:
        print(f"ERROR al cargar Google Sheets: {e}")
        return pd.DataFrame()

faqs_df = load_faqs_from_sheet()

def extract_entities(req):
    entities = {}
    # Obtener parámetros principales
    parameters = req.get('queryResult', {}).get('parameters', {}).copy()
    
    # Procesar contextos
    contexts = req.get('queryResult', {}).get('outputContexts', [])
    for ctx in contexts:
        ctx_params = ctx.get('parameters', {})
        for key, val in ctx_params.items():
            # Ignorar sufijos .original
            if '.original' in key:
                continue
            # Tomar solo el primer valor si es lista
            if isinstance(val, list) and val:
                entities[key] = val[0]
            else:
                entities[key] = val
    
    # Combinar con parámetros principales
    for key, val in parameters.items():
        if '.original' in key:
            continue
        if isinstance(val, list) and val:
            entities[key] = val[0]
        else:
            entities[key] = val
    
    return {k: v for k, v in entities.items() if k not in IGNORE_ENTITIES}

def find_faq_response(df, intent, params):
    if df.empty:
        return None

    print(f"🔍 Intent inicial: {intent}")
    print(f"🔍 Pregunta con entidades: {params}")

    filtered_df = df[df['intencion'] == intent].copy()
    
    for entity_name, entity_value in params.items():
        if entity_name not in filtered_df.columns:
            continue
        
        # Normalizar valor de entidad
        if isinstance(entity_value, list):
            entity_value = entity_value[0] if entity_value else None
        entity_value_str = str(entity_value).lower().strip()
        
        # Manejar múltiples valores en celda (ej: "virtual;presencial")
        mask = filtered_df[entity_name].apply(
            lambda x: any(
                val.strip().lower() == entity_value_str 
                for val in str(x).split(';')
            ) if pd.notna(x) else False
        )
        filtered_df = filtered_df[mask]
    
    print("📋 Valores en la base para este intent:")
    print(filtered_df.head(3).to_dict(orient="records"))
    print(f"🔎 Coincidencias encontradas: {len(filtered_df)}")
    print(f"DataFrame filtrado por intent '{intent}': {len(filtered_df)} filas")
    print("Columnas disponibles:", filtered_df.columns.tolist())

    if not filtered_df.empty:
        return filtered_df.iloc[0]['respuesta']

    return None



@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        req = request.get_json(force=True)
        print(f"REQ JSON: {json.dumps(req, indent=2, ensure_ascii=False)}")

        intent = req.get('queryResult', {}).get('intent', {}).get('displayName')
        if not intent:
            raise ValueError("No se encontró la intención en la solicitud.")

        entities = extract_entities(req)

        print(f"Intent detectado: {intent}")
        print(f"Entidades procesadas: {entities}")

        respuesta = find_faq_response(faqs_df, intent, entities)

        if not respuesta:
            respuesta = "Lo siento, no encontré una respuesta para esa consulta específica."

        return jsonify({'fulfillmentText': respuesta})

    except Exception as e:
        print(f"ERROR en webhook: {e}")
        return jsonify({'fulfillmentText': 'Ocurrió un error procesando tu solicitud. Por favor, intenta de nuevo.'}), 200

if __name__ == '__main__':
    if faqs_df.empty:
        print("ADVERTENCIA: El DataFrame de FAQs está vacío.")

    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
