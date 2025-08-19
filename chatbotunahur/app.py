from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import pandas as pd
import numpy as np
import re

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
    """
    Extrae solo las entidades de la consulta actual, ignorando
    los contextos para evitar contaminación entre intents.
    """
    # Inicia con los parámetros de la consulta actual
    params = req.get('queryResult', {}).get('parameters', {}).copy()
    
    entities = {}
    for key, val in params.items():
        # Nos aseguramos de que la entidad no esté en la lista de ignorados y tenga un valor
        if key not in IGNORE_ENTITIES and val:
            # Si el valor es una lista (como lo envía Dialogflow), tomamos el primer elemento
            if isinstance(val, list) and val:
                entities[key] = val[0]
            # Si no es una lista pero tiene valor, lo tomamos
            elif not isinstance(val, list):
                entities[key] = val
                
    return entities

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
        respuesta_filtrada = filtered_df.iloc[0]['respuesta']
        respuesta_normalizada = re.sub(r'\s*[\r\n]+\s*', '\n', respuesta_filtrada)
        respuesta_procesada = respuesta_normalizada.replace('\n', '\n\n')
        respuesta_final = respuesta_procesada.strip()
        return respuesta_con_salto
        
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
            respuesta = ["Lo siento, no encontré una respuesta para esa consulta específica.","Disculpame, puedes especificar el tema de tu consulta", "Disculpa, no he logrado entendente",
                        "Disculpe, no he encontrado una respuesta a esa consulta"]
            indice = np.random.randint(0,len(respuesta)-1)
                                       
        return jsonify({'fulfillmentText': respuesta[indice]})

    except Exception as e:
        print(f"ERROR en webhook: {e}")
        return jsonify({'fulfillmentText': 'Ocurrió un error procesando tu solicitud. Por favor, intenta de nuevo.'}), 200

if __name__ == '__main__':
    if faqs_df.empty:
        print("ADVERTENCIA: El DataFrame de FAQs está vacío.")

    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
