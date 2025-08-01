from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import pandas as pd

app = Flask(__name__)

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

        spreadsheet = client.open("FAQs_intent_entidades2")
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

def find_faq_response(df, intent, params, use_entities=True):
    if df.empty:
        return None

    filtered_df = df[df['intencion'] == intent]

    if use_entities and params:
        for entity_name, entity_value in params.items():
            # Tomar solo el primer valor si es lista
            if isinstance(entity_value, list) and entity_value:
                entity_value = entity_value[0]

            if entity_name in filtered_df.columns and pd.notna(entity_value):
                filtered_df = filtered_df.dropna(subset=[entity_name])
                filtered_df = filtered_df[filtered_df[entity_name] == entity_value]

    if not filtered_df.empty:
        return filtered_df.iloc[0]['respuesta']
    return None

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        req = request.get_json(force=True)
        print(f"REQ JSON: {json.dumps(req, indent=2)}")

        intent = req.get('queryResult', {}).get('intent', {}).get('displayName')
        if not intent:
            raise ValueError("No se encontró la intención en la solicitud.")

        contexts = req.get('queryResult', {}).get('outputContexts', [])
        hay_contexto = len(contexts) > 0

        if hay_contexto:
            # Extraer parámetros de todos los contextos que tengan parámetros y unirlos
            entidades_contexto = {}
            for ctx in contexts:
                params = ctx.get('parameters', {})
                if params:
                    entidades_contexto.update(params)
            entities = entidades_contexto
        else:
            # Extraer parámetros directamente de la consulta
            entities = req.get('queryResult', {}).get('parameters', {}).copy()

        print(f"Intent detectado: {intent}")
        print(f"Contexto activo: {hay_contexto}")
        print(f"Entidades procesadas: {entities}")

        respuesta = find_faq_response(faqs_df, intent, entities, use_entities=True)

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
