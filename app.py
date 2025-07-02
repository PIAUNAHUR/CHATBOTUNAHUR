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

def find_faq_response(df, intent, params):
    if df.empty:
        return None

    filtered_df = df[df['intencion'] == intent]

    for entity_name, entity_value in params.items():
        if entity_name in filtered_df.columns and entity_value:
            # Si es lista, usá el primer valor
            if isinstance(entity_value, list):
                entity_value = entity_value[0]

            filtered_df = filtered_df.dropna(subset=[entity_name])
            filtered_df = filtered_df[filtered_df[entity_name] == entity_value]

    if not filtered_df.empty:
        return filtered_df.iloc[0]['respuesta']
    return None

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        req = request.get_json(force=True)
        print(f"REQ JSON: {json.dumps(req, indent=2)}")  # Diagnóstico útil

        intent = req.get('queryResult', {}).get('intent', {}).get('displayName')
        entities = req.get('queryResult', {}).get('parameters', {})

        if not intent:
            raise ValueError("No se encontró la intención en la solicitud.")

        print(f"Intent detectado: {intent}")
        print(f"Entidades recibidas: {entities}")

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

    
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)


