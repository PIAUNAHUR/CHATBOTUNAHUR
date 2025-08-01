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

def extract_entities(req):
    # Extraer entidades de parámetros directos
    entities = req.get('queryResult', {}).get('parameters', {}).copy()

    # Remover entidades automáticas que no queremos usar
    for bad_entity in ['no-input', 'no-match']:
        if bad_entity in entities:
            del entities[bad_entity]

    # Extraer entidades desde contextos (si existen), para complementar
    contexts = req.get('queryResult', {}).get('outputContexts', [])
    for ctx in contexts:
        ctx_params = ctx.get('parameters', {})
        for key, val in ctx_params.items():
            # Agregar solo si no existe o está vacío
            if key not in entities or not entities[key]:
                entities[key] = val

    return entities

def find_faq_response(df, intent, params):
    if df.empty:
        return None

    filtered_df = df[df['intencion'] == intent]

    for entity_name, entity_value in params.items():
        if isinstance(entity_value, list) and entity_value:
            entity_value = entity_value[0]

        # Solo filtrar si la columna existe y el valor no es nulo o vacío
        if entity_name in filtered_df.columns and entity_value is not None and entity_value != '':
            # Filtrar filas que tienen el valor en la columna, o que tengan NaN (para no perder filas sin valor)
            filtered_df = filtered_df[
                (filtered_df[entity_name] == entity_value)
            ]
            # No hagas dropna porque elimina filas válidas
            # Esto filtra solo las filas que tengan ese valor exacto en la columna
            # Si querés, podrías hacer algo más flexible para filtrar también si la columna está vacía.

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
