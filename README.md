# CHATBOTUNAHUR
# 🤖 Chatbot UNAHUR

Este proyecto implementa un chatbot basado en Dialogflow, que responde preguntas frecuentes conectándose en tiempo real
a una hoja de cálculo de Google Sheets a través de un backend desarrollado en Python (Flask) y desplegado con Render.

---

## 🧠 ¿Qué hace este bot?

- Responde a intenciones como `consultar_inscripcion`.
- Busca las respuestas dinámicamente desde una hoja de cálculo.
- Utiliza un webhook en Flask que conecta Dialogflow con Google Sheets.
- El contenido de la hoja se puede actualizar sin modificar el código.

---

## 🛠 Tecnologías utilizadas

- [Dialogflow ES](https://dialogflow.cloud.google.com/)
- [Python 3](https://www.python.org/)
- [Flask](https://flask.palletsprojects.com/)
- [gspread](https://github.com/burnash/gspread)
- [Render.com](https://render.com/) para despliegue
- Google Sheets como base de conocimiento

---

## 📁 Estructura de la hoja de cálculo

La hoja debe tener este formato:

| Intencion             | Respuesta                                               |
|----------------------|----------------------------------------------------------|
| consultar_inscripcion | La inscripción es del 1 al 15 de agosto. Más info aquí. |

- El nombre del documento debe coincidir con el configurado en el código (`BaseChatbot` por defecto).
- Debe estar compartido con la cuenta de servicio.

---

## 🚀 Despliegue

1. Crear una cuenta en [Render](https://render.com).
2. Subir este repo a GitHub.
3. Crear un nuevo Web Service en Render desde este repositorio.
4. Configurar la variable de entorno:
   - `GOOGLE_CREDENTIALS`: contenido del archivo `credenciales.json`
5. Usar el siguiente comando de inicio:
   ```bash
   gunicorn app:app


6. Activar el fulfillment en Dialogflow con la URL:

https://<tu-app>.onrender.com/webhook  


📡 Cómo usar desde Dialogflow
Crear una intención en Dialogflow, por ejemplo consultar_inscripcion.

Activar el webhook para esa intención.

Enviar frases como:
¿Cuándo es la inscripción?
