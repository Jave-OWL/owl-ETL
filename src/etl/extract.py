import time
import logging
from unstract.llmwhisperer import LLMWhispererClientV2
import google.generativeai as genai
from src.config.settings import LLMWHISPERER_API_KEY, GEMINI_API_KEY, LLMWHISPERER_BASE_URL

# crea (o recupera) un logger con el nombre del módulo actual.
logger = logging.getLogger(__name__)

# Configurar clientes
whisper_client = LLMWhispererClientV2(
    base_url=LLMWHISPERER_BASE_URL,
    api_key=LLMWHISPERER_API_KEY
)

# Configurar Gemini
genai.configure(api_key=GEMINI_API_KEY)

# Prompt base
BASE_PROMPT = """
Eres un asistente experto en extraer información financiera de Fondos de Inversión Colectiva (FICs) en Colombia. 
Recibirás como entrada texto plano extraído de un PDF que contiene la ficha técnica de un FIC. 
Debes devolver un JSON estructurado con la información encontrada, siguiendo este esquema:

{
  "fic"{
	"nombre_fic" : "",
	"gestor" : "",
	"custodio" : "",
	"fecha_corte" : null,
	"politica_de_inversion" : ""
  },

  "plazo_duracion": [
    {"plazo": "", "participacion": 0.0}
  ],
  
  "composicion_portafolio": {
    "por_activo": [
      {"activo": "", "participacion": 0.0}
    ],
    "por_tipo_de_renta": [
      {"tipo": "", "participacion": 0.0}
    ],
    "por_sector_economico": [
      {"sector": "", "participacion": 0.0}
    ],
    "por_pais_emisor": [
      {"pais": "", "participacion": 0.0}
    ],
    "por_moneda": [
      {"moneda": "", "participacion": 0.0}
    ],
    "por_calificacion": [
      {"calificacion": "", "participacion": 0.0}
    ]
  },
  
  "caracteristicas"{
	"tipo" : "",
	"valor" : 0.0,
	"fecha_inicio_operaciones" : null,
	"no_unidades_en_circulacion" : 0.0
  },
  
  "calificacion": {
    "calificacion": "",
    "fecha_ultima_calificacion": null,
    "entidad_calificadora": ""
  },
  
  "principales_inversiones": [
    {"emisor": "", "participacion": 0.0}
  ],

  "rentabilidad_volatilidad": [
	{
		"tipo_de_participacion": "",
		"rentabilidad_historica_ea": {
			"ultimo_mes": 0.0,
			"ultimos_6_meses": 0.0,
			"anio_corrido": 0.0,
			"ultimo_anio": 0.0,
			"ultimos_2_anios": 0.0,
			"ultimos_3_anios": 0.0
		},
		"volatilidad_historica": {
			"ultimo_mes": 0.0,
			"ultimos_6_meses": 0.0,
			"anio_corrido": 0.0,
			"ultimo_anio": 0.0,
			"ultimos_2_anios": 0.0,
			"ultimos_3_anios": 0.0
		}
	}
  ],

}



Reglas:
- Si no encuentras un dato, deja el valor como "", 0.0, null, segun corresponda.
- Mantén los números en el formato del json float en cada caso.
- No inventes información, solo usa lo que aparece en el texto.

Texto a procesar:
"""


def extract_text_from_pdf(pdf_path: str, max_retries: int = 3) -> str:
    """
    Extrae texto de un PDF usando LLMWhisperer

    Args:
        pdf_path: Ruta al archivo PDF
        max_retries: Número máximo de intentos

    Returns:
        Texto extraído del PDF
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"Extrayendo texto de {pdf_path} (intento {attempt + 1})")

            # Enviar PDF a LLMWhisperer
            result = whisper_client.whisper(file_path=pdf_path)

            # Esperar a que termine el procesamiento
            wait_time = 5
            max_wait = 300  # 5 minutos máximo
            elapsed = 0

            # Bucle para consultar el estado del procesamiento hasta que termine o se agote el tiempo
            while elapsed < max_wait:
                # consulta el estado
                status = whisper_client.whisper_status(whisper_hash=result['whisper_hash'])

                if status['status'] == 'processed':
                    resultx = whisper_client.whisper_retrieve(whisper_hash=result['whisper_hash'])
                    break
                elif status['status'] == 'error':
                    raise Exception(f"Error en LLMWhisperer: {status.get('error', 'Unknown error')}")

                time.sleep(wait_time)
                elapsed += wait_time

            if elapsed >= max_wait:
                raise TimeoutError("Timeout esperando procesamiento de LLMWhisperer")

            # Obtener el texto extraído
            extracted_text = resultx['extraction']['result_text']

            if not extracted_text.strip():
                raise ValueError("Texto extraído está vacío")

            logger.info(f"Texto extraído exitosamente de {pdf_path}")
            return extracted_text

        except Exception as e:
            logger.error(f"Error en intento {attempt + 1} para {pdf_path}: {str(e)}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # Backoff exponencial

    raise Exception(f"Todos los intentos fallaron para {pdf_path}")


def extract_json_from_text(extracted_text: str) -> str:
    """
    Convierte texto extraído a JSON estructurado usando Gemini

    Args:
        extracted_text: Texto extraído del PDF

    Returns:
        JSON estructurado con la información del FIC
    """
    try:
        logger.info("Procesando texto con Gemini para extraer JSON")

        # Construir el prompt para Gemini
        prompt = BASE_PROMPT + extracted_text

        # Mandar a Google Gemini y recolectar la respuesta
        response = genai.GenerativeModel('gemini-2.5-flash').generate_content(prompt)

        # Extraer solo el JSON de la respuesta
        json_output = response.text.strip()

        # Limpiar la respuesta para obtener solo el JSON
        if '```json' in json_output:
            json_output = json_output.split('```json')[1].split('```')[0].strip()
        elif '```' in json_output:
            json_output = json_output.split('```')[1].split('```')[0].strip()

        logger.info("JSON extraído exitosamente")
        return json_output

    except Exception as e:
        logger.error(f"Error procesando texto con Gemini: {str(e)}")
        raise