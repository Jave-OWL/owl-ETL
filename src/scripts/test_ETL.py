import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
import pandas as pd
import re


def extract_bank_fund_name(filename: str) -> Optional[Tuple[str, str]]:
    """
    Extrae el nombre del banco y fondo del archivo.
    Formato esperado: 'NombreBanco_NombreFondo_raw_transformed[_PRUEBA].json'
    """
    # Patrón para extraer banco y fondo
    pattern = r'^(.+?)_(.+?)_raw_transformed(?:_PRUEBA)?\.json$'
    match = re.match(pattern, filename)

    if match:
        return match.group(1), match.group(2)  # (banco, fondo)
    return None


def load_json_file(file_path: str) -> Dict[str, Any]:
    """Carga un archivo JSON y retorna el diccionario."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error cargando {file_path}: {e}")
        return {}


def count_total_fields(data: Dict[str, Any]) -> int:
    """Cuenta el número total de campos en el JSON (hojas del árbol)."""

    def count_fields(obj, path=""):
        count = 0
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{path}.{key}" if path else key
                count += 1 + count_fields(value, new_path)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                new_path = f"{path}[{i}]"
                count += count_fields(item, new_path)
        return count

    return count_fields(data)


def compare_values(val1: Any, val2: Any, path: str = "") -> List[Tuple[str, Any, Any]]:
    """
    Compara dos valores recursivamente y retorna las diferencias.
    """
    differences = []

    if type(val1) != type(val2):
        differences.append((path, f"{type(val1).__name__}: {val1}", f"{type(val2).__name__}: {val2}"))

    elif isinstance(val1, dict):
        # Comparar claves de diccionarios
        all_keys = set(val1.keys()) | set(val2.keys())
        for key in all_keys:
            new_path = f"{path}.{key}" if path else key
            if key in val1 and key in val2:
                differences.extend(compare_values(val1[key], val2[key], new_path))
            elif key in val1:
                differences.append((new_path, val1[key], "MISSING"))
            else:
                differences.append((new_path, "MISSING", val2[key]))

    elif isinstance(val1, list):
        # Para listas, comparamos elemento por elemento
        if len(val1) != len(val2):
            differences.append((path, f"Lista con {len(val1)} elementos", f"Lista con {len(val2)} elementos"))
        else:
            for i, (item1, item2) in enumerate(zip(val1, val2)):
                new_path = f"{path}[{i}]"
                differences.extend(compare_values(item1, item2, new_path))

    else:
        # Comparación directa para tipos primitivos
        if val1 != val2:
            # Para floats, comparamos con tolerancia
            if isinstance(val1, float) and isinstance(val2, float):
                if abs(val1 - val2) > 1e-10:  # Tolerancia para floats
                    differences.append((path, val1, val2))
            else:
                differences.append((path, val1, val2))

    return differences


def compare_list_of_dicts(list1: List[Dict], list2: List[Dict], path: str, key_field: str = None) -> List[
    Tuple[str, Any, Any]]:
    """
    Compara dos listas de diccionarios, intentando emparejarlos por una clave común.
    """
    differences = []

    if len(list1) != len(list2):
        differences.append((path, f"Lista con {len(list1)} elementos", f"Lista con {len(list2)} elementos"))

    # Si tenemos una clave para emparejar
    if key_field and list1 and list2 and key_field in list1[0] and key_field in list2[0]:
        dict1 = {item[key_field]: item for item in list1}
        dict2 = {item[key_field]: item for item in list2}

        all_keys = set(dict1.keys()) | set(dict2.keys())
        for key in all_keys:
            new_path = f"{path}[{key}]"
            if key in dict1 and key in dict2:
                differences.extend(compare_values(dict1[key], dict2[key], new_path))
            elif key in dict1:
                differences.append((new_path, dict1[key], "MISSING"))
            else:
                differences.append((new_path, "MISSING", dict2[key]))
    else:
        # Comparación secuencial
        for i, (item1, item2) in enumerate(zip(list1, list2)):
            new_path = f"{path}[{i}]"
            differences.extend(compare_values(item1, item2, new_path))

    return differences


def compare_json_files(file1: Dict[str, Any], file2: Dict[str, Any]) -> Tuple[List[Tuple[str, Any, Any]], int]:
    """
    Compara dos archivos JSON estructurados según el formato especificado.
    Retorna: (diferencias, total_campos)
    """
    differences = []
    total_fields = count_total_fields(file1)  # Usamos file1 como referencia

    # Comparar estructura principal
    main_sections = ['fic', 'plazo_duracion', 'composicion_portafolio',
                     'caracteristicas', 'calificacion', 'principales_inversiones',
                     'rentabilidad_volatilidad']

    for section in main_sections:
        if section in file1 and section in file2:
            if section == 'plazo_duracion':
                differences.extend(compare_list_of_dicts(file1[section], file2[section], section, 'plazo'))
            elif section == 'principales_inversiones':
                differences.extend(compare_list_of_dicts(file1[section], file2[section], section, 'emisor'))
            elif section == 'rentabilidad_volatilidad':
                differences.extend(
                    compare_list_of_dicts(file1[section], file2[section], section, 'tipo_de_participacion'))
            elif section == 'composicion_portafolio':
                # Subsecciones de composicion_portafolio
                for subsection in file1[section]:
                    if subsection in file2[section]:
                        sub_path = f"{section}.{subsection}"
                        if subsection in ['por_activo', 'por_tipo_de_renta', 'por_sector_economico',
                                          'por_pais_emisor', 'por_moneda', 'por_calificacion']:
                            key_field = 'activo' if subsection == 'por_activo' else \
                                'tipo' if subsection == 'por_tipo_de_renta' else \
                                    'sector' if subsection == 'por_sector_economico' else \
                                        'pais' if subsection == 'por_pais_emisor' else \
                                            'moneda' if subsection == 'por_moneda' else \
                                                'calificacion'
                            differences.extend(compare_list_of_dicts(
                                file1[section][subsection],
                                file2[section][subsection],
                                sub_path,
                                key_field
                            ))
                    else:
                        differences.append((f"{section}.{subsection}", file1[section][subsection], "MISSING"))
            else:
                differences.extend(compare_values(file1[section], file2[section], section))
        elif section in file1:
            differences.append((section, file1[section], "MISSING"))
        elif section in file2:
            differences.append((section, "MISSING", file2[section]))

    return differences, total_fields


def calculate_reliability(total_fields: int, differences_count: int) -> float:
    """Calcula el porcentaje de confiabilidad de los datos."""
    if total_fields == 0:
        return 0.0
    return max(0.0, (1 - differences_count / total_fields)) * 100


def process_comparison_folder(folder_path: str) -> pd.DataFrame:
    """
    Procesa todos los pares de archivos en la carpeta y retorna un DataFrame con las diferencias.
    """
    folder = Path(folder_path)
    all_differences = []
    reliability_summary = []

    # Agrupar archivos por banco y fondo
    file_groups = {}

    # Primero, agrupar todos los archivos por (banco, fondo)
    for json_file in folder.glob("*.json"):
        name_info = extract_bank_fund_name(json_file.name)
        if name_info:
            bank, fund = name_info
            key = (bank, fund)
            if key not in file_groups:
                file_groups[key] = {'etl': None, 'prueba': None}

            if '_PRUEBA.json' in json_file.name:
                file_groups[key]['prueba'] = json_file
            else:
                file_groups[key]['etl'] = json_file

    # Procesar cada grupo
    for (bank, fund), files in file_groups.items():
        etl_file = files['etl']
        prueba_file = files['prueba']

        if etl_file and prueba_file:
            print(f"Comparando: {bank}_{fund}")
            print(f"  ETL:    {etl_file.name}")
            print(f"  PRUEBA: {prueba_file.name}")

            # Cargar archivos
            etl_data = load_json_file(str(etl_file))
            prueba_data = load_json_file(str(prueba_file))

            if etl_data and prueba_data:
                # Comparar
                differences, total_fields = compare_json_files(etl_data, prueba_data)
                reliability = calculate_reliability(total_fields, len(differences))

                # Agregar a resultados
                for diff_path, val1, val2 in differences:
                    all_differences.append({
                        'banco': bank,
                        'fondo': fund,
                        'archivo_etl': etl_file.name,
                        'archivo_prueba': prueba_file.name,
                        'campo': diff_path,
                        'valor_etl': val1,
                        'valor_prueba': val2
                    })

                # Agregar a resumen de confiabilidad
                reliability_summary.append({
                    'banco': bank,
                    'fondo': fund,
                    'archivo_etl': etl_file.name,
                    'archivo_prueba': prueba_file.name,
                    'total_campos': total_fields,
                    'diferencias': len(differences),
                    'confiabilidad': reliability
                })

                print(
                    f"Comparación completada: {len(differences)} diferencias, {reliability:.2f}% de confiabilidad")
            else:
                print(f"Error: No se pudieron cargar ambos archivos")
        else:
            missing_files = []
            if not etl_file:
                missing_files.append(f"ETL para {bank}_{fund}")
            if not prueba_file:
                missing_files.append(f"PRUEBA para {bank}_{fund}")
            print(f"Archivos faltantes: {', '.join(missing_files)}")

    return pd.DataFrame(all_differences), pd.DataFrame(reliability_summary)


def main():
    folder_path = "data/pruebas_09_2025"

    if not os.path.exists(folder_path):
        print(f"Error: La carpeta {folder_path} no existe")
        return

    print("Iniciando comparación de archivos...")
    print("=" * 60)

    df_differences, df_reliability = process_comparison_folder(folder_path)

    if df_differences.empty:
        print("\nNo se encontraron diferencias entre los archivos")
    else:
        print(f"\nRESUMEN GENERAL:")
        print(f"Total de archivos comparados: {len(df_reliability)}")
        print(f"Total de diferencias encontradas: {len(df_differences)}")

        # Métricas generales
        avg_reliability = df_reliability['confiabilidad'].mean()
        min_reliability = df_reliability['confiabilidad'].min()
        max_reliability = df_reliability['confiabilidad'].max()

        print(f"\nMÉTRICAS DE CONFIABILIDAD:")
        print(f"Confiabilidad promedio: {avg_reliability:.2f}%")
        print(f"Confiabilidad mínima: {min_reliability:.2f}%")
        print(f"Confiabilidad máxima: {max_reliability:.2f}%")

        # Mostrar resumen por archivo
        print(f"\n📋 RESUMEN POR ARCHIVO:")
        for _, row in df_reliability.sort_values('confiabilidad').iterrows():
            status = "✅" if row['confiabilidad'] >= 95 else "⚠️" if row['confiabilidad'] >= 80 else "❌"
            print(f"  {status} {row['banco']}_{row['fondo']}: {row['confiabilidad']:.2f}% "
                  f"({row['diferencias']} difs de {row['total_campos']} campos)")

        # Guardar resultados en Excel
        output_file = "comparacion_resultados.xlsx"
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df_differences.to_excel(writer, sheet_name='Diferencias_Detalladas', index=False)
            df_reliability.to_excel(writer, sheet_name='Confiabilidad', index=False)

            # Crear resumen por campo
            field_summary = df_differences.groupby('campo').size().reset_index(name='ocurrencias')
            field_summary = field_summary.sort_values('ocurrencias', ascending=False)
            field_summary.to_excel(writer, sheet_name='Resumen_Campos', index=False)

            # Resumen por banco
            bank_summary = df_reliability.groupby('banco').agg({
                'confiabilidad': 'mean',
                'diferencias': 'sum',
                'fondo': 'count'
            }).round(2).reset_index()
            bank_summary.columns = ['Banco', 'Confiabilidad_Promedio', 'Total_Diferencias', 'Cantidad_Fondos']
            bank_summary.to_excel(writer, sheet_name='Resumen_Bancos', index=False)

        print(f"\nResultados guardados en: {output_file}")

        # Mostrar algunas diferencias de ejemplo
        print(f"\nPRIMERAS 5 DIFERENCIAS ENCONTRADAS:")
        for i, (_, row) in enumerate(df_differences.head().iterrows()):
            print(f"  {i + 1}. [{row['banco']}_{row['fondo']}] {row['campo']}")
            print(f"     ETL:    {row['valor_etl']}")
            print(f"     PRUEBA: {row['valor_prueba']}")

        # Recomendaciones basadas en la confiabilidad
        print(f"\nRECOMENDACIONES:")
        if avg_reliability >= 95:
            print("Excelente calidad de datos - El ETL funciona muy bien")
        elif avg_reliability >= 80:
            print("Buena calidad - Revisar campos problemáticos específicos")
        elif avg_reliability >= 60:
            print("Calidad regular - Se necesitan mejoras significativas en el ETL")
        else:
            print("Calidad pobre - Revisión completa del ETL requerida")


if __name__ == "__main__":
    main()