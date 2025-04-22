# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import re
import uuid
import pandas as pd

def filter_names(file_names):
    return [file_name for file_name in file_names if file_name and len(file_name) >= 3 and file_name[:3].isupper()]


def extract_data_to_json(file, mapping):
    survey_content = {}
    for key, (row, col) in mapping.items():
        try:
            value = file.iloc[row-1, col-1]
            if pd.isna(value):
                survey_content[key] = None
            elif isinstance(value, pd.Timestamp):
                survey_content[key] = value  # Keep timestamp objects for datetime fields
            else:
                survey_content[key] = str(value) if isinstance(value, (float, int)) else value
        except IndexError:
            survey_content[key] = None
    return survey_content


def parse_date(date_str):
    if not date_str:
        return None

    if isinstance(date_str, pd.Timestamp):
        return date_str
    cleaned_str = re.sub(r'^[^\d]+', '', date_str)
    
    # Find all sequences of digits
    number_strings = re.findall(r'\d+', cleaned_str)
    dates_numbers_string = " ".join(number_strings)
    for fmt in ["%m %Y", "%d %m %Y", "%Y %m %d"]:
        try:
            dt = datetime.strptime(dates_numbers_string, fmt)
            return dt.strftime("%m.%Y")
        except ValueError:
            continue
    return None


def safe_int(value):
    """Convert value to int safely, return 0 if not possible"""
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def create_portfolio_project(data, file_name):
    """Create a PortfolioProject instance from extracted data"""
    project_id = str(uuid.uuid4())
    
    return PortfolioProject(
        project_id=project_id,
        navn=data.get("Navn på tiltak", "Unnamed"),
        oppstart=parse_date(data.get("Oppstart/Planlagt oppstart")),
        avdeling=data.get("Avdeling", ""),
        kontaktpersoner=data.get("Kontaktperson", "")
    )


def create_ressursbehov(data, project_id):
    """Create a Ressursbehov instance from extracted data"""
    return Ressursbehov(
        ressursbehov_id=str(uuid.uuid4()),
        tidspunkt=datetime.now(),  # Current time as default
        antall_mandsverk_intern=safe_int(data.get("Estimert antall månedsverk intern")),
        antall_mandsverk_ekstern=safe_int(data.get("Estimert antall månedsverk ekstern")),
        antall_mandsverk_ekstern_betalt=safe_int(data.get("Estimerte antall månedsverk eksterne som betales 2025")),
        risiko_av_estimat=data.get("Hvor sikkert er estimatene", ""),
        kompetanse_som_trengs=data.get("Hvilke kompetanser trenges for tiltaket?", ""),
        kompetanse_tilgjengelig=data.get("Er kompetanser tilgjengelige internt?", ""),
        prosjekt_id=project_id
    )


def create_problemstilling(data, project_id):
    """Create a Problemstilling instance from extracted data"""
    return Problemstilling(
        problem_stilling_id=str(uuid.uuid4()),
        problem_stilling=data.get("Problemstilling", ""),
        prosjekt_id=project_id
    )


def create_tiltak(data, project_id):
    """Create a Tiltak instance from extracted data"""
    return Tiltak(
        tiltak_id=str(uuid.uuid4()),
        tiltak_beskrivelse=data.get("Beskrivelse av tiltaket", ""),
        prosjekt_id=project_id
    )


def create_risikovurdering(data, project_id):
    """Create a Risikovurdering instance from extracted data"""
    return Risikovurdering(
        risiko_vurdering_id=str(uuid.uuid4()),
        vurdering=data.get("Risiko", ""),
        prosjekt_id=project_id
    )


def create_fremskritt(data, project_id):
    """Create a Fremskritt instance from extracted data"""
    return Fremskritt(
        fremskritt_id=str(uuid.uuid4()),
        tidspunkt=datetime.now(),  # Current time as default
        fase=data.get("Hvilken fase er tiltaket i", ""),
        planlagt_ferdig=parse_date(data.get("Planlagt ferdig")),
        er_gjeldende = True,
        prosjekt_id=project_id
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
