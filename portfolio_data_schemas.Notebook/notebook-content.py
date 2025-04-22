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

from dataclasses import dataclass
from datetime import datetime

@dataclass
class PortfolioProject:
    project_id: str
    navn: str
    oppstart: datetime
    avdeling: str
    kontaktpersoner: str

@dataclass
class Ressursbehov:
    ressursbehov_id: str
    tidspunkt: datetime
    antall_mandsverk_intern: int
    antall_mandsverk_ekstern: int
    antall_mandsverk_ekstern_betalt: int
    risiko_av_estimat: str
    kompetanse_som_trengs: str
    kompetanse_tilgjengelig: str
    prosjekt_id: str #Foreign key

@dataclass
class StrategiskForankring:
    strategisk_forankring_id: str
    base: str  # Hoved_instruks, tildelingsbrev, digital strategi, ...
    kode: str  # Hoved_instruks 3.1 
    beskrivelse: str  # ref setningen

@dataclass
class Problemstilling:
    problem_stilling_id: str
    problem_stilling: str
    prosjekt_id: str #Foreign key

@dataclass
class Tiltak:
    tiltak_id: str
    tiltak_beskrivelse: str
    prosjekt_id: str #Foreign key

@dataclass
class Risikovurdering:
    risiko_vurdering_id: str
    vurdering: str
    prosjekt_id: str

@dataclass 
class Fremskritt:
    fremskritt_id: str
    tidspunkt: datetime
    fase: str
    planlagt_ferdig: datetime
    er_gjeldende: bool
    prosjekt_id: str #Foreign key

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
