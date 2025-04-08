# Automatisert Rapportering for Internasjonale Aktiviteter
Dette prosjektet implementerer en automatisert løsning for registrering og rapportering av internasjonale aktiviteter i Digdir. Løsningen erstatter manuelle prosesser basert på Excel, spørreundersøkelser og dialogmøter med en standardisert og automatisert datainnsamlingsprosess som oppdaterer to Power BI-dashbord (for intern og ekstern bruk).

## Problemstilling
Den nåværende prosessen for innhenting av informasjon om internasjonale aktiviteter er manuell, tidkrevende og risikoutsatt for feil og uenhetlige data. Informasjon innhentes via:
- Microsoft forms spørreundersøkelser sendt til ulike avdelinger
- Analoge dialoger som gir viktig kontekst, men mangler standardisering
- Manuell sammenstilling av data i en master Excel-fil

## Teknisk løsning
Løsningen er implementert i Microsoft Fabric og består av følgende komponenter:

- Datainnsamling: Microsoft Forms for standardisert spørreundersøkelse
- Datalagring: Dataflow i Fabric for automatisert datainnsamling og lagring
- Dataanalyse og visualisering: Power BI-dashbord for intern og ekstern rapportering