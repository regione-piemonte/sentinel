# SentinelScript - Scarico ed elaborazione immagini Copernicus Sentinel-2

Da alcuni anni il programma COPERNICUS della Commissione Europea ha reso pubblicamente 
disponibili dati satellitari provenienti da diverse costellazioni di satelliti per 
l’Osservazione della Terra (EO satellites). Tra questi, i dati della costellazione Sentinel-2

La missione "Copernicus Sentinel-2" è basata su una costellazione di due identici
satelliti nella stesa orbita. Ogni satellite trasporta un'ampia e innovativa serie di fotocamere
multispettrali (13 bande spettrale ad alta risoluzione - risoluzione geometrica di 10, 20 e 60 m 
dipendentemente dalla banda) ed una risoluzione temporale di 5 giorni.
per una nuova prospettiva della nostra terra e della nostra vegetazione.

Nel quadro dell'Infrastruttura Geografica Regionale (IGR) complessiva, la Direzione Agricoltura 
e il Settore Foreste hanno sostenuto uno specifico progetto denominato "Telerilevamento Piemonte"
attuato da CSI-Piemonte con il contributo scientifico del Gruppo di Ricerca di Geomatica 
Agro-Forestale (GEO4Agri) del DISAFA (Dipartimento di Scienze Agrarie, Forestali e Alimentari) 
- Università degli Studi di Torino, coordinato dal Prof. Borgogno Mondino Enrico.

Particolare attenzione, con la cura scientifica del DISAFA, è stata rivolta al trattamento dei dati 
Sentinel2, nello specifico si è messa a punto una metodologia che prevede l’individuazione di un 
valore di riferimento mensile e su questo il calcolo di tutti gli indicatori (raster) mensili 
(NDVI, NDWI, EVI, NBR, delta NBR).

Il servizio di scarico ed elaborazione CSI SentinelScript si occupa di ricercare e scaricare 
le immagini satellitari sulla zona configurata (Piemonte, Valle d'Aosta e Liguria).
Successivamente le immagini scaricate vengono elaborate per generare i suddetti indicatori utili 
ad un'analisi approfondita del terreno. 

Durante l'elaborazione vengono prodotti dei files utili alla successiva pubblicazione 
tramite protocolli OGC con il motore cartografico Mapserver. 
Ciascun soggetto riutilizzatore potrà pubblicare con il proprio sistema
i dati derivati dalle suddette elaborazioni.


Per approfondimenti https://www.geoportale.piemonte.it/cms/progetti/telerilevamento-piemonte

Il sistema è composto da 3 componenti principali :

- Scripts Python : componente server schedulata per lo scarico ed elaborazione delle immagini
- Database       : componente per la memorizzazione dei log operazionali
- Filesystem     : componente per il salvataggio delle immagini originali ed elaborate 

## Features:
* Nessuna interfaccia grafica
* Ricerca di immagini satellitari su un ambito geografico definito
* Scarico delle immagini
* Elaborazione delle immagini e produzioni di indicatori 

# Configurations 
CentOS Linux 7
Python 2.7
PostgreSQL  9.6.X or higher
GDAL 2.3.x or higher
Sentinel Python Scripts (from repository)
PostgreSQL Scripts (from repository)
Crontab example (from repository)

# Getting Started
These scripts were created to allow downloading of Sentinel dataset and creation of vegetation indexes (like NDVI, EVI, NBR and NDWI) on them. 
Optionally it is possible to create MapServer WMS and WCS configurations (based on templates) to expose such data as OGC WMS and WCS web services (MapServer is required).

Folders description:

	• /home/geosent/py_scripts/ <- home folder, with all python scripts
	• /sentinel/download/ <- archive folder, containing downloaded files from Sentinel Official Repository 
	• /sentinel/work/ <- temporary files folder, usefull for elaboration
	• /sentinel/output/ <- folder for final files, organized as follow:

		- /sentinel/output/images/<YEAR>/<MONTH>/tile/ (i.e. /sentinel/output/images/2018/03/ -> ) folder containing .vrt files.
		- /sentinel/output/images/trimonth/<YEAR>/<MM-MM-MM>/ (i.e. /sentinel/output/images/trimonth/2018/11-10-9/) folder containing .vrt files grouped quarterly.
		- /sentinel/output/include_ogc/ folder containing WCS and WMS MapServer Include file.


# Prerequisites
- Python 2.7
- PostgreSQL  9.6.X or higher
- GDAL 2.3.x or higher
- MapServer 7.2.1 (optional)

# Installing
1) Verify that GDAL is properly running (i.e. run command "/usr/local/bin/gdalinfo --version")
2) Copy .py files on your server (i.e. /home/geosent/py_scripts)
3) Configure PostgreSQL database based on sql files (scarichi2.sql and script_log.sql)
4) Configure parameters in file "csi_sentinel_config.py"
5) Configure email parameters in file "csi_sentinel_07_system_status_evaluation.py"
5) Configure crontab (based on "my_cron_SENTINEL_prod.txt" file example)

# Versioning
We use Semantic Versioning for versioning. (http://semver.org)

# Authors
See the list of contributors who participated inthis project in file AUTHORS.md 

# Copyrights
© Copyright: Regione Piemonte 2022

# License
See the LICENSE file for details (EUPL-1.2-or-later)




