"""
*******************************************
Copyright: Regione Piemonte 2022
SPDX-License-Identifier: EUPL-1.2-or-later
*******************************************

/***************************************************************************
SentinelAPI
Accesso organizzato a dati Sentinel e generazione automatica di indici di vegetazione 
archiviati su base temporale mensile e trimestrale.
Python scripts, designed for an organization where the Administrators 
want to download Sentinel datasets and calculate vegetation indexes.
Date : 2022-04-16
copyright : (C) 2012-2022 by Regione Piemonte
authors : Luca Guida(Genegis), Fabio Roncato(Trilogis), Stefano Piffer(Trilogis) 
***************************************************************************/

/***************************************************************************
* *
* This program is free software; you can redistribute it and/or modify *
* it under the terms of the GNU General Public License as published by *
* the Free Software Foundation; either version 2 of the License, or *
* (at your option) any later version. *
* *
***************************************************************************/
"""

    # Naming Conventions SAFE file
    # https://sentinel.esa.int/web/sentinel/user-guides/sentinel-1-sar/naming-conventions
    
def splitNomeProdottoSentinel(nomeProdotto):
    
    # New format Naming Convention for Sentinel-2 Level-1C products generated after the 6th of December, 2016
    # https://sentinel.esa.int/web/sentinel/user-guides/sentinel-2-msi/naming-convention
    
    # MMM_MSIL1C_YYYYMMDDHHMMSS_Nxxyy_ROOO_Txxxxx_<Product Discriminator>.SAFE
    
    # The products contain two dates.
    # The first date (YYYYMMDDHHMMSS) is the datatake sensing time.
    # The second date is the "<Product Discriminator>" field, which is 15 characters in length,
    # and is used to distinguish between different end user products from the same datatake.
    # Depending on the instance, the time in this field can be earlier or slightly later than the datatake sensing time.

    # The other components of the filename are:

    # MMM: is the mission ID(S2A/S2B)
    # MSIL1C: denotes the Level-1C product level
    # YYYYMMDDHHMMSS: the datatake sensing start time
    # Nxxyy: the Processing Baseline number (e.g. N0204)
    # ROOO: Relative Orbit number (R001 - R143)
    # Txxxxx: Tile Number field
    # SAFE: Product Format (Standard Archive Format for Europe)
    
    mission, plevel, datatake, pbn, orbit, tile, data2 = nomeProdotto.split("_")
    
    anno = datatake[0:4]
    mese = datatake[4:6]
    
    return anno, mese, tile

