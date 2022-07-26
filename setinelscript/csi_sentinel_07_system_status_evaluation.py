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

import smtplib
from email.mime.text import MIMEText
import subprocess

# Open a plain text file for reading.  For this example, assume that
# the text file contains only ASCII characters.
allert_threshold = 92
account_username = "XXX@YYY.ZZZ"    # TO CONFIGURE
account_password = "AAA"            # TO CONFIGURE
email_sender = "QQQ@CCC.SSS"        # TO CONFIGURE
email_receivers = ["KKK@JJJ.LLL","SSS@RRR.WWW", "HHH@FFF.EEE"] # TO CONFIGURE

child = subprocess.Popen(['df', '-h'], stdout=subprocess.PIPE)
output = child.communicate()[0].strip().split("\n")
for x in output[1:]:
    if str(x.split()[-1]).find("sentinel") != -1:
        if int(x.split()[-2][:-1]) > allert_threshold:
            
            disk_space_message = """Attenzione, 
la dimensione dello spazio disco disponibile della macchina, macchina di produzione, sta per esaurirsi.
Dimensione dello spazio occupato al'""" + str(x.split()[-2][:-1]) + """%. Soglia di segnalazione attuale impostata all'""" + str(allert_threshold) + """%.
Procedere liberando spazio cancellado i file non necessari.
Problema rilevato da script automatico.

CSI Piemonte"""
            msg_disk_space_message = MIMEText(disk_space_message)
            msg_disk_space_message['Subject'] = 'WARNING - Telerilevamento'
            msg_disk_space_message['From'] = email_sender
            
            server = smtplib.SMTP("UUU.NNN.MMM", XXX) # TO CONFIGURE. FOR GMAIL ACCOUNT USE ("smtp.gmail.com", 587)
            server.ehlo()
            server.starttls()
            server.login(account_username, account_password)
            server.sendmail(email_sender, email_receivers, msg_disk_space_message.as_string())
            server.quit()
        else:
            print("Dimensione spazio disco nella norma. Spazio disco " + str(x.split()[-2][:-1]) + "%.")
            print("Soglia di segnalazione attuale impostata all'" + str(allert_threshold) + "%.")
        
