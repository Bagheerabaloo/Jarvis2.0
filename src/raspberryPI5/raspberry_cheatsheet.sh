#!/bin/bash
# ============================================
# RASPBERRY PI 5 – CHEAT SHEET COMANDI USATI
# ============================================

############################
# 1. SSH & NAVIGAZIONE
############################

ssh vale@raspberrypi.local              # collegati al Raspberry via SSH dal PC
ssh vale@192.168.x.y                    # variante con IP diretto

whoami                                  # mostra l'utente corrente
pwd                                     # mostra la directory corrente
cd /home/vale                           # vai nella home di vale
cd /home/vale/Jarvis2.0                 # vai nella cartella del progetto Jarvis
ls                                      # lista file e cartelle
ls -l                                   # lista dettagliata
ls -a                                   # mostra anche file nascosti (tipo .env)

############################
# 2. GESTIONE FILE / EDITOR
############################

nano requirements.txt                   # modifica il file requirements.txt
nano .env                               # crea/modifica il file .env
nano /etc/systemd/system/monitor_temp.service   # crea/modifica servizio systemd monitor_temp
nano /etc/systemd/system/jarvis_main.service    # crea/modifica servizio systemd jarvis_main

cat /etc/os-release                     # info sulla distribuzione (Debian, versioni, ecc.)
cat cron.log                            # visualizza file di log locale (es. log di cron)

############################
# 3. PYTHON & VIRTUALENV
############################

sudo apt update                         # aggiorna lista pacchetti
sudo apt install -y python3 python3-venv python3-pip libpq-dev build-essential \
    libssl-dev zlib1g-dev libncurses5-dev libncursesw5-dev \
    libreadline-dev libsqlite3-dev libgdbm-dev libdb5.3-dev \
    libbz2-dev libexpat1-dev liblzma-dev tk-dev libffi-dev uuid-dev
# installa Python, venv e tool per compilare librerie (utile per psycopg2, ecc.)

python3 -m venv .venv                   # crea un virtualenv nella cartella .venv
source .venv/bin/activate               # attiva il virtualenv
pip install --upgrade pip               # aggiorna pip
pip install -r requirements.txt         # installa librerie dal requirements.txt
pip install psycopg2-binary             # driver PostgreSQL per Python
pip install python-telegram-bot python-dotenv requests
# installa librerie per bot Telegram e gestione variabili d’ambiente

python main.py                          # esegue lo script main (finché il terminale è aperto)
python monitor_temp.py                  # esegue lo script di monitoraggio temperatura

############################
# 4. COMPILAZIONE PYTHON DA SORGENTE (ESEMPIO)
############################

cd /usr/src                             # cartella dove mettere i sorgenti
wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz
tar -xzf Python-3.11.9.tgz              # estrai i sorgenti
cd Python-3.11.9

./configure --enable-optimizations      # configura la build di Python
make -j4                                # compila usando 4 core
sudo make altinstall                    # installa come versione aggiuntiva (es. python3.11)

python3.11 -m venv .venv                # crea venv specifico con Python 3.11

############################
# 5. GIT (PROGETTO JARVIS)
############################

cd /home/vale/Jarvis2.0
git status                              # mostra lo stato della repo
git pull                                # tira gli aggiornamenti da GitHub
git stash                               # salva (nasconde) modifiche locali per poter fare pull
git clone https://github.com/Bagheerabaloo/Jarvis2.0.git
# clona la repo (esempio) nella cartella corrente

############################
# 6. POSTGRESQL – SERVER E DB
############################

sudo apt install -y postgresql postgresql-contrib   # installa Postgres server + extra

systemctl status postgresql             # stato del servizio Postgres
sudo systemctl start postgresql         # avvia Postgres
sudo systemctl stop postgresql          # ferma Postgres
sudo systemctl restart postgresql       # riavvia Postgres

sudo -u postgres psql                   # entra nella console psql come utente postgres
sudo -u postgres createdb quotesdb      # crea database quotesdb
sudo -u postgres createdb metricsdb     # crea database metricsdb

# Connessione come utente admin via TCP
psql -h localhost -U admin -d metricsdb               # con password

# Connessione come postgres e comando SQL interno:
sudo -u postgres psql -d metricsdb                    # entra in metricsdb
# Dentro psql:
#   SHOW data_directory;                              # mostra dove vive il cluster
#   \q                                                # esci da psql

############################
# 7. POSTGRESQL – BACKUP & RESTORE
############################

# Backup da Heroku verso SSD montato
pg_dump "$DATABASE_URL" -Fc -f /mnt/ssd/heroku_backup_20251114_1741.dump
# crea un dump in formato custom sul tuo SSD

sudo pg_restore -l /mnt/ssd/heroku_backup_20251114_1741.dump | head
# mostra l’elenco (TOC) delle entry nel dump (prime righe)

# Restore verso DB locale (es. quotesdb)
sudo -u postgres pg_restore --no-owner --no-acl \
    -d quotesdb /mnt/ssd/heroku_backup_20251114_1741.dump

############################
# 8. POSTGRESQL – TABLESPACE (SOLO SE FS È ext4, NON exFAT/NTFS)
############################

sudo mkdir -p /mnt/ssd/postgres/quotesdb
sudo chown -R postgres:postgres /mnt/ssd/postgres
# prepara cartella su SSD (solo se SSD è ext4 o altro FS Linux)

# Dentro psql come postgres:
#   CREATE TABLESPACE quotespace LOCATION '/mnt/ssd/postgres/quotesdb';
#   ALTER DATABASE quotesdb SET TABLESPACE quotespace;
#   SELECT d.datname, t.spcname
#   FROM pg_database d JOIN pg_tablespace t ON d.dattablespace = t.oid;

############################
# 9. SSD / DISCHI / MOUNT
############################

lsblk                                   # mostra dischi e partizioni
lsblk -f                                # mostra FS, UUID e mountpoint
sudo fdisk -l                           # mostra tabella partizioni
sudo fdisk -l /dev/sda                  # dettaglio del disco SSD /dev/sda

sudo mount /dev/sda1 /mnt/ssd           # monta la partizione sda1 in /mnt/ssd
sudo umount /mnt/ssd                    # smonta la partizione
mount | grep sda                        # controlla dove è montato sda*
grep sda /proc/mounts                   # stessa cosa via /proc

dmesg | tail                            # ultimi messaggi del kernel (incluso disco collegato)
dmesg | tail -n 20                      # ultimi 20 messaggi

sudo fsck.exfat /dev/sda1               # controlla/ripara partizione exFAT
sudo apt install exfatprogs             # tool per exFAT (fsck, mkfs, ecc.)

ls /mnt/ssd                             # mostra i file nella cartella montata

############################
# 10. PARTIZIONI NUOVE (PER EXT4 SU SSD)
############################

sudo fdisk /dev/sda                     # crea/modifica partizioni su sda (INTERATTIVO!)
# Poi:
sudo mkfs.ext4 /dev/sda2                # formatta sda2 in ext4
sudo mkdir /mnt/ssd_pg                  # cartella per montare partizione ext4
sudo mount /dev/sda2 /mnt/ssd_pg        # monta la partizione ext4
sudo chown postgres:postgres /mnt/ssd_pg   # se la userà Postgres

############################
# 11. SYSTEMD – SERVIZI (monitor_temp, jarvis_main)
############################

# Dopo aver creato i file .service con nano:

sudo systemctl daemon-reload            # ricarica definizioni systemd

# monitor_temp
sudo systemctl start monitor_temp       # avvia servizio monitor_temp
sudo systemctl stop monitor_temp        # ferma servizio monitor_temp
sudo systemctl restart monitor_temp     # riavvia
sudo systemctl enable monitor_temp      # abilita avvio automatico al boot
systemctl status monitor_temp           # mostra stato del servizio
journalctl -u monitor_temp -f           # log in tempo reale

# jarvis_main (main.py)
sudo systemctl start jarvis_main        # avvia servizio jarvis_main
sudo systemctl stop jarvis_main         # ferma servizio jarvis_main
sudo systemctl restart jarvis_main      # riavvia servizio
sudo systemctl enable jarvis_main       # abilita al boot
systemctl status jarvis_main            # mostra stato
journalctl -u jarvis_main -f            # log in tempo reale

############################
# 12. SYSTEMD – COSA PARTE AL BOOT
############################

systemctl list-unit-files --type=service --state=enabled
# mostra i servizi abilitati (partono al boot)

systemctl --type=service --state=running
# mostra i servizi attualmente in esecuzione

ls /etc/systemd/system
# mostra i servizi custom (tra cui i tuoi *.service)

ls /etc/systemd/system/multi-user.target.wants/
# servizi abilitati in modalità multi-user (boot normale)

systemctl is-enabled monitor_temp       # dice se il servizio è abilitato al boot o no

############################
# 13. CRON
############################

crontab -e                              # modifica i job cron dell’utente corrente

# Esempio di job (da inserire in crontab):
# 0 9 * * * cd /home/vale/telegram-bot && /home/vale/telegram-bot/.venv/bin/python /home/vale/telegram-bot/bot.py >> /home/vale/telegram-bot/cron.log 2>&1

############################
# 14. MONITOR TEMPERATURA / CPU / RAM
############################

vcgencmd measure_temp                   # legge la temperatura CPU (Raspberry)
# (su alcuni sistemi: /usr/bin/vcgencmd oppure serve installare libreria firmware)

sudo apt install htop                   # installa htop
htop                                    # monitor risorse interattivo (CPU, RAM, processi)

top                                     # monitor risorse standard
free -h                                 # mostra RAM totale/usata/libera in formato leggibile
uptime                                  # mostra uptime e load average
sudo apt install sysstat                # installa tools sysstat
mpstat 1                                # mostra l’uso CPU ogni secondo

sudo apt install glances                # installa glances (monitor avanzato)
glances                                 # avvia glances

############################
# 15. LOG DI SISTEMA & DIAGNOSTICA
############################

journalctl -u monitor_temp              # log del servizio monitor_temp
journalctl -u jarvis_main               # log del servizio jarvis_main
journalctl -u postgresql                # log del servizio Postgres
journalctl -xe                          # log di sistema con dettagli errori

dmesg | tail                            # ultimi messaggi del kernel

sudo lsof | grep /mnt/ssd               # processi che stanno usando /mnt/ssd
sudo fuser -m /mnt/ssd                  # processi che tengono occupato il mount /mnt/ssd

############################
# 16. TMUX / NOHUP PER PROCESSI CHE SOPRAVVIVONO A SSH
############################

tmux new -s bot                         # crea nuova sessione tmux chiamata "bot"
python main.py                          # esegui lo script dentro tmux
# stacca da tmux:
#   Ctrl + B, poi D
tmux attach -t bot                      # ricollegati alla sessione tmux "bot"

nohup python main.py > main.log 2>&1 &  # esegui main.py in background, anche se chiudi SSH

############################
# 17. COMANDI DI SPEGNIMENTO / RIAVVIO RASPBERRY
############################

sudo shutdown -h now                    # spegne subito il Raspberry in modo sicuro
sudo poweroff                           # equivalente: spegne il sistema
sudo shutdown -h +5                     # spegne tra 5 minuti
sudo reboot                             # riavvia il Raspberry

# Dopo questi, puoi staccare la corrente solo quando i led indicano che è spento.

# ============================================
echo "Cheat sheet caricato (NON eseguire tutto in blocco 😄)"
